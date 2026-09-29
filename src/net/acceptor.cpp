#include "net/acceptor.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <chrono>
#include <stdexcept>
#include <system_error>
#include <utility>

namespace rrs::net {
namespace {

// How long to stop accepting for after the machine runs out of something,
// giving whatever is holding it a chance to let go.
constexpr auto kExhaustionPause = std::chrono::milliseconds(100);

// Nagle holds a small segment back until the one before it has been
// acknowledged, and the peer's delayed ACK is entitled to sit on that
// acknowledgement for tens of milliseconds. A room broadcast is exactly the
// traffic that punishes: small frames, one after another, to a client that has
// nothing to say back and so has no packet to carry the ACK along with.
//
// Left on, it does not slow everything down evenly -- it puts a ~40 ms cliff
// under a fraction of deliveries, which is worse. This server would rather
// send now than save a header.
void disable_nagle(int fd) {
    const int nodelay = 1;
    if (::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay)) < 0) {
        // Worth saying, not worth refusing the connection over: it works, it
        // is only slower than it should be.
        std::fprintf(stderr, "acceptor: could not disable Nagle: %s\n",
                     std::strerror(errno));
    }
}

[[noreturn]] void throw_errno(const char* what) {
    throw std::system_error(errno, std::system_category(), what);
}

// Closes a descriptor unless it is released, so a setup step that fails
// half-way does not leak the socket it was working on.
class FdGuard {
public:
    explicit FdGuard(int fd) : fd_(fd) {}

    ~FdGuard() {
        if (fd_ >= 0) {
            const int saved = errno;  // close() must not overwrite the failure
            ::close(fd_);
            errno = saved;
        }
    }

    FdGuard(const FdGuard&) = delete;
    FdGuard& operator=(const FdGuard&) = delete;

    int get() const { return fd_; }

    int release() {
        const int fd = fd_;
        fd_ = -1;
        return fd;
    }

private:
    int fd_;
};

int create_listening_socket(const std::string& host, std::uint16_t port) {
    FdGuard socket(::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    if (socket.get() < 0) {
        throw_errno("socket");
    }

    // Lets the server restart without waiting out TIME_WAIT on its own port.
    const int reuse = 1;
    if (::setsockopt(socket.get(), SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        throw_errno("setsockopt(SO_REUSEADDR)");
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = ::htons(port);
    if (::inet_pton(AF_INET, host.c_str(), &address.sin_addr) != 1) {
        throw std::invalid_argument("listen host must be an IPv4 address: " + host);
    }

    if (::bind(socket.get(), reinterpret_cast<const sockaddr*>(&address),
               sizeof(address)) < 0) {
        throw_errno("bind");
    }
    if (::listen(socket.get(), SOMAXCONN) < 0) {
        throw_errno("listen");
    }

    return socket.release();
}

}  // namespace

// The first two are the connection's own doing: ECONNABORTED for a peer that
// gave up between the handshake and the accept, EPERM for one a firewall rule
// refused. The rest are the list accept(2) gives for TCP/IP under "Error
// handling" -- Linux passes a pending network error on the new socket back
// through accept, and says to retry rather than to read it as a failure of the
// socket accepted from.
bool is_pending_connection_error(int error) {
    switch (error) {
        case ECONNABORTED:
        case EPERM:
        case ENETDOWN:
        case EPROTO:
        case ENOPROTOOPT:
        case EHOSTDOWN:
        case ENONET:
        case EHOSTUNREACH:
        case EOPNOTSUPP:
        case ENETUNREACH:
            return true;
        default:
            return false;
    }
}

bool is_exhaustion_error(int error) {
    return error == EMFILE || error == ENFILE || error == ENOBUFS ||
           error == ENOMEM;
}

bool is_stream_socket(int fd) {
    int type = 0;
    socklen_t length = sizeof(type);

    // Saved and put back: this is called from a failed accept4, whose errno the
    // caller still has to classify and may have to report.
    const int failure = errno;
    const bool answered =
        ::getsockopt(fd, SOL_SOCKET, SO_TYPE, &type, &length) == 0;
    errno = failure;

    return answered && type == SOCK_STREAM;
}

Acceptor::Acceptor(EventLoop& loop, const std::string& host, std::uint16_t port,
                   ConnectionCallback on_connection)
    : loop_(loop),
      fd_(create_listening_socket(host, port)),
      on_connection_(std::move(on_connection)) {
    loop_.add_fd(fd_, EPOLLIN, [this](std::uint32_t) { accept_ready(); });
}

Acceptor::~Acceptor() {
    if (retry_timer_ != 0) {
        loop_.cancel_timer(retry_timer_);  // it captures this
    }
    try {
        loop_.remove_fd(fd_);
    } catch (const std::system_error&) {
        // A destructor has nothing useful to do about it, and closing the
        // descriptor drops it out of epoll regardless.
    }
    ::close(fd_);
}

std::uint16_t Acceptor::port() const {
    sockaddr_in address{};
    socklen_t length = sizeof(address);
    if (::getsockname(fd_, reinterpret_cast<sockaddr*>(&address), &length) < 0) {
        throw_errno("getsockname");
    }
    return ::ntohs(address.sin_port);
}

void Acceptor::accept_ready() {
    // Accepting drains the backlog rather than taking one connection per
    // notification. The backlog is bounded by the kernel, so unlike reading
    // from a connection this cannot be stretched out by a busy client, and a
    // fresh connection does no work until it sends something.
    while (true) {
        const int fd = ::accept4(fd_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (fd >= 0) {
            disable_nagle(fd);
            on_connection_(fd);
            continue;
        }

        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return;  // backlog drained
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno == EOPNOTSUPP && !is_stream_socket(fd_)) {
            // The entry in the table that means two things, resolved here
            // because only the socket can resolve it. On this side of it the
            // descriptor is the wrong kind, so accept4 will answer the same way
            // forever: sorting it as the waiting connection's problem would
            // turn the retry below into a loop that never gets anywhere.
            throw_errno("accept4");
        }
        if (is_pending_connection_error(errno)) {
            // That connection's problem, not this server's; the next one is
            // still waiting. Losing the whole listening socket over one peer
            // that gave up would be the far worse answer.
            continue;
        }
        if (is_exhaustion_error(errno)) {
            pause_accepting();
            return;
        }

        // Anything left says the listening socket itself is not what this
        // thinks it is -- a closed or wrong descriptor -- which is a bug in the
        // server rather than a condition to ride out, and is worth stopping on.
        throw_errno("accept4");
    }
}

void Acceptor::pause_accepting() {
    // The connection still waiting keeps the listening socket readable, so a
    // level-triggered loop would spin on it forever. Stop watching and look
    // again in a moment instead.
    std::fprintf(stderr, "acceptor: out of resources (%s), pausing accepts\n",
                 std::strerror(errno));

    loop_.mod_fd(fd_, 0);
    retry_timer_ = loop_.run_after(kExhaustionPause, [this] {
        retry_timer_ = 0;
        loop_.mod_fd(fd_, EPOLLIN);
    });
}

}  // namespace rrs::net
