#include "net/acceptor.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <gtest/gtest.h>

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <initializer_list>
#include <stdexcept>
#include <system_error>
#include <vector>

#include "net/event_loop.h"

namespace {

using namespace std::chrono_literals;

using rrs::net::Acceptor;
using rrs::net::EventLoop;
using rrs::net::is_exhaustion_error;
using rrs::net::is_pending_connection_error;
using rrs::net::is_stream_socket;

class AcceptorTest : public ::testing::Test {
protected:
    ~AcceptorTest() override {
        for (const int fd : open_fds_) {
            ::close(fd);
        }
    }

    // A blocking client socket, which connects to a listening socket on
    // loopback without waiting.
    int connect_to(std::uint16_t port) {
        const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        EXPECT_GE(fd, 0);

        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_port = ::htons(port);
        EXPECT_EQ(::inet_pton(AF_INET, "127.0.0.1", &address.sin_addr), 1);
        EXPECT_EQ(::connect(fd, reinterpret_cast<const sockaddr*>(&address),
                            sizeof(address)),
                  0);

        open_fds_.push_back(fd);
        return fd;
    }

    void track(int fd) { open_fds_.push_back(fd); }

    void pump(std::chrono::milliseconds duration) {
        loop_.run_after(duration, [this] { loop_.stop(); });
        loop_.run();
    }

    EventLoop loop_;
    std::vector<int> open_fds_;
};

TEST_F(AcceptorTest, BindsAnEphemeralPort) {
    const Acceptor acceptor(loop_, "127.0.0.1", 0, [](int fd) { ::close(fd); });

    EXPECT_NE(acceptor.port(), 0);
}

TEST_F(AcceptorTest, AcceptsAConnectionAndHandsOverAWorkingSocket) {
    std::vector<int> accepted;
    Acceptor acceptor(loop_, "127.0.0.1", 0, [&](int fd) {
        accepted.push_back(fd);
        track(fd);
    });

    const int client = connect_to(acceptor.port());
    pump(50ms);

    ASSERT_EQ(accepted.size(), 1u);

    // The descriptor is the other end of this client, not just some number.
    const char sent = 'x';
    ASSERT_EQ(::send(client, &sent, 1, 0), 1);
    char got = '\0';
    pump(50ms);
    EXPECT_EQ(::recv(accepted[0], &got, 1, 0), 1);
    EXPECT_EQ(got, sent);
}

TEST_F(AcceptorTest, AcceptsEveryConnectionWaitingInTheBacklog) {
    // All three connect before the loop runs once, so a single readiness
    // notification has to yield all of them.
    std::vector<int> accepted;
    Acceptor acceptor(loop_, "127.0.0.1", 0, [&](int fd) {
        accepted.push_back(fd);
        track(fd);
    });

    for (int i = 0; i < 3; ++i) {
        connect_to(acceptor.port());
    }
    pump(50ms);

    EXPECT_EQ(accepted.size(), 3u);
}

TEST_F(AcceptorTest, AcceptedDescriptorsAreNonBlocking) {
    // Session takes this for granted, and a blocking descriptor would let one
    // client stall the entire loop.
    std::vector<int> accepted;
    Acceptor acceptor(loop_, "127.0.0.1", 0, [&](int fd) {
        accepted.push_back(fd);
        track(fd);
    });

    connect_to(acceptor.port());
    pump(50ms);

    ASSERT_EQ(accepted.size(), 1u);
    const int flags = ::fcntl(accepted[0], F_GETFL, 0);
    ASSERT_GE(flags, 0);
    EXPECT_TRUE((flags & O_NONBLOCK) != 0);
}


TEST_F(AcceptorTest, AcceptedDescriptorsHaveNagleDisabled) {
    // A room broadcast is a run of small frames to a client with nothing to
    // say back. Nagle waits for an acknowledgement the peer is in no hurry to
    // send, which put a ~40 ms cliff under a share of every delivery until
    // this was turned off.
    std::vector<int> accepted;
    Acceptor acceptor(loop_, "127.0.0.1", 0, [&](int fd) {
        accepted.push_back(fd);
        track(fd);
    });

    connect_to(acceptor.port());
    pump(50ms);

    ASSERT_EQ(accepted.size(), 1u);
    int nodelay = 0;
    socklen_t length = sizeof(nodelay);
    ASSERT_EQ(::getsockopt(accepted[0], IPPROTO_TCP, TCP_NODELAY, &nodelay, &length), 0);
    EXPECT_NE(nodelay, 0);
}
TEST_F(AcceptorTest, RefusesAPortThatIsAlreadyListening) {
    const Acceptor first(loop_, "127.0.0.1", 0, [](int fd) { ::close(fd); });

    EXPECT_THROW(
        {
            const Acceptor second(loop_, "127.0.0.1", first.port(),
                                  [](int fd) { ::close(fd); });
        },
        std::system_error);
}

TEST_F(AcceptorTest, RefusesAHostThatIsNotAnAddress) {
    EXPECT_THROW(
        {
            const Acceptor acceptor(loop_, "localhost", 0, [](int fd) { ::close(fd); });
        },
        std::invalid_argument);
}

// A failed accept4 gets one of three answers, and which one is a table lookup.
// Staging the failures themselves takes a firewall or an exhausted machine, so
// the table is checked directly: a wrong entry loses the listening socket over
// one peer, or spins the loop on a socket that stays readable, and neither is
// visible until it happens.

TEST(AcceptError, TreatsAPendingNetworkErrorAsTheConnectionsOwn) {
    // The list accept(2) gives for TCP/IP. Linux hands the pending connection's
    // errors back through accept, and says to retry rather than to read them as
    // a failure of the socket being accepted from.
    for (const int error : {ENETDOWN, EPROTO, ENOPROTOOPT, EHOSTDOWN, ENONET,
                            EHOSTUNREACH, EOPNOTSUPP, ENETUNREACH}) {
        EXPECT_TRUE(is_pending_connection_error(error))
            << "errno " << error << ": " << std::strerror(error);
    }
}

TEST(AcceptError, CountsAPeerThatGaveUpAndOneAFirewallRefused) {
    // Not network errors on the new socket, but the same answer for the same
    // reason: the backlog behind them is untouched.
    EXPECT_TRUE(is_pending_connection_error(ECONNABORTED));
    EXPECT_TRUE(is_pending_connection_error(EPERM));
}

TEST(AcceptError, KeepsRunningOutOfSomethingApart) {
    // These pause accepting instead. Sorting one as the connection's problem
    // would spin: the connection stays in the backlog, so the socket stays
    // readable, and the next accept fails the same way with nothing changed.
    for (const int error : {EMFILE, ENFILE, ENOBUFS, ENOMEM}) {
        EXPECT_TRUE(is_exhaustion_error(error))
            << "errno " << error << ": " << std::strerror(error);
        EXPECT_FALSE(is_pending_connection_error(error));
    }
}

TEST(AcceptError, LeavesTheListeningSocketsOwnFaultsToStopOn) {
    // What neither answer claims reaches the throw, and it should: these say
    // the descriptor is not the socket this code thinks it is, which is a bug
    // rather than a condition to ride out.
    for (const int error : {EBADF, EINVAL, ENOTSOCK, EFAULT}) {
        EXPECT_FALSE(is_pending_connection_error(error))
            << "errno " << error << ": " << std::strerror(error);
        EXPECT_FALSE(is_exhaustion_error(error));
    }
}

// EOPNOTSUPP is in that table and also means the descriptor is not a stream
// socket, and the two want opposite answers. What separates them is the socket,
// so these cover the asking rather than the table.

TEST(AcceptError, TellsAStreamSocketFromADatagramOne) {
    const int stream = ::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(stream, 0);
    EXPECT_TRUE(is_stream_socket(stream));
    ::close(stream);

    // The kind of descriptor accept4 refuses for what it is rather than for
    // anything a peer did, which is the answer that has to reach the throw.
    const int datagram = ::socket(AF_INET, SOCK_DGRAM, 0);
    ASSERT_GE(datagram, 0);
    EXPECT_FALSE(is_stream_socket(datagram));
    ::close(datagram);
}

TEST(AcceptError, TreatsADescriptorItCannotAskAsNoStreamSocket) {
    // Nothing to retry on here either, so the unanswerable cases fall the same
    // way as a datagram socket rather than being taken for a stream one.
    EXPECT_FALSE(is_stream_socket(-1));

    const int closed = ::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(closed, 0);
    ::close(closed);
    EXPECT_FALSE(is_stream_socket(closed));
}

TEST(AcceptError, LeavesErrnoWhereTheFailedAcceptLeftIt) {
    // It is asked in the middle of handling a failure, whose errno is still
    // wanted afterwards -- to finish sorting, and to report if it throws. The
    // question fails here, which is the case that would overwrite it.
    const int closed = ::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(closed, 0);
    ::close(closed);

    errno = EOPNOTSUPP;
    EXPECT_FALSE(is_stream_socket(closed));
    EXPECT_EQ(errno, EOPNOTSUPP);
}

}  // namespace
