#include "session/session.h"

#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <system_error>
#include <utility>

#include "protocol/frame.h"

namespace rrs::session {
namespace {

// A peer that vanished mid-write would otherwise raise SIGPIPE and take the
// whole server down; MSG_NOSIGNAL turns that into a plain EPIPE.
constexpr int kSendFlags = MSG_NOSIGNAL;

bool would_block() {
    return errno == EAGAIN || errno == EWOULDBLOCK;
}

}  // namespace

Session::Session(net::EventLoop& loop, int fd, MessageCallback on_message,
                 CloseCallback on_close)
    : loop_(loop),
      fd_(fd),
      on_message_(std::move(on_message)),
      on_close_(std::move(on_close)),
      last_activity_(net::Clock::now()) {
    loop_.add_fd(fd_, EPOLLIN,
                 [this](std::uint32_t events) { handle_events(events); });
    // Mirrors what add_fd() just registered, so update_interest() can tell a
    // mask worth an epoll_ctl from one that changes nothing.
    interest_ = EPOLLIN;

    idle_timer_ = loop_.run_after(kIdleTimeout, [this] { check_idle(); });
}

Session::~Session() {
    if (idle_timer_ != 0) {
        loop_.cancel_timer(idle_timer_);  // it captures this
    }
    if (close_timer_ != 0) {
        loop_.cancel_timer(close_timer_);  // likewise
    }
    if (fd_ < 0) {
        return;  // finish_close() already ran
    }

    // Destroyed while still registered, which happens when the server tears
    // down its connections at shutdown.
    try {
        loop_.remove_fd(fd_);
    } catch (const std::system_error&) {
        // A destructor has nothing useful to do about it, and closing the
        // descriptor drops it out of epoll regardless.
    }
    ::close(fd_);
}

void Session::handle_events(std::uint32_t events) {
    if (events & (EPOLLERR | EPOLLHUP)) {
        close();
    }
    // Draining the socket before reading frees space for whatever the inbound
    // messages are about to generate in reply.
    if (state_ == State::kOpen && (events & EPOLLOUT)) {
        flush();
    }
    if (state_ == State::kOpen && (events & EPOLLIN)) {
        read_once();
    }
}

void Session::read_once() {
    std::uint8_t chunk[kReadChunkBytes];
    const ssize_t received = ::recv(fd_, chunk, sizeof(chunk), 0);

    if (received == 0) {
        close();  // the peer sent FIN and has nothing more to say
        return;
    }
    if (received < 0) {
        if (would_block() || errno == EINTR) {
            return;
        }
        close();
        return;
    }

    last_activity_ = net::Clock::now();
    recv_buffer_.append(chunk, static_cast<std::size_t>(received));
    dispatch_frames();
}

void Session::check_idle() {
    idle_timer_ = 0;

    const net::Duration quiet_for = net::Clock::now() - last_activity_;
    if (quiet_for >= kIdleTimeout) {
        close();
        return;
    }

    // The connection spoke after this was scheduled, so the deadline moved.
    idle_timer_ = loop_.run_after(kIdleTimeout - quiet_for, [this] { check_idle(); });
}

void Session::dispatch_frames() {
    // One recv() can carry a fragment of a frame, several whole frames, or
    // both, so this keeps consuming until what is left is incomplete.
    while (state_ == State::kOpen) {
        if (recv_buffer_.size() < protocol::kHeaderSize) {
            return;  // not even a full header yet
        }

        const auto header = protocol::parse_header(recv_buffer_.data());
        const std::size_t frame_size = protocol::kHeaderSize + header.payload_len;
        if (recv_buffer_.size() < frame_size) {
            return;  // header is in, payload still arriving
        }

        // The band is checked before the payload is touched, so a client
        // forging a server-only type is rejected without parsing its fields.
        if (!protocol::is_client_to_server(header.type)) {
            send_error(protocol::ErrorCode::kWrongDirection,
                       "server-only message type");
            close();
            return;
        }

        on_message_(*this, header.type, recv_buffer_.data() + protocol::kHeaderSize,
                    header.payload_len);
        recv_buffer_.consume(frame_size);
    }
}

void Session::send(const std::uint8_t* data, std::size_t size) {
    if (state_ != State::kOpen) {
        return;
    }

    if (send_buffer_.empty()) {
        // Nothing queued ahead of this, so try the socket directly and buffer
        // only the tail that did not fit.
        const ssize_t written = ::send(fd_, data, size, kSendFlags);
        if (written < 0) {
            if (!would_block() && errno != EINTR) {
                close();
                return;
            }
        } else {
            data += written;
            size -= static_cast<std::size_t>(written);
        }
        if (size == 0) {
            return;
        }
    }

    send_buffer_.append(data, size);

    if (send_buffer_.size() > kMaxSendBufferBytes) {
        // Backpressure. No ERROR_NTF goes out first: the reason this limit was
        // reached is that the peer is not draining, so the notification would
        // only join the queue that is already too long.
        close();
        return;
    }

    update_interest();
}

void Session::flush() {
    while (!send_buffer_.empty()) {
        const ssize_t written =
            ::send(fd_, send_buffer_.data(), send_buffer_.size(), kSendFlags);
        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (would_block()) {
                return;  // socket is full again, wait for the next EPOLLOUT
            }
            close();
            return;
        }
        send_buffer_.consume(static_cast<std::size_t>(written));
    }

    update_interest();
}

void Session::update_interest() {
    // Derived rather than tracked: reading stops at shutdown, writability is
    // wanted exactly while something is queued, and both are answered by state
    // that is already correct.
    std::uint32_t events = 0;
    if (reading_) {
        events |= EPOLLIN;
    }
    if (!send_buffer_.empty()) {
        events |= EPOLLOUT;
    }

    if (events == interest_) {
        return;  // spare the epoll_ctl syscall
    }
    interest_ = events;
    loop_.mod_fd(fd_, events);
}

void Session::stop_reading() {
    if (state_ != State::kOpen) {
        return;  // already on its way out
    }
    reading_ = false;
    update_interest();
}

void Session::send_error(protocol::ErrorCode code, std::string_view message) {
    protocol::FrameBuilder builder(protocol::MessageType::kErrorNtf);
    builder.write_u16(static_cast<std::uint16_t>(code));
    builder.write_string(message);

    const protocol::Bytes frame = builder.build();
    send(frame.data(), frame.size());
}

void Session::close() {
    if (state_ != State::kOpen) {
        return;
    }
    state_ = State::kClosing;

    // Teardown is handed to the loop rather than run here, because a session is
    // routinely closed from inside a call on it: a broadcast walks a list of
    // recipients and sends to each in turn, and tearing one down mid-send would
    // leave that walk holding a dangling pointer to whichever *other* recipient
    // the resulting cascade also closed. A zero-delay timer puts the teardown
    // after the batch of callbacks instead, where nothing is mid-iteration.
    //
    // Until then the session stays alive and inert: send() and the dispatch
    // loop both stop at a state that is no longer kOpen. Its descriptor stays
    // open too, which keeps the number from being handed to a fresh connection
    // while events for this one are still queued in the batch being delivered.
    close_timer_ = loop_.run_after(net::Duration::zero(), [this] {
        close_timer_ = 0;
        finish_close();
    });
}

void Session::finish_close() {
    if (idle_timer_ != 0) {
        loop_.cancel_timer(idle_timer_);
        idle_timer_ = 0;
    }
    loop_.remove_fd(fd_);
    ::close(fd_);
    fd_ = -1;

    // Moved out before the call: the owner destroys this session from inside
    // the callback, which would otherwise destroy the std::function mid-call.
    CloseCallback on_close = std::move(on_close_);
    if (on_close) {
        on_close(*this);
    }
}

}  // namespace rrs::session
