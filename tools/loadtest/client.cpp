#include "client.h"

#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdio>
#include <optional>
#include <string_view>
#include <system_error>
#include <utility>

#include "protocol/byte_reader.h"
#include "protocol/frame.h"
#include "protocol/message.h"

namespace rrs::loadtest {
namespace {

using protocol::Bytes;
using protocol::FrameBuilder;
using protocol::MessageType;

// One recv() takes at most this much. Larger than the server's chunk because
// this side is the one under fan-out: every message a room sees arrives here
// once per member.
constexpr std::size_t kReadChunkBytes = 64 * 1024;

// A peer that goes away mid-write must not raise SIGPIPE and take the run down
// with it.
constexpr int kSendFlags = MSG_NOSIGNAL;

// A run against a broken server would otherwise bury the terminal in identical
// lines. The counters still see every one of them.
constexpr std::size_t kMaxReportedFailures = 10;
std::size_t reported_failures = 0;

// How long a login the server was too busy to check waits before asking again.
// A run larger than the server's login ceiling meets that answer by design, and
// the connection stays up for the retry; waiting it out instead would leave the
// client stuck until the server's login deadline hangs up on it.
constexpr auto kLoginRetryDelay = std::chrono::milliseconds(250);

bool would_block() {
    return errno == EAGAIN || errno == EWOULDBLOCK;
}

// The body carries the sending clock reading in decimal, then padding out to
// whatever size the run asked for. Decimal so that a packet dump stays
// readable, and first so that reading it back does not depend on where the
// padding ends.
std::string chat_body(std::size_t size_bytes) {
    std::string body =
        std::to_string(net::Clock::now().time_since_epoch().count());
    if (body.size() < size_bytes) {
        body.append(size_bytes - body.size(), '.');
    }
    return body;
}

// The reading the sender stamped in, or nothing when the text did not come
// from this tool. Anything else in the room would be timed against a number it
// never wrote.
std::optional<std::int64_t> stamped_ticks(std::string_view text) {
    std::int64_t ticks = 0;
    std::size_t digits = 0;

    for (const char c : text) {
        if (c < '0' || c > '9') {
            break;
        }
        ticks = ticks * 10 + (c - '0');
        ++digits;
    }
    return digits == 0 ? std::nullopt : std::optional<std::int64_t>(ticks);
}

}  // namespace

Client::Client(net::EventLoop& loop, int fd, Config config,
               Latencies& latencies, Counters& counters,
               std::function<void()> on_joined)
    : loop_(loop),
      fd_(fd),
      config_(std::move(config)),
      latencies_(latencies),
      counters_(counters),
      on_joined_(std::move(on_joined)) {
    // Writability is how a non-blocking connect reports that it finished.
    loop_.add_fd(fd_, EPOLLOUT,
                 [this](std::uint32_t events) { handle_events(events); });
    interest_ = EPOLLOUT;
}

Client::~Client() {
    if (send_timer_ != 0) {
        loop_.cancel_timer(send_timer_);  // it captures this
    }
    if (fd_ < 0) {
        return;  // give_up() already ran
    }

    try {
        loop_.remove_fd(fd_);
    } catch (const std::system_error&) {
        // Closing the descriptor drops it out of epoll regardless.
    }
    ::close(fd_);
}

void Client::start_sending() {
    if (state_ != State::kJoined || send_timer_ != 0) {
        return;
    }

    send_timer_ = loop_.run_after(config_.start_offset, [this] {
        send_chat();
        send_timer_ =
            loop_.run_every(config_.send_interval, [this] { send_chat(); });
    });
}

void Client::handle_events(std::uint32_t events) {
    if (state_ == State::kDone) {
        return;
    }
    if (events & (EPOLLERR | EPOLLHUP)) {
        give_up("connection lost");
        return;
    }
    if (state_ == State::kConnecting) {
        finish_connect();
        return;
    }

    if (events & EPOLLOUT) {
        flush();
    }
    if (state_ != State::kDone && (events & EPOLLIN)) {
        read_available();
    }
}

void Client::finish_connect() {
    int error = 0;
    socklen_t length = sizeof(error);
    if (::getsockopt(fd_, SOL_SOCKET, SO_ERROR, &error, &length) < 0) {
        give_up("getsockopt(SO_ERROR)");
        return;
    }
    if (error != 0) {
        errno = error;
        give_up("connect");
        return;
    }

    ++counters_.connected;
    state_ = State::kLoggingIn;
    send_login();
    update_interest();
}

void Client::read_available() {
    std::uint8_t chunk[kReadChunkBytes];

    // Drained to EAGAIN rather than one read per notification. Bytes left
    // sitting in the socket while every other connection takes its turn would
    // be counted as latency the server did not cause.
    while (true) {
        const ssize_t received = ::recv(fd_, chunk, sizeof(chunk), 0);
        if (received == 0) {
            give_up("server closed the connection");
            return;
        }
        if (received < 0) {
            if (would_block()) {
                return;
            }
            if (errno == EINTR) {
                continue;
            }
            give_up("recv");
            return;
        }

        recv_buffer_.append(chunk, static_cast<std::size_t>(received));
        dispatch_frames();
        if (state_ == State::kDone) {
            return;
        }
    }
}

void Client::dispatch_frames() {
    while (state_ != State::kDone) {
        if (recv_buffer_.size() < protocol::kHeaderSize) {
            return;
        }

        const auto header = protocol::parse_header(recv_buffer_.data());
        const std::size_t frame_size = protocol::kHeaderSize + header.payload_len;
        if (recv_buffer_.size() < frame_size) {
            return;
        }

        handle_frame(header.type, recv_buffer_.data() + protocol::kHeaderSize,
                     header.payload_len);
        recv_buffer_.consume(frame_size);
    }
}

void Client::handle_frame(std::uint16_t type, const std::uint8_t* payload,
                          std::uint16_t length) {
    protocol::ByteReader reader(payload, length);

    switch (static_cast<MessageType>(type)) {
        case MessageType::kLoginRes: {
            if (reader.read_u8() != 0) {
                ++counters_.login_failed;
                give_up("login refused");
                return;
            }
            state_ = State::kJoining;
            send_join();
            return;
        }

        case MessageType::kJoinRoomRes: {
            if (reader.read_u8() != 0) {
                ++counters_.join_failed;
                give_up("join refused");
                return;
            }
            state_ = State::kJoined;
            ++counters_.joined;

            // Moved out before the call: the run starts every client from
            // inside this one, and the last of them would otherwise destroy
            // the function it is being called through.
            std::function<void()> joined = std::move(on_joined_);
            if (joined) {
                joined();
            }
            return;
        }

        case MessageType::kChatNtf: {
            reader.read_u64();     // the sender's id is not what is measured
            reader.read_string();  // nor their name
            const std::string_view text = reader.read_string();
            if (!reader.ok()) {
                return;
            }

            const std::optional<std::int64_t> ticks = stamped_ticks(text);
            if (!ticks) {
                return;
            }
            ++counters_.deliveries;
            latencies_.add(net::Clock::now().time_since_epoch().count() - *ticks);
            return;
        }

        case MessageType::kErrorNtf: {
            ++counters_.protocol_errors;
            const std::uint16_t code = reader.read_u16();
            if (reported_failures < kMaxReportedFailures) {
                ++reported_failures;
                std::fprintf(stderr, "loadtest: %s: ERROR_NTF %u\n",
                             config_.username.c_str(), code);
            }

            // Still counted above: a shed login is the server saying something
            // the run should report, even when the retry gets the client in.
            if (state_ == State::kLoggingIn && send_timer_ == 0 &&
                code == static_cast<std::uint16_t>(protocol::ErrorCode::kFieldValidationFailed)) {
                send_timer_ = loop_.run_after(kLoginRetryDelay, [this] {
                    send_timer_ = 0;
                    send_login();
                });
            }
            return;
        }

        default:
            // Joins, leaves and pongs arrive and are none of this tool's
            // business. Taking them off the wire is the whole obligation.
            return;
    }
}

void Client::send_login() {
    FrameBuilder builder(MessageType::kLoginReq);
    builder.write_u16(protocol::kProtocolVersion);
    builder.write_string(config_.username);
    builder.write_string(config_.password);
    send_frame(builder.build());
}

void Client::send_join() {
    FrameBuilder builder(MessageType::kJoinRoomReq);
    builder.write_u32(config_.room_id);
    send_frame(builder.build());
}

void Client::send_chat() {
    if (state_ != State::kJoined) {
        return;
    }

    FrameBuilder builder(MessageType::kChatReq);
    builder.write_string(chat_body(config_.message_bytes));
    send_frame(builder.build());
    ++counters_.messages_sent;
}

void Client::send_frame(const Bytes& frame) {
    if (state_ == State::kDone) {
        return;
    }

    const std::uint8_t* data = frame.data();
    std::size_t size = frame.size();

    if (send_buffer_.empty()) {
        const ssize_t written = ::send(fd_, data, size, kSendFlags);
        if (written < 0) {
            if (!would_block() && errno != EINTR) {
                give_up("send");
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
    update_interest();
}

void Client::flush() {
    while (!send_buffer_.empty()) {
        const ssize_t written =
            ::send(fd_, send_buffer_.data(), send_buffer_.size(), kSendFlags);
        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (would_block()) {
                return;
            }
            give_up("send");
            return;
        }
        send_buffer_.consume(static_cast<std::size_t>(written));
    }

    update_interest();
}

void Client::update_interest() {
    std::uint32_t events = state_ == State::kConnecting
                               ? static_cast<std::uint32_t>(EPOLLOUT)
                               : static_cast<std::uint32_t>(EPOLLIN);
    if (!send_buffer_.empty()) {
        events |= EPOLLOUT;
    }

    if (events == interest_) {
        return;
    }
    interest_ = events;
    loop_.mod_fd(fd_, events);
}

void Client::give_up(const char* what) {
    if (state_ == State::kDone) {
        return;
    }
    state_ = State::kDone;
    ++counters_.lost;

    if (send_timer_ != 0) {
        loop_.cancel_timer(send_timer_);
        send_timer_ = 0;
    }

    // Removing its own descriptor from inside its own callback is something
    // the loop supports on purpose.
    loop_.remove_fd(fd_);
    ::close(fd_);
    fd_ = -1;

    if (reported_failures < kMaxReportedFailures) {
        ++reported_failures;
        std::fprintf(stderr, "loadtest: %s: %s\n", config_.username.c_str(), what);
    }
}

}  // namespace rrs::loadtest
