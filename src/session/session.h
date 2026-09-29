#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <string_view>

#include "net/event_loop.h"
#include "protocol/message.h"
#include "session/buffer.h"

namespace rrs::session {

// One recv() per readiness notification reads at most this much.
inline constexpr std::size_t kReadChunkBytes = 16 * 1024;

// A client that stops draining its socket must not be able to grow the
// server's memory without bound. Past this much queued output the connection
// is judged hopeless and dropped; at 64 KiB per frame it still leaves room for
// a sizeable burst to ride out a brief stall.
//
// This counts bytes still owed to the peer, not bytes reserved to hold them.
// The buffer defers reclaiming what has already gone out -- doing it eagerly
// costs a copy of the whole backlog per send, on the loop thread -- and that
// deferral plus the vector's own growth puts the reservation at about four
// times this, so one connection at the limit costs nearer four megabytes than
// one. See session/buffer.h.
inline constexpr std::size_t kMaxSendBufferBytes = 1024 * 1024;

// A connection that has said nothing for this long is presumed gone. Silence is
// the only evidence available, so a client with nothing to say keeps itself out
// of this by sending PING_REQ; the window is more than double the interval the
// protocol asks for, which means several pings have to go missing before a live
// connection is mistaken for a dead one.
inline constexpr auto kIdleTimeout = std::chrono::minutes(5);

// One client connection. Owns its socket, turns the inbound byte stream into
// frames, and queues outbound frames without ever blocking the event loop.
//
// The descriptor must already be non-blocking. Registration is level-triggered
// and exactly one recv() runs per notification: the loop re-reports a socket
// that still holds data, so no client can hold an iteration hostage by being
// drained to EAGAIN while every other connection waits.
//
// Teardown never runs inline. close() only marks the session; the event loop
// takes it apart once the callbacks in flight have returned, so close() is safe
// to call from anywhere, including from inside a broadcast to other sessions.
// The close callback is the last thing a session does, and the owner is
// expected to destroy it from there.
class Session {
public:
    // `payload` points into the receive buffer and is valid only for the
    // duration of the call.
    using MessageCallback = std::function<void(Session&, std::uint16_t type,
                                               const std::uint8_t* payload,
                                               std::uint16_t payload_len)>;
    using CloseCallback = std::function<void(Session&)>;

    Session(net::EventLoop& loop, int fd, MessageCallback on_message,
            CloseCallback on_close);
    ~Session();

    Session(const Session&) = delete;
    Session& operator=(const Session&) = delete;

    int fd() const { return fd_; }

    // Queues an encoded frame, writing straight through to the socket when it
    // can and buffering only what did not fit.
    void send(const std::uint8_t* data, std::size_t size);

    // Queues an ERROR_NTF. Whether the connection then ends is the caller's
    // decision, because the protocol keeps it alive for field validation
    // failures and drops it for framing violations.
    void send_error(protocol::ErrorCode code, std::string_view message);

    // Requests teardown. Takes effect once the loop is done delivering the
    // events it is currently working through.
    void close();

    // Stops taking anything more from the peer while leaving what is already
    // queued on its way out. Shutdown uses this: a message the server has
    // accepted should still reach the client, but nothing new should be
    // started while the process is on its way down.
    void stop_reading();

    // True while bytes queued for the peer have not reached the socket yet.
    bool has_pending_output() const { return !send_buffer_.empty(); }

private:
    enum class State { kOpen, kClosing };

    void handle_events(std::uint32_t events);
    void read_once();
    void dispatch_frames();
    void flush();
    void update_interest();
    void check_idle();
    void finish_close();

    net::EventLoop& loop_;
    int fd_;
    MessageCallback on_message_;
    CloseCallback on_close_;

    Buffer recv_buffer_;
    Buffer send_buffer_;

    // Rescheduled lazily rather than reset on every message: the timer wakes
    // at the deadline, sees the connection spoke since, and pushes itself out
    // again. Costs one timestamp per message instead of a heap operation.
    net::TimerId idle_timer_ = 0;
    net::TimePoint last_activity_;

    // Set by close(), cleared when the deferred teardown runs. Non-zero means a
    // teardown is already on the way and must be called off if the session is
    // destroyed before it gets there.
    net::TimerId close_timer_ = 0;

    State state_ = State::kOpen;

    // What the loop is currently watching for. Cached so an interest that has
    // not moved costs no epoll_ctl; update_interest() owns both of these.
    std::uint32_t interest_ = 0;
    bool reading_ = true;
};

}  // namespace rrs::session
