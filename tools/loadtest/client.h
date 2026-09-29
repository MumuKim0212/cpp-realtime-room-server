#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "net/event_loop.h"
#include "session/buffer.h"
#include "stats.h"

namespace rrs::loadtest {

// One simulated connection: connects, logs in, joins a room, and from then on
// sends chat at a fixed interval while timing everything the room hands back.
//
// Latency is measured from the bytes a sender wrote to the bytes a receiver
// read, which is the delay a person would notice. The sending clock reading
// travels inside the message text, so any connection that receives it can work
// the delay out on its own -- the two ends need agree on nothing beyond the
// steady_clock they already share by living in one process.
//
// The reactor and the codec are the server's own. That keeps the tool honest
// about the wire format and small enough to read, at the cost of not being an
// independent implementation: a framing bug the two share would cancel out
// here. The end-to-end tests exist to catch that, and this measures speed.
class Client {
public:
    struct Config {
        std::string username;
        std::string password;
        std::uint32_t room_id = 0;

        // Between this client's own messages, and how far into the first
        // interval it starts, so that a population does not fire in lockstep.
        std::chrono::nanoseconds send_interval{0};
        std::chrono::nanoseconds start_offset{0};

        std::size_t message_bytes = 0;
    };

    // `fd` is a non-blocking socket with connect() already under way; the
    // client takes it over, including closing it.
    Client(net::EventLoop& loop, int fd, Config config, Latencies& latencies,
           Counters& counters, std::function<void()> on_joined);
    ~Client();

    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;

    // Starts this client's traffic. Held back until the whole population is
    // in, because a login costs an Argon2id verify and the last client is
    // seconds behind the first; measuring through that would be measuring the
    // ramp.
    void start_sending();

private:
    enum class State { kConnecting, kLoggingIn, kJoining, kJoined, kDone };

    void handle_events(std::uint32_t events);
    void finish_connect();
    void read_available();
    void dispatch_frames();
    void handle_frame(std::uint16_t type, const std::uint8_t* payload,
                      std::uint16_t length);

    void send_login();
    void send_join();
    void send_chat();
    void send_frame(const std::vector<std::uint8_t>& frame);
    void flush();
    void update_interest();

    // Ends this connection and counts it. The run continues without it.
    void give_up(const char* what);

    net::EventLoop& loop_;
    int fd_;
    Config config_;
    Latencies& latencies_;
    Counters& counters_;
    std::function<void()> on_joined_;

    session::Buffer recv_buffer_;
    session::Buffer send_buffer_;

    net::TimerId send_timer_ = 0;
    State state_ = State::kConnecting;
    std::uint32_t interest_ = 0;
};

}  // namespace rrs::loadtest
