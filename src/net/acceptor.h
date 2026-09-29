#pragma once

#include <cstdint>
#include <functional>
#include <string>

#include "net/event_loop.h"

namespace rrs::net {

// How a failed accept4 is sorted, which decides one of three answers: take the
// next connection, stop accepting for a moment, or stop altogether. The sorting
// is a table, and a table is the part that rots -- a wrong entry either takes
// the listening socket down over one peer or spins the loop on a socket that
// stays readable, and neither shows up until the day it does. Declared here so
// it can be checked directly rather than only through a failure nobody can
// stage; only the accept path calls them.

// Errors that belong to the connection being accepted rather than to the
// listening socket: the peer went away, or the network answered for it, or a
// firewall hook refused it, before accept4 reached it. The rest of the backlog
// is unaffected, so the answer is to take the next one.
bool is_pending_connection_error(int error);

// Errors that say the machine has run out of something. Waiting is the only
// answer, and the one thing that must not happen is spinning on a listening
// socket that stays readable because the connection is still there.
bool is_exhaustion_error(int error);

// Whether `fd` is a stream socket, which settles the one entry in the table
// above that means two things. accept(2) lists EOPNOTSUPP twice over: as a
// pending network error on the connection, and as "the referenced socket is
// not of type SOCK_STREAM". Those want opposite answers -- take the next
// connection, and stop -- and the error cannot tell them apart, because which
// one it is, is a property of the socket rather than of the failure. So the
// socket is asked. A descriptor that cannot answer counts as no stream socket:
// it is not one to keep retrying on either. Leaves errno as it found it, since
// the only caller is in the middle of reading it.
bool is_stream_socket(int fd);

// A listening socket registered with an event loop.
//
// The host and port are handed in rather than defaulted here: where a server
// listens is a deployment decision, and burying one in the network layer only
// makes it harder to find.
class Acceptor {
public:
    // Receives an accepted, already non-blocking descriptor and owns it from
    // that point on, including closing it.
    using ConnectionCallback = std::function<void(int fd)>;

    // Binds and starts listening immediately. Throws std::system_error if the
    // address cannot be bound, and std::invalid_argument if `host` is not a
    // dotted-quad IPv4 address.
    Acceptor(EventLoop& loop, const std::string& host, std::uint16_t port,
             ConnectionCallback on_connection);
    ~Acceptor();

    Acceptor(const Acceptor&) = delete;
    Acceptor& operator=(const Acceptor&) = delete;

    // The port actually bound, which is what tells you the real one after
    // asking for port 0.
    std::uint16_t port() const;

private:
    void accept_ready();
    void pause_accepting();

    EventLoop& loop_;
    int fd_;
    ConnectionCallback on_connection_;
    TimerId retry_timer_ = 0;
};

}  // namespace rrs::net
