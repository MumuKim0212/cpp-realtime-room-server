#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "net/timer_queue.h"

namespace rrs::session {
class Session;
}

namespace rrs::room {

// A set of connected clients that receive the same packets. Broadcasting is
// defined as "send this packet to everyone here" rather than as a chat
// operation, because once rooms tick a room also owns shared state and the
// packets it sends out are state snapshots as much as messages.
//
// A room belongs to exactly one event loop, which is what keeps it free of
// locking: the same thread delivers its messages and runs its tick.
class Room {
public:
    Room(std::uint32_t id, std::size_t capacity);

    std::uint32_t id() const { return id_; }
    std::size_t member_count() const { return members_.size(); }
    bool full() const { return members_.size() >= capacity_; }
    bool contains(const session::Session& session) const;

    // False when the room is at capacity. A session already inside is not
    // added twice and the call still succeeds; turning a re-join into an error
    // is left to the caller, which the protocol wants distinguished from a
    // full room anyway.
    bool join(session::Session& session);

    // Harmless for a session that is not a member.
    void leave(session::Session& session);

    void broadcast(const std::uint8_t* data, std::size_t size);
    void broadcast_except(const session::Session& excluded,
                          const std::uint8_t* data, std::size_t size);

    // Advances shared room state. Empty for as long as chat rooms have nothing
    // to simulate; the seat exists now because adding it later would mean
    // changing how rooms are owned and ticked, not just filling in a body.
    void update(net::Duration dt);

private:
    void send_to_members(const session::Session* excluded,
                         const std::uint8_t* data, std::size_t size);

    std::uint32_t id_;
    std::size_t capacity_;
    std::vector<session::Session*> members_;
};

}  // namespace rrs::room
