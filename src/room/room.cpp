#include "room/room.h"

#include <algorithm>

#include "session/session.h"

namespace rrs::room {

Room::Room(std::uint32_t id, std::size_t capacity)
    : id_(id), capacity_(capacity) {}

bool Room::contains(const session::Session& session) const {
    return std::find(members_.begin(), members_.end(), &session) != members_.end();
}

bool Room::join(session::Session& session) {
    if (contains(session)) {
        return true;
    }
    if (full()) {
        return false;
    }
    members_.push_back(&session);
    return true;
}

void Room::leave(session::Session& session) {
    const auto it = std::find(members_.begin(), members_.end(), &session);
    if (it != members_.end()) {
        members_.erase(it);
    }
}

void Room::broadcast(const std::uint8_t* data, std::size_t size) {
    send_to_members(nullptr, data, size);
}

void Room::broadcast_except(const session::Session& excluded,
                            const std::uint8_t* data, std::size_t size) {
    send_to_members(&excluded, data, size);
}

void Room::send_to_members(const session::Session* excluded,
                           const std::uint8_t* data, std::size_t size) {
    // Iterated over a copy. A send can trip the backpressure limit and close
    // the connection it was aimed at, and that close eventually takes the
    // session out of this very list; the copy keeps the walk from depending on
    // exactly when.
    const std::vector<session::Session*> recipients = members_;

    for (session::Session* member : recipients) {
        if (member != excluded) {
            member->send(data, size);
        }
    }
}

void Room::update(net::Duration /*dt*/) {
    // Chat rooms hold no state to advance. A ticking room applies the inputs
    // collected since the last tick here and broadcasts the resulting snapshot.
}

}  // namespace rrs::room
