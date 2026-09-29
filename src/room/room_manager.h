#pragma once

#include <cstddef>
#include <cstdint>
#include <unordered_map>

#include "room/room.h"

namespace rrs::room {

// Owns the rooms one event loop is responsible for.
//
// Rooms exist independently of the clients in them: a client asking for a room
// that was never created is refused rather than having one conjured for it,
// which is what the protocol's "no such target" result is for. Creating them
// is the server's business, from a configured list or from the database.
class RoomManager {
public:
    // Capacity comes per room because the database records it per row. Returns
    // the existing room, capacity untouched, when the id is already taken.
    Room& create(std::uint32_t room_id, std::size_t capacity);

    // Null when no such room exists.
    Room* find(std::uint32_t room_id);

    std::size_t room_count() const { return rooms_.size(); }

private:
    // Node-based, so a Room& handed out stays valid as more rooms are created.
    std::unordered_map<std::uint32_t, Room> rooms_;
};

}  // namespace rrs::room
