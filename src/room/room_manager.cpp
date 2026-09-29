#include "room/room_manager.h"

namespace rrs::room {

Room& RoomManager::create(std::uint32_t room_id, std::size_t capacity) {
    return rooms_.try_emplace(room_id, room_id, capacity).first->second;
}

Room* RoomManager::find(std::uint32_t room_id) {
    const auto it = rooms_.find(room_id);
    return it == rooms_.end() ? nullptr : &it->second;
}

}  // namespace rrs::room
