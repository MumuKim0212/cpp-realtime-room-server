#include "session/buffer.h"

#include <iterator>

namespace rrs::session {
namespace {

// A buffer that has fully drained gives its allocation back once it is larger
// than this. Sized above one maximum frame so ordinary traffic never pays for
// the release: only a connection that actually queued a burst holds more, and
// holding megabytes for the rest of a quiet connection's life is what this
// stops.
constexpr std::size_t kRetainedCapacityBytes = 64 * 1024;

}  // namespace

void Buffer::compact() {
    moved_bytes_ += bytes_.size() - read_pos_;
    bytes_.erase(bytes_.begin(),
                 bytes_.begin() + static_cast<std::ptrdiff_t>(read_pos_));
    read_pos_ = 0;
}

void Buffer::append(const std::uint8_t* data, std::size_t size) {
    // Reclaiming the consumed prefix means moving everything behind it, so it
    // waits until that prefix is at least half the buffer. Then each move
    // reclaims at least as many bytes as it copies, which is what makes the
    // cost amortised constant instead of proportional to the backlog.
    //
    // Doing it eagerly is the trap: a peer that drains part of what is queued
    // leaves read_pos_ > 0, and every later append would move the whole
    // remaining backlog -- up to kMaxSendBufferBytes -- on the event loop
    // thread, once per send, while every other connection waits.
    if (read_pos_ > 0 && read_pos_ * 2 >= bytes_.size()) {
        compact();
    }
    bytes_.insert(bytes_.end(), data, data + size);
}

void Buffer::consume(std::size_t count) {
    read_pos_ += count;
    if (read_pos_ < bytes_.size()) {
        return;
    }

    // Fully drained, the common case once whole frames have been dispatched.
    // Reset rather than letting the offset creep upward.
    bytes_.clear();
    read_pos_ = 0;
    if (bytes_.capacity() > kRetainedCapacityBytes) {
        bytes_.shrink_to_fit();
    }
}

}  // namespace rrs::session
