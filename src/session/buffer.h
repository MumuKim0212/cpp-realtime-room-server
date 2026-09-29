#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

namespace rrs::session {

// A byte queue for one direction of a connection. Producers append, consumers
// read from the front and mark bytes consumed, because socket reads and writes
// never line up with message boundaries.
//
// Consumed bytes are not reclaimed on the spot. Doing that on every append is
// what turns a peer that drains slowly into a quadratic cost, so the front is
// left in place until it is worth moving what is behind it -- see append().
//
// Two multipliers follow from that and they compound. The bytes held span less
// than twice size(), because the front is reclaimed once it accounts for half
// of them. Behind that span is a vector, which doubles when it grows, so what
// is actually reserved reaches about four times size(). Only the first of those
// is this class's policy; the second is the standard library's. Anyone
// budgeting memory per connection wants the product, not either one.
class Buffer {
public:
    void append(const std::uint8_t* data, std::size_t size);

    // Start of the bytes not yet consumed. Invalidated by append() and
    // consume().
    const std::uint8_t* data() const { return bytes_.data() + read_pos_; }

    std::size_t size() const { return bytes_.size() - read_pos_; }
    bool empty() const { return size() == 0; }

    // Bytes currently reserved, which reaches about four times size() for the
    // two reasons above. Nothing in the server needs this; it is here so the
    // allocation policy described above can be tested rather than assumed.
    std::size_t capacity() const { return bytes_.capacity(); }

    // Drops `count` bytes from the front. `count` may not exceed size().
    void consume(std::size_t count);

    // Bytes copied so far by reclaiming the consumed front. The policy in
    // append() is that a move never copies more than it gives back, so this
    // stays under the bytes consumed however the traffic is shaped -- which is
    // the whole of what "amortised constant" means here, and is a count rather
    // than a duration. Nothing in the server needs it; like capacity(), it is
    // here so the policy can be tested rather than assumed.
    std::size_t moved_bytes() const { return moved_bytes_; }

private:
    // Moves the unread tail to the front and drops the consumed prefix.
    void compact();

    std::vector<std::uint8_t> bytes_;
    std::size_t read_pos_ = 0;
    std::size_t moved_bytes_ = 0;
};

}  // namespace rrs::session
