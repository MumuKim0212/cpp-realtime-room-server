#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

#include "protocol/message.h"

namespace rrs::protocol {

using Bytes = std::vector<std::uint8_t>;

// Every frame opens with u16 payload length and u16 type, both big-endian.
// The length covers the payload only and never these four bytes; the two sides
// disagreeing about that is the classic length-prefix bug.
inline constexpr std::size_t kHeaderSize = 4;

// Structural ceiling on a payload. Because the length field is u16, no client
// can ask the server for a larger allocation than this, so the limit needs no
// separate check.
inline constexpr std::size_t kMaxPayloadSize = 0xFFFF;

struct FrameHeader {
    std::uint16_t payload_len;
    std::uint16_t type;
};

// Reads a header from a buffer holding at least kHeaderSize bytes.
FrameHeader parse_header(const std::uint8_t* data);

// Builds one outbound frame. The header is reserved up front and its length
// field patched in by build(), which keeps payload writes append-only.
//
// build() moves the buffer out, so a builder is good for exactly one frame.
class FrameBuilder {
public:
    explicit FrameBuilder(MessageType type);

    void write_u8(std::uint8_t value);
    void write_u16(std::uint16_t value);
    void write_u32(std::uint32_t value);
    void write_u64(std::uint64_t value);
    void write_i64(std::int64_t value);

    // u16 byte length followed by the bytes, with no terminator. The length
    // counts bytes, not characters.
    void write_string(std::string_view value);

    // Throws std::length_error if the payload outgrew the u16 length field.
    // Reaching that point means the server tried to send something it should
    // have paginated, which is a bug rather than bad input.
    Bytes build();

private:
    Bytes buffer_;
};

}  // namespace rrs::protocol
