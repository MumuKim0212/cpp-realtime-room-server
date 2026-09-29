#include "protocol/frame.h"

#include <stdexcept>
#include <utility>

namespace rrs::protocol {
namespace {

// Appends the low `width` bytes of `value`, most significant first.
void append_be(Bytes& out, std::uint64_t value, std::size_t width) {
    for (std::size_t i = width; i > 0; --i) {
        out.push_back(static_cast<std::uint8_t>(value >> ((i - 1) * 8)));
    }
}

}  // namespace

FrameHeader parse_header(const std::uint8_t* data) {
    return FrameHeader{
        static_cast<std::uint16_t>((data[0] << 8) | data[1]),
        static_cast<std::uint16_t>((data[2] << 8) | data[3]),
    };
}

FrameBuilder::FrameBuilder(MessageType type) {
    append_be(buffer_, 0, 2);  // payload length, patched in by build()
    append_be(buffer_, static_cast<std::uint64_t>(type), 2);
}

void FrameBuilder::write_u8(std::uint8_t value) {
    append_be(buffer_, value, 1);
}

void FrameBuilder::write_u16(std::uint16_t value) {
    append_be(buffer_, value, 2);
}

void FrameBuilder::write_u32(std::uint32_t value) {
    append_be(buffer_, value, 4);
}

void FrameBuilder::write_u64(std::uint64_t value) {
    append_be(buffer_, value, 8);
}

void FrameBuilder::write_i64(std::int64_t value) {
    append_be(buffer_, static_cast<std::uint64_t>(value), 8);
}

void FrameBuilder::write_string(std::string_view value) {
    if (value.size() > kMaxPayloadSize) {
        throw std::length_error("string does not fit a u16 length prefix");
    }
    write_u16(static_cast<std::uint16_t>(value.size()));
    buffer_.insert(buffer_.end(), value.begin(), value.end());
}

Bytes FrameBuilder::build() {
    const std::size_t payload_size = buffer_.size() - kHeaderSize;
    if (payload_size > kMaxPayloadSize) {
        throw std::length_error("frame payload exceeds the u16 length field");
    }
    buffer_[0] = static_cast<std::uint8_t>(payload_size >> 8);
    buffer_[1] = static_cast<std::uint8_t>(payload_size);
    return std::move(buffer_);
}

}  // namespace rrs::protocol
