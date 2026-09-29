#include "protocol/byte_reader.h"

namespace rrs::protocol {

ByteReader::ByteReader(const std::uint8_t* data, std::size_t size)
    : data_(data), size_(size) {}

std::uint64_t ByteReader::read_be(std::size_t width) {
    if (!ok_ || size_ - pos_ < width) {
        ok_ = false;
        return 0;
    }

    std::uint64_t value = 0;
    for (std::size_t i = 0; i < width; ++i) {
        value = (value << 8) | data_[pos_ + i];
    }
    pos_ += width;
    return value;
}

std::uint8_t ByteReader::read_u8() {
    return static_cast<std::uint8_t>(read_be(1));
}

std::uint16_t ByteReader::read_u16() {
    return static_cast<std::uint16_t>(read_be(2));
}

std::uint32_t ByteReader::read_u32() {
    return static_cast<std::uint32_t>(read_be(4));
}

std::uint64_t ByteReader::read_u64() {
    return read_be(8);
}

std::int64_t ByteReader::read_i64() {
    // Two's complement, so the bit pattern carries straight over.
    return static_cast<std::int64_t>(read_be(8));
}

std::string_view ByteReader::read_string() {
    const std::size_t length = read_u16();
    if (!ok_ || size_ - pos_ < length) {
        ok_ = false;
        return {};
    }

    const auto* begin = reinterpret_cast<const char*>(data_ + pos_);
    pos_ += length;
    return std::string_view(begin, length);
}

}  // namespace rrs::protocol
