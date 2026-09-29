#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace rrs::protocol {

// Reads big-endian fields out of a payload, refusing to run past its end.
//
// A payload whose length field checked out can still lie about what is inside
// it: a string length reaching past the payload boundary is the easiest way to
// walk a parser off the end of a buffer. So every read is bounds-checked here
// rather than at the call sites, where one forgotten check is enough.
//
// Failure latches. Once a read comes up short, ok() stays false and every
// later read yields zero, which lets a caller parse a whole message and check
// once at the end instead of after every field. Values obtained after a
// failure are meaningless and must not be used.
class ByteReader {
public:
    ByteReader(const std::uint8_t* data, std::size_t size);

    std::uint8_t read_u8();
    std::uint16_t read_u16();
    std::uint32_t read_u32();
    std::uint64_t read_u64();
    std::int64_t read_i64();

    // u16 byte length followed by that many bytes. The view points into the
    // payload and is valid only for as long as the payload is.
    std::string_view read_string();

    bool ok() const { return ok_; }

    // True when every byte was consumed and nothing trailed the last field.
    // Bytes left over mean the sender's idea of the message differs from ours.
    bool exhausted() const { return ok_ && pos_ == size_; }

private:
    std::uint64_t read_be(std::size_t width);

    const std::uint8_t* data_;
    std::size_t size_;
    std::size_t pos_ = 0;
    bool ok_ = true;
};

}  // namespace rrs::protocol
