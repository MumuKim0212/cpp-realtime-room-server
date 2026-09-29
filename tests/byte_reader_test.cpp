#include "protocol/byte_reader.h"

#include <gtest/gtest.h>

#include <cstdint>

namespace {

using rrs::protocol::ByteReader;

TEST(ByteReader, ReadsWidthsBigEndian) {
    const std::uint8_t payload[] = {
        0x01,
        0x02, 0x03,
        0x04, 0x05, 0x06, 0x07,
        0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F,
    };
    ByteReader reader(payload, sizeof(payload));

    EXPECT_EQ(reader.read_u8(), 0x01);
    EXPECT_EQ(reader.read_u16(), 0x0203);
    EXPECT_EQ(reader.read_u32(), 0x04050607U);
    EXPECT_EQ(reader.read_u64(), 0x08090A0B0C0D0E0FULL);
    EXPECT_TRUE(reader.exhausted());
}

TEST(ByteReader, ReadingPastTheEndLatchesFailure) {
    const std::uint8_t payload[] = {0x00, 0x01};
    ByteReader reader(payload, sizeof(payload));

    EXPECT_EQ(reader.read_u16(), 1);
    EXPECT_TRUE(reader.ok());

    reader.read_u8();

    EXPECT_FALSE(reader.ok());
    EXPECT_FALSE(reader.exhausted());
}

TEST(ByteReader, ReadsAfterAFailureYieldZero) {
    const std::uint8_t payload[] = {0xFF};
    ByteReader reader(payload, sizeof(payload));

    reader.read_u32();  // only one byte is available
    ASSERT_FALSE(reader.ok());

    // The byte that is present must not be handed out once parsing has failed;
    // the caller has already lost track of where the fields begin.
    EXPECT_EQ(reader.read_u8(), 0);
    EXPECT_FALSE(reader.ok());
}

TEST(ByteReader, StringReachingPastThePayloadFails) {
    // The frame's own length field checked out, but the string inside claims
    // more bytes than the payload holds. This is the read that walks a parser
    // off the end of the buffer if it goes unchecked.
    const std::uint8_t payload[] = {0x00, 0x10, 'a', 'b'};
    ByteReader reader(payload, sizeof(payload));

    EXPECT_TRUE(reader.read_string().empty());
    EXPECT_FALSE(reader.ok());
}

TEST(ByteReader, TruncatedStringLengthFails) {
    const std::uint8_t payload[] = {0x00};  // half of the u16 length prefix
    ByteReader reader(payload, sizeof(payload));

    reader.read_string();

    EXPECT_FALSE(reader.ok());
}

TEST(ByteReader, ReadsAnEmptyString) {
    const std::uint8_t payload[] = {0x00, 0x00};
    ByteReader reader(payload, sizeof(payload));

    EXPECT_EQ(reader.read_string(), "");
    EXPECT_TRUE(reader.exhausted());
}

TEST(ByteReader, TrailingBytesLeaveItUnexhausted) {
    const std::uint8_t payload[] = {0x00, 0x01, 0xAA};
    ByteReader reader(payload, sizeof(payload));

    EXPECT_EQ(reader.read_u16(), 1);

    EXPECT_TRUE(reader.ok());
    EXPECT_FALSE(reader.exhausted());
}

TEST(ByteReader, HandlesAnEmptyPayload) {
    ByteReader reader(nullptr, 0);

    EXPECT_TRUE(reader.exhausted());

    reader.read_u8();

    EXPECT_FALSE(reader.ok());
}

}  // namespace
