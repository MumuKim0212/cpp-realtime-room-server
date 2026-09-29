#include "protocol/frame.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <stdexcept>
#include <string>

#include "protocol/byte_reader.h"

namespace {

using rrs::protocol::ByteReader;
using rrs::protocol::Bytes;
using rrs::protocol::FrameBuilder;
using rrs::protocol::kHeaderSize;
using rrs::protocol::MessageType;
using rrs::protocol::parse_header;

TEST(Frame, HeaderIsBigEndian) {
    const std::uint8_t raw[] = {0x01, 0x02, 0x80, 0x04};

    const auto header = parse_header(raw);

    EXPECT_EQ(header.payload_len, 0x0102);
    EXPECT_EQ(header.type, 0x8004);
}

TEST(Frame, PayloadLengthExcludesTheHeader) {
    // The documented trap of length-prefixed protocols: the field counts
    // payload bytes only, never the four header bytes.
    FrameBuilder builder(MessageType::kJoinRoomReq);
    builder.write_u32(7);

    const Bytes frame = builder.build();

    ASSERT_EQ(frame.size(), kHeaderSize + 4);
    const auto header = parse_header(frame.data());
    EXPECT_EQ(header.payload_len, 4);
    EXPECT_EQ(header.type, static_cast<std::uint16_t>(MessageType::kJoinRoomReq));
}

TEST(Frame, EmptyPayloadIsAHeaderOnly) {
    // LEAVE_ROOM_REQ carries no fields at all.
    FrameBuilder builder(MessageType::kLeaveRoomReq);

    const Bytes frame = builder.build();

    EXPECT_EQ(frame.size(), kHeaderSize);
    EXPECT_EQ(parse_header(frame.data()).payload_len, 0);
}

TEST(Frame, WritesFieldsBigEndian) {
    FrameBuilder builder(MessageType::kChatNtf);
    builder.write_u8(0x01);
    builder.write_u16(0x0203);
    builder.write_u32(0x04050607);
    builder.write_u64(0x08090A0B0C0D0E0FULL);

    const Bytes frame = builder.build();

    const Bytes expected = {
        0x00, 0x0F,  // payload length
        0x80, 0x04,  // CHAT_NTF
        0x01,
        0x02, 0x03,
        0x04, 0x05, 0x06, 0x07,
        0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F,
    };
    EXPECT_EQ(frame, expected);
}

TEST(Frame, StringIsLengthPrefixedWithoutATerminator) {
    FrameBuilder builder(MessageType::kChatReq);
    builder.write_string("hi");

    const Bytes frame = builder.build();

    const Bytes expected = {0x00, 0x04, 0x00, 0x04, 0x00, 0x02, 'h', 'i'};
    EXPECT_EQ(frame, expected);
}

TEST(Frame, StringLengthCountsBytesNotCharacters) {
    FrameBuilder builder(MessageType::kChatReq);
    builder.write_string("한글");  // three UTF-8 bytes per character

    const Bytes frame = builder.build();

    EXPECT_EQ(parse_header(frame.data()).payload_len, 2 + 6);
    EXPECT_EQ(frame[kHeaderSize], 0x00);
    EXPECT_EQ(frame[kHeaderSize + 1], 6);
}

TEST(Frame, NegativeTimestampSurvivesTheRoundTrip) {
    FrameBuilder builder(MessageType::kChatNtf);
    builder.write_i64(-1234567890123LL);

    const Bytes frame = builder.build();

    ByteReader reader(frame.data() + kHeaderSize, frame.size() - kHeaderSize);
    EXPECT_EQ(reader.read_i64(), -1234567890123LL);
    EXPECT_TRUE(reader.exhausted());
}

TEST(Frame, RoundTripsAChatNotification) {
    FrameBuilder builder(MessageType::kChatNtf);
    builder.write_u64(42);
    builder.write_string("henry");
    builder.write_string("안녕");
    builder.write_i64(1723526400000LL);

    const Bytes frame = builder.build();

    const auto header = parse_header(frame.data());
    ASSERT_EQ(frame.size(), kHeaderSize + header.payload_len);

    ByteReader reader(frame.data() + kHeaderSize, header.payload_len);
    EXPECT_EQ(reader.read_u64(), 42u);
    EXPECT_EQ(reader.read_string(), "henry");
    EXPECT_EQ(reader.read_string(), "안녕");
    EXPECT_EQ(reader.read_i64(), 1723526400000LL);
    EXPECT_TRUE(reader.exhausted());
}

TEST(Frame, RejectsAStringTooLongForItsLengthPrefix) {
    FrameBuilder builder(MessageType::kChatNtf);

    EXPECT_THROW(builder.write_string(std::string(0x10000, 'x')), std::length_error);
}

}  // namespace
