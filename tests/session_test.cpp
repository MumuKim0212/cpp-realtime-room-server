#include "session/session.h"

#include <sys/socket.h>
#include <unistd.h>

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "net/event_loop.h"
#include "protocol/byte_reader.h"
#include "protocol/frame.h"
#include "socket_pair.h"

namespace {

using namespace std::chrono_literals;

using rrs::net::EventLoop;
using rrs::protocol::ByteReader;
using rrs::protocol::Bytes;
using rrs::protocol::ErrorCode;
using rrs::protocol::FrameBuilder;
using rrs::protocol::kHeaderSize;
using rrs::protocol::MessageType;
using rrs::protocol::parse_header;
using rrs::session::Session;
using rrs::test::SocketPair;

struct Received {
    std::uint16_t type;
    Bytes payload;
};

class SessionTest : public ::testing::Test {
protected:
    void start_session() {
        start_session([](Session&, std::uint16_t, const std::uint8_t*, std::uint16_t) {});
    }

    void start_session(Session::MessageCallback on_message) {
        session_ = std::make_unique<Session>(
            loop_, sockets_.release_server_fd(),
            [this, on_message](Session& session, std::uint16_t type,
                               const std::uint8_t* payload, std::uint16_t length) {
                received_.push_back(Received{type, Bytes(payload, payload + length)});
                on_message(session, type, payload, length);
            },
            [this](Session&) { closed_ = true; });
    }

    // Lets the reactor deliver whatever is pending, then returns.
    void pump(std::chrono::milliseconds duration) {
        loop_.run_after(duration, [this] { loop_.stop(); });
        loop_.run();
    }

    EventLoop loop_;
    SocketPair sockets_;
    std::unique_ptr<Session> session_;
    std::vector<Received> received_;
    bool closed_ = false;
};

Bytes join_room_frame(std::uint32_t room_id) {
    FrameBuilder builder(MessageType::kJoinRoomReq);
    builder.write_u32(room_id);
    return builder.build();
}

TEST_F(SessionTest, DeliversACompleteFrame) {
    start_session();

    sockets_.write_to_server(join_room_frame(7));
    pump(20ms);

    ASSERT_EQ(received_.size(), 1u);
    EXPECT_EQ(received_[0].type, static_cast<std::uint16_t>(MessageType::kJoinRoomReq));
    ByteReader reader(received_[0].payload.data(), received_[0].payload.size());
    EXPECT_EQ(reader.read_u32(), 7u);
    EXPECT_TRUE(reader.exhausted());
    EXPECT_FALSE(closed_);
}

TEST_F(SessionTest, WaitsForTheRestOfASplitFrame) {
    start_session();
    const Bytes frame = join_room_frame(7);

    // TCP can hand over a header with no payload behind it.
    sockets_.write_to_server(Bytes(frame.begin(), frame.begin() + kHeaderSize));
    pump(20ms);
    EXPECT_TRUE(received_.empty());

    sockets_.write_to_server(Bytes(frame.begin() + kHeaderSize, frame.end()));
    pump(20ms);

    ASSERT_EQ(received_.size(), 1u);
}

TEST_F(SessionTest, DispatchesEveryFrameThatArrivedTogether) {
    start_session();

    // Just as easily, one read can carry several whole frames.
    Bytes stream;
    for (std::uint32_t room = 1; room <= 3; ++room) {
        const Bytes frame = join_room_frame(room);
        stream.insert(stream.end(), frame.begin(), frame.end());
    }
    sockets_.write_to_server(stream);

    pump(20ms);

    ASSERT_EQ(received_.size(), 3u);
    for (std::uint32_t i = 0; i < received_.size(); ++i) {
        ByteReader reader(received_[i].payload.data(), received_[i].payload.size());
        EXPECT_EQ(reader.read_u32(), i + 1);
    }
}

TEST_F(SessionTest, HandlesAFrameWithNoPayload) {
    start_session();

    FrameBuilder builder(MessageType::kLeaveRoomReq);
    sockets_.write_to_server(builder.build());
    pump(20ms);

    ASSERT_EQ(received_.size(), 1u);
    EXPECT_TRUE(received_[0].payload.empty());
}

TEST_F(SessionTest, RejectsAServerOnlyMessageType) {
    start_session();

    // A client forging CHAT_NTF, which only the server is allowed to send.
    FrameBuilder builder(MessageType::kChatNtf);
    builder.write_u64(1);
    sockets_.write_to_server(builder.build());

    pump(20ms);

    EXPECT_TRUE(received_.empty());
    EXPECT_TRUE(closed_);

    const Bytes reply = sockets_.read_from_server();
    ASSERT_GE(reply.size(), kHeaderSize + 2);
    const auto header = parse_header(reply.data());
    EXPECT_EQ(header.type, static_cast<std::uint16_t>(MessageType::kErrorNtf));
    ByteReader reader(reply.data() + kHeaderSize, header.payload_len);
    EXPECT_EQ(reader.read_u16(), static_cast<std::uint16_t>(ErrorCode::kWrongDirection));
}

TEST_F(SessionTest, ClosesWhenThePeerDisconnects) {
    start_session();

    sockets_.close_client();
    pump(20ms);

    EXPECT_TRUE(closed_);
}

TEST_F(SessionTest, StopsDispatchingOnceACallbackCloses) {
    int handled = 0;
    start_session([&handled](Session& session, std::uint16_t, const std::uint8_t*,
                             std::uint16_t) {
        ++handled;
        session.close();
    });

    Bytes stream;
    for (int i = 0; i < 3; ++i) {
        FrameBuilder builder(MessageType::kLeaveRoomReq);
        const Bytes frame = builder.build();
        stream.insert(stream.end(), frame.begin(), frame.end());
    }
    sockets_.write_to_server(stream);

    pump(20ms);

    EXPECT_EQ(handled, 1);
    EXPECT_TRUE(closed_);
}

TEST_F(SessionTest, DoesNotTearDownUntilTheLoopRuns) {
    start_session();

    // The owner destroys the session from the close callback, so running that
    // callback inline would pull the ground out from under whoever called
    // close(). Usually that is a room broadcast, part-way through the list of
    // recipients it is sending to -- and a cascade can close a session further
    // down that same list.
    session_->close();
    EXPECT_FALSE(closed_);

    pump(20ms);

    EXPECT_TRUE(closed_);
}

TEST_F(SessionTest, WritesStraightThroughWhenTheSocketIsClear) {
    start_session();

    FrameBuilder builder(MessageType::kLoginRes);
    builder.write_u8(0);
    builder.write_u64(99);
    const Bytes frame = builder.build();

    session_->send(frame.data(), frame.size());

    EXPECT_EQ(sockets_.read_from_server(), frame);
}

TEST_F(SessionTest, BuffersAndDrainsWhatDidNotFitInOneWrite) {
    start_session();

    // More than the socket will take in one go, so the tail has to be queued
    // and flushed later, once EPOLLOUT says there is room.
    Bytes expected;
    for (std::uint64_t i = 0; i < 8; ++i) {
        FrameBuilder builder(MessageType::kChatNtf);
        builder.write_u64(i);
        builder.write_string(std::string(60000, 'x'));
        const Bytes frame = builder.build();

        session_->send(frame.data(), frame.size());
        expected.insert(expected.end(), frame.begin(), frame.end());
    }
    ASSERT_FALSE(closed_);

    // Everything the socket could take is already there. If it swallowed the
    // lot, nothing was ever queued and the rest of this test proves nothing.
    Bytes actual = sockets_.read_from_server();
    ASSERT_LT(actual.size(), expected.size());

    // Draining the client end as the loop runs is what lets the queued tail go
    // out; nothing else would make the socket writable again.
    loop_.run_every(1ms, [&] {
        const Bytes chunk = sockets_.read_from_server();
        actual.insert(actual.end(), chunk.begin(), chunk.end());
        if (actual.size() >= expected.size()) {
            loop_.stop();
        }
    });
    loop_.run_after(5s, [this] { loop_.stop(); });
    loop_.run();

    EXPECT_EQ(actual, expected);
    EXPECT_FALSE(closed_);
}

TEST_F(SessionTest, DropsAConnectionThatStopsDraining) {
    start_session();

    // The client never reads. Output piles up until the backpressure limit
    // decides the connection is not coming back, which is the alternative to
    // letting one stalled peer grow the server's memory without bound.
    FrameBuilder builder(MessageType::kChatNtf);
    builder.write_u64(0);
    builder.write_string(std::string(60000, 'x'));
    const Bytes frame = builder.build();

    for (int i = 0; i < 64; ++i) {
        session_->send(frame.data(), frame.size());
    }

    // The limit is reached during the sends, but teardown belongs to the loop,
    // so the drop only lands once it runs. The sends that follow the limit are
    // no-ops rather than further growth.
    pump(20ms);

    EXPECT_TRUE(closed_);
}

TEST_F(SessionTest, StopReadingDeliversNothingFurther) {
    start_session();

    // What shutdown does first: the connection stays up, but nothing more the
    // peer sends is turned into work the server would then have to finish.
    session_->stop_reading();
    sockets_.write_to_server(join_room_frame(7));
    pump(50ms);

    EXPECT_TRUE(received_.empty());
    EXPECT_FALSE(closed_);
}

TEST_F(SessionTest, StopReadingLeavesQueuedOutputOnItsWay) {
    start_session();

    // More than the socket takes in one go, so there is a queued tail for the
    // shutdown to be judged on.
    Bytes expected;
    for (std::uint64_t i = 0; i < 8; ++i) {
        FrameBuilder builder(MessageType::kChatNtf);
        builder.write_u64(i);
        builder.write_string(std::string(60000, 'x'));
        const Bytes frame = builder.build();

        session_->send(frame.data(), frame.size());
        expected.insert(expected.end(), frame.begin(), frame.end());
    }

    Bytes actual = sockets_.read_from_server();
    ASSERT_LT(actual.size(), expected.size());
    ASSERT_TRUE(session_->has_pending_output());

    // Reading stops, writing does not. Everything already accepted still has
    // to arrive, which is the difference between draining and dropping.
    session_->stop_reading();

    loop_.run_every(1ms, [&] {
        const Bytes chunk = sockets_.read_from_server();
        actual.insert(actual.end(), chunk.begin(), chunk.end());
        if (actual.size() >= expected.size()) {
            loop_.stop();
        }
    });
    loop_.run_after(5s, [this] { loop_.stop(); });
    loop_.run();

    EXPECT_EQ(actual, expected);
    EXPECT_FALSE(session_->has_pending_output());
    EXPECT_FALSE(closed_);
}

}  // namespace
