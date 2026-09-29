#include "room/room.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "net/event_loop.h"
#include "protocol/frame.h"
#include "room/room_manager.h"
#include "session/session.h"
#include "socket_pair.h"

namespace {

using rrs::net::EventLoop;
using rrs::protocol::Bytes;
using rrs::protocol::FrameBuilder;
using rrs::protocol::MessageType;
using rrs::room::Room;
using rrs::room::RoomManager;
using rrs::session::Session;
using rrs::test::SocketPair;

Bytes chat_frame(const std::string& text) {
    FrameBuilder builder(MessageType::kChatNtf);
    builder.write_u64(1);
    builder.write_string("henry");
    builder.write_string(text);
    builder.write_i64(0);
    return builder.build();
}

class RoomTest : public ::testing::Test {
protected:
    // A connected client whose server end is driven by a real session, so
    // broadcasts are checked against bytes that actually reached a socket.
    struct Client {
        SocketPair sockets;
        std::unique_ptr<Session> session;
    };

    Client& add_client() {
        auto client = std::make_unique<Client>();
        client->session = std::make_unique<Session>(
            loop_, client->sockets.release_server_fd(),
            [](Session&, std::uint16_t, const std::uint8_t*, std::uint16_t) {},
            [](Session&) {});
        clients_.push_back(std::move(client));
        return *clients_.back();
    }

    EventLoop loop_;
    std::vector<std::unique_ptr<Client>> clients_;
};

TEST_F(RoomTest, StartsEmpty) {
    const Room room(1, 8);

    EXPECT_EQ(room.id(), 1u);
    EXPECT_EQ(room.member_count(), 0u);
    EXPECT_FALSE(room.full());
}

TEST_F(RoomTest, JoinAddsAMember) {
    Room room(1, 8);
    Client& client = add_client();

    EXPECT_TRUE(room.join(*client.session));

    EXPECT_EQ(room.member_count(), 1u);
    EXPECT_TRUE(room.contains(*client.session));
}

TEST_F(RoomTest, JoiningTwiceDoesNotDuplicateAMember) {
    Room room(1, 8);
    Client& client = add_client();

    EXPECT_TRUE(room.join(*client.session));
    EXPECT_TRUE(room.join(*client.session));

    EXPECT_EQ(room.member_count(), 1u);

    // The real cost of a duplicate would be a member receiving everything
    // twice, so check the wire rather than just the count.
    const Bytes frame = chat_frame("hello");
    room.broadcast(frame.data(), frame.size());
    EXPECT_EQ(client.sockets.read_from_server(), frame);
}

TEST_F(RoomTest, JoinFailsWhenTheRoomIsFull) {
    Room room(1, 1);
    Client& first = add_client();
    Client& second = add_client();
    ASSERT_TRUE(room.join(*first.session));

    EXPECT_TRUE(room.full());
    EXPECT_FALSE(room.join(*second.session));
    EXPECT_EQ(room.member_count(), 1u);
}

TEST_F(RoomTest, LeaveRemovesAMember) {
    Room room(1, 8);
    Client& client = add_client();
    ASSERT_TRUE(room.join(*client.session));

    room.leave(*client.session);

    EXPECT_EQ(room.member_count(), 0u);
    EXPECT_FALSE(room.contains(*client.session));
}

TEST_F(RoomTest, LeavingWithoutHavingJoinedIsHarmless) {
    Room room(1, 8);
    Client& member = add_client();
    Client& stranger = add_client();
    ASSERT_TRUE(room.join(*member.session));

    room.leave(*stranger.session);

    EXPECT_EQ(room.member_count(), 1u);
}

TEST_F(RoomTest, BroadcastReachesEveryMember) {
    Room room(1, 8);
    Client& first = add_client();
    Client& second = add_client();
    Client& outsider = add_client();
    ASSERT_TRUE(room.join(*first.session));
    ASSERT_TRUE(room.join(*second.session));

    const Bytes frame = chat_frame("hello");
    room.broadcast(frame.data(), frame.size());

    EXPECT_EQ(first.sockets.read_from_server(), frame);
    EXPECT_EQ(second.sockets.read_from_server(), frame);
    EXPECT_TRUE(outsider.sockets.read_from_server().empty());
}

TEST_F(RoomTest, BroadcastExceptSkipsOneMember) {
    Room room(1, 8);
    Client& sender = add_client();
    Client& other = add_client();
    ASSERT_TRUE(room.join(*sender.session));
    ASSERT_TRUE(room.join(*other.session));

    const Bytes frame = chat_frame("hello");
    room.broadcast_except(*sender.session, frame.data(), frame.size());

    EXPECT_TRUE(sender.sockets.read_from_server().empty());
    EXPECT_EQ(other.sockets.read_from_server(), frame);
}

TEST_F(RoomTest, BroadcastToAnEmptyRoomDoesNothing) {
    Room room(1, 8);

    const Bytes frame = chat_frame("hello");
    room.broadcast(frame.data(), frame.size());
}

TEST(RoomManagerTest, FindReturnsNullForARoomThatWasNeverCreated) {
    RoomManager manager;

    EXPECT_EQ(manager.find(1), nullptr);
    EXPECT_EQ(manager.room_count(), 0u);
}

TEST(RoomManagerTest, CreatedRoomsAreFindable) {
    RoomManager manager;

    Room& room = manager.create(42, 8);

    EXPECT_EQ(manager.find(42), &room);
    EXPECT_EQ(room.id(), 42u);
    EXPECT_EQ(manager.room_count(), 1u);
}

TEST(RoomManagerTest, CreatingTheSameRoomTwiceReturnsTheSameRoom) {
    RoomManager manager;

    Room& first = manager.create(42, 8);
    Room& second = manager.create(42, 8);

    EXPECT_EQ(&first, &second);
    EXPECT_EQ(manager.room_count(), 1u);
}

TEST(RoomManagerTest, RoomsKeepTheirReferencesAsMoreAreCreated) {
    // Handing out Room& only works if the container does not relocate them.
    RoomManager manager;
    Room& first = manager.create(1, 8);

    for (std::uint32_t id = 2; id <= 200; ++id) {
        manager.create(id, 8);
    }

    EXPECT_EQ(&first, manager.find(1));
    EXPECT_EQ(first.id(), 1u);
}

TEST_F(RoomTest, EachRoomGetsTheCapacityItWasCreatedWith) {
    RoomManager manager;
    Room& room = manager.create(1, 1);
    Client& first = add_client();
    Client& second = add_client();

    EXPECT_TRUE(room.join(*first.session));
    EXPECT_FALSE(room.join(*second.session));
}

}  // namespace
