#include "server.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "db/connection.h"
#include "db/password.h"
#include "net/event_loop.h"
#include "protocol/byte_reader.h"
#include "protocol/frame.h"

namespace {

using namespace std::chrono_literals;

using rrs::Server;
using rrs::db::Connection;
using rrs::net::EventLoop;
using rrs::protocol::ByteReader;
using rrs::protocol::Bytes;
using rrs::protocol::ErrorCode;
using rrs::protocol::FrameBuilder;
using rrs::protocol::kHeaderSize;
using rrs::protocol::kProtocolVersion;
using rrs::protocol::MessageType;
using rrs::protocol::parse_header;

// A blocking client that speaks the wire protocol, driven from a thread of its
// own while the server's loop runs on the main one. Nothing here shares state
// with the server; it talks to a socket like any other client would.
class TestClient {
public:
    explicit TestClient(std::uint16_t port) {
        fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        EXPECT_GE(fd_, 0);

        // A reply that never comes must fail the test rather than hang it.
        timeval timeout{};
        timeout.tv_sec = 10;
        ::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_port = ::htons(port);
        EXPECT_EQ(::inet_pton(AF_INET, "127.0.0.1", &address.sin_addr), 1);
        EXPECT_EQ(::connect(fd_, reinterpret_cast<const sockaddr*>(&address),
                            sizeof(address)),
                  0);
    }

    ~TestClient() {
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }

    TestClient(const TestClient&) = delete;
    TestClient& operator=(const TestClient&) = delete;

    void send_frame(const Bytes& frame) {
        EXPECT_EQ(::send(fd_, frame.data(), frame.size(), 0),
                  static_cast<ssize_t>(frame.size()));
    }

    void login(const std::string& username, const std::string& password) {
        FrameBuilder builder(MessageType::kLoginReq);
        builder.write_u16(kProtocolVersion);
        builder.write_string(username);
        builder.write_string(password);
        send_frame(builder.build());
    }

    void join(std::uint32_t room_id) {
        FrameBuilder builder(MessageType::kJoinRoomReq);
        builder.write_u32(room_id);
        send_frame(builder.build());
    }

    void leave() { send_frame(FrameBuilder(MessageType::kLeaveRoomReq).build()); }

    void ping() { send_frame(FrameBuilder(MessageType::kPingReq).build()); }

    void chat(const std::string& text) {
        FrameBuilder builder(MessageType::kChatReq);
        builder.write_string(text);
        send_frame(builder.build());
    }

    struct Frame {
        std::uint16_t type = 0;
        Bytes payload;
    };

    // Reads exactly one frame. An empty type means the read failed or timed out.
    Frame read_frame() {
        Bytes header;
        if (!read_exactly(kHeaderSize, header)) {
            return {};
        }

        const auto parsed = parse_header(header.data());
        Frame frame;
        frame.type = parsed.type;
        if (parsed.payload_len > 0 && !read_exactly(parsed.payload_len, frame.payload)) {
            return {};
        }
        return frame;
    }

private:
    bool read_exactly(std::size_t count, Bytes& out) {
        out.resize(count);
        std::size_t filled = 0;
        while (filled < count) {
            const ssize_t received = ::recv(fd_, out.data() + filled, count - filled, 0);
            if (received <= 0) {
                return false;
            }
            filled += static_cast<std::size_t>(received);
        }
        return true;
    }

    int fd_ = -1;
};

const char* database_url() {
    return std::getenv("RRS_TEST_DATABASE_URL");
}

class ServerTest : public ::testing::Test {
protected:
    // Lets a fixture below change one thing about the server under test without
    // repeating the startup dance. The limits are configuration precisely so
    // that a test does not have to wait out a production timeout to see them.
    virtual void adjust(Server::Config&) {}

    void SetUp() override {
        const char* url = database_url();
        if (url == nullptr) {
            GTEST_SKIP() << "RRS_TEST_DATABASE_URL is unset, skipping";
        }
        conninfo_ = url;

        Connection connection(conninfo_);
        connection.execute("TRUNCATE chat_messages, users RESTART IDENTITY", {});

        Server::Config config;
        config.listen_host = "127.0.0.1";
        config.listen_port = 0;  // the kernel picks; port() reports it back
        config.database_url = conninfo_;
        config.database_workers = 2;
        adjust(config);

        server_ = std::make_unique<Server>(loop_, config);
        server_->start();

        // Listening only begins once the room list has loaded.
        const rrs::net::TimerId waiting = loop_.run_every(1ms, [this] {
            if (server_->port() != 0) {
                loop_.stop();
            }
        });
        const rrs::net::TimerId deadline = loop_.run_after(30s, [this] { loop_.stop(); });
        loop_.run();

        // Both have to go. Either one left pending would fire during the test
        // itself and stop the loop out from under the conversation.
        loop_.cancel_timer(waiting);
        loop_.cancel_timer(deadline);

        ASSERT_NE(server_->port(), 0);
    }

    void create_account(const std::string& username, const std::string& password) {
        Connection connection(conninfo_);
        connection.execute("INSERT INTO users (username, password_hash) VALUES ($1, $2)",
                           {username, rrs::db::hash_password(password)});
    }

    // Runs `conversation` on its own thread while the server's loop runs here.
    void converse(std::function<void()> conversation) {
        std::thread client([this, conversation = std::move(conversation)] {
            conversation();
            loop_.post([this] { loop_.stop(); });
        });
        loop_.run_after(30s, [this] { loop_.stop(); });
        loop_.run();
        client.join();
    }

    std::uint16_t port() const { return server_->port(); }

    std::string conninfo_;
    EventLoop loop_;
    std::unique_ptr<Server> server_;
};

std::uint8_t result_code(const TestClient::Frame& frame) {
    ByteReader reader(frame.payload.data(), frame.payload.size());
    return reader.read_u8();
}

TEST_F(ServerTest, AcceptsALoginAndReportsTheUserId) {
    create_account("henry", "correct horse");

    TestClient::Frame reply;
    converse([&] {
        TestClient client(port());
        client.login("henry", "correct horse");
        reply = client.read_frame();
    });

    EXPECT_EQ(reply.type, static_cast<std::uint16_t>(MessageType::kLoginRes));
    ByteReader reader(reply.payload.data(), reply.payload.size());
    EXPECT_EQ(reader.read_u8(), 0);       // accepted
    EXPECT_GT(reader.read_u64(), 0u);     // a real user id
    EXPECT_TRUE(reader.exhausted());
}

TEST_F(ServerTest, RejectsAWrongPassword) {
    create_account("henry", "correct horse");

    TestClient::Frame reply;
    converse([&] {
        TestClient client(port());
        client.login("henry", "correct horst");
        reply = client.read_frame();
    });

    EXPECT_EQ(reply.type, static_cast<std::uint16_t>(MessageType::kLoginRes));
    EXPECT_EQ(result_code(reply), 1);  // bad credentials
}

TEST_F(ServerTest, RefusesToJoinBeforeLogin) {
    TestClient::Frame reply;
    converse([&] {
        TestClient client(port());
        client.join(1);
        reply = client.read_frame();
    });

    EXPECT_EQ(reply.type, static_cast<std::uint16_t>(MessageType::kJoinRoomRes));
    EXPECT_EQ(result_code(reply), 3);  // state violation
}

TEST_F(ServerTest, RefusesToJoinARoomThatDoesNotExist) {
    create_account("henry", "correct horse");

    TestClient::Frame reply;
    converse([&] {
        TestClient client(port());
        client.login("henry", "correct horse");
        client.read_frame();
        client.join(9999);
        reply = client.read_frame();
    });

    EXPECT_EQ(reply.type, static_cast<std::uint16_t>(MessageType::kJoinRoomRes));
    EXPECT_EQ(result_code(reply), 2);  // no such target
}

TEST_F(ServerTest, ClosesAConnectionSpeakingTheWrongProtocolVersion) {
    TestClient::Frame reply;
    converse([&] {
        TestClient client(port());

        FrameBuilder builder(MessageType::kLoginReq);
        builder.write_u16(kProtocolVersion + 1);
        builder.write_string("henry");
        builder.write_string("correct horse");
        client.send_frame(builder.build());

        reply = client.read_frame();
    });

    EXPECT_EQ(reply.type, static_cast<std::uint16_t>(MessageType::kErrorNtf));
    ByteReader reader(reply.payload.data(), reply.payload.size());
    EXPECT_EQ(reader.read_u16(),
              static_cast<std::uint16_t>(ErrorCode::kProtocolVersionMismatch));
}

TEST_F(ServerTest, AnnouncesAJoinToTheRoomButNotToTheJoiner) {
    create_account("first", "pw");
    create_account("second", "pw");

    TestClient::Frame seen_by_first;
    TestClient::Frame joiners_own_reply;
    std::uint16_t member_count = 0;

    converse([&] {
        TestClient first(port());
        first.login("first", "pw");
        first.read_frame();
        first.join(1);
        first.read_frame();  // its own JOIN_ROOM_RES

        TestClient second(port());
        second.login("second", "pw");
        second.read_frame();
        second.join(1);
        joiners_own_reply = second.read_frame();

        seen_by_first = first.read_frame();

        ByteReader reader(joiners_own_reply.payload.data(),
                          joiners_own_reply.payload.size());
        reader.read_u8();
        reader.read_u32();
        member_count = reader.read_u16();
    });

    EXPECT_EQ(joiners_own_reply.type, static_cast<std::uint16_t>(MessageType::kJoinRoomRes));
    EXPECT_EQ(result_code(joiners_own_reply), 0);
    EXPECT_EQ(member_count, 2);

    EXPECT_EQ(seen_by_first.type, static_cast<std::uint16_t>(MessageType::kUserJoinedNtf));
    ByteReader reader(seen_by_first.payload.data(), seen_by_first.payload.size());
    reader.read_u64();
    EXPECT_EQ(reader.read_string(), "second");
}

TEST_F(ServerTest, ChatReachesEveryoneInTheRoomIncludingTheSender) {
    create_account("first", "pw");
    create_account("second", "pw");

    TestClient::Frame senders_copy;
    TestClient::Frame others_copy;

    converse([&] {
        TestClient first(port());
        first.login("first", "pw");
        first.read_frame();
        first.join(1);
        first.read_frame();

        TestClient second(port());
        second.login("second", "pw");
        second.read_frame();
        second.join(1);
        second.read_frame();

        first.read_frame();  // USER_JOINED_NTF for second

        second.chat("안녕하세요");
        senders_copy = second.read_frame();
        others_copy = first.read_frame();
    });

    for (const TestClient::Frame* frame : {&senders_copy, &others_copy}) {
        EXPECT_EQ(frame->type, static_cast<std::uint16_t>(MessageType::kChatNtf));
        ByteReader reader(frame->payload.data(), frame->payload.size());
        reader.read_u64();
        EXPECT_EQ(reader.read_string(), "second");
        EXPECT_EQ(reader.read_string(), "안녕하세요");
        EXPECT_GT(reader.read_i64(), 0);
        EXPECT_TRUE(reader.exhausted());
    }
}

TEST_F(ServerTest, RecordsChatInTheDatabase) {
    create_account("henry", "pw");

    converse([&] {
        TestClient client(port());
        client.login("henry", "pw");
        client.read_frame();
        client.join(1);
        client.read_frame();
        client.chat("logged");
        client.read_frame();  // the broadcast comes back before the write lands
    });

    Connection connection(conninfo_);
    for (int attempt = 0; attempt < 100; ++attempt) {
        if (connection.execute("SELECT 1 FROM chat_messages", {}).row_count() > 0) {
            break;
        }
        std::this_thread::sleep_for(50ms);
    }

    const auto rows = connection.execute("SELECT room_id, body FROM chat_messages", {});
    ASSERT_EQ(rows.row_count(), 1);
    EXPECT_EQ(rows.value(0, 0), "1");
    EXPECT_EQ(rows.value(0, 1), "logged");
}

TEST_F(ServerTest, TellsTheRoomWhenSomeoneLeaves) {
    create_account("first", "pw");
    create_account("second", "pw");

    TestClient::Frame left_notice;
    TestClient::Frame leave_reply;

    converse([&] {
        TestClient first(port());
        first.login("first", "pw");
        first.read_frame();
        first.join(1);
        first.read_frame();

        TestClient second(port());
        second.login("second", "pw");
        second.read_frame();
        second.join(1);
        second.read_frame();

        first.read_frame();  // USER_JOINED_NTF

        second.leave();
        leave_reply = second.read_frame();
        left_notice = first.read_frame();
    });

    EXPECT_EQ(leave_reply.type, static_cast<std::uint16_t>(MessageType::kLeaveRoomRes));
    EXPECT_EQ(result_code(leave_reply), 0);
    EXPECT_EQ(left_notice.type, static_cast<std::uint16_t>(MessageType::kUserLeftNtf));
}

TEST_F(ServerTest, AnswersAPingBeforeAnyLogin) {
    // The idle timeout applies from the moment a connection is accepted, so a
    // client has to be able to hold it open before it has an account attached.
    TestClient::Frame reply;
    converse([&] {
        TestClient client(port());
        client.ping();
        reply = client.read_frame();
    });

    EXPECT_EQ(reply.type, static_cast<std::uint16_t>(MessageType::kPongNtf));
    EXPECT_TRUE(reply.payload.empty());
}

TEST_F(ServerTest, RejectsAnUnknownMessageType) {
    TestClient::Frame reply;
    converse([&] {
        TestClient client(port());

        // In the client band, but nothing this server answers to.
        FrameBuilder builder(static_cast<MessageType>(0x0077));
        client.send_frame(builder.build());

        reply = client.read_frame();
    });

    EXPECT_EQ(reply.type, static_cast<std::uint16_t>(MessageType::kErrorNtf));
    ByteReader reader(reply.payload.data(), reply.payload.size());
    EXPECT_EQ(reader.read_u16(),
              static_cast<std::uint16_t>(ErrorCode::kUnknownMessageType));
}

// One connection is the whole allowance, so the second one is the one that
// meets the ceiling.
class FullServerTest : public ServerTest {
protected:
    void adjust(Server::Config& config) override { config.max_connections = 1; }
};

TEST_F(FullServerTest, TurnsAConnectionAwayOnceItIsFull) {
    TestClient::Frame refusal;
    TestClient::Frame after_refusal;
    TestClient::Frame still_serving;
    converse([&] {
        TestClient first(port());
        // Answered before the second one connects, so the two do not race for
        // the single slot.
        first.ping();
        ASSERT_EQ(first.read_frame().type,
                  static_cast<std::uint16_t>(MessageType::kPongNtf));

        TestClient second(port());
        refusal = second.read_frame();
        after_refusal = second.read_frame();  // the server hangs up behind it

        // What the ceiling is for: the connection already inside is not the one
        // that pays for the one arriving.
        first.ping();
        still_serving = first.read_frame();
    });

    EXPECT_EQ(refusal.type, static_cast<std::uint16_t>(MessageType::kErrorNtf));
    ByteReader reader(refusal.payload.data(), refusal.payload.size());
    EXPECT_EQ(reader.read_u16(),
              static_cast<std::uint16_t>(ErrorCode::kConnectionRefused));

    // Told rather than dropped in silence, and then dropped.
    EXPECT_EQ(after_refusal.type, 0);
    EXPECT_EQ(still_serving.type, static_cast<std::uint16_t>(MessageType::kPongNtf));
    EXPECT_EQ(server_->refused_connections(), 1u);
}

// Short enough that a test can wait it out, which is the only thing separating
// it from what a deployed server uses.
class ImpatientServerTest : public ServerTest {
protected:
    void adjust(Server::Config& config) override { config.login_deadline = 100ms; }
};

TEST_F(ImpatientServerTest, HangsUpOnAConnectionThatNeverLogsIn) {
    // A descriptor and a session held by a peer that never identified itself is
    // exactly what an unauthenticated flood costs, and pinging must not renew
    // the lease -- the idle timeout is a different question from this one.
    TestClient::Frame refusal;
    TestClient::Frame after_refusal;
    converse([&] {
        TestClient client(port());
        client.ping();
        ASSERT_EQ(client.read_frame().type,
                  static_cast<std::uint16_t>(MessageType::kPongNtf));

        refusal = client.read_frame();
        after_refusal = client.read_frame();
    });

    EXPECT_EQ(refusal.type, static_cast<std::uint16_t>(MessageType::kErrorNtf));
    ByteReader reader(refusal.payload.data(), refusal.payload.size());
    EXPECT_EQ(reader.read_u16(),
              static_cast<std::uint16_t>(ErrorCode::kConnectionRefused));
    EXPECT_EQ(after_refusal.type, 0);
}

TEST_F(ImpatientServerTest, HangsUpOnAConnectionThatKeepsFailingToLogIn) {
    // A peer that sends the next wrong password the moment the last one is
    // turned down almost always has a login in flight when the deadline comes
    // round. The extension that spares a client the server's own backlog must
    // not become a way to hold the connection, one Argon2 hash at a time.
    create_account("henry", "correct horse");

    int attempts = 0;
    TestClient::Frame last;
    converse([&] {
        TestClient client(port());
        while (attempts < 30) {
            client.login("henry", "correct horst");
            ++attempts;
            last = client.read_frame();
            if (last.type != static_cast<std::uint16_t>(MessageType::kLoginRes)) {
                break;
            }
        }
    });

    // Each attempt is a hash, about a tenth of a second, so thirty of them is
    // many more deadlines than the one extension a connection is owed.
    EXPECT_LT(attempts, 30);
    EXPECT_NE(last.type, static_cast<std::uint16_t>(MessageType::kLoginRes));
}

TEST_F(ImpatientServerTest, LeavesAConnectionThatLoggedInAlone) {
    // The deadline is called off at the login, not merely pushed out: a session
    // that got in has to survive however many windows go by.
    create_account("patient", "correct horse");

    TestClient::Frame reply;
    converse([&] {
        TestClient client(port());
        client.login("patient", "correct horse");
        ASSERT_EQ(client.read_frame().type,
                  static_cast<std::uint16_t>(MessageType::kLoginRes));

        std::this_thread::sleep_for(500ms);  // five deadlines' worth
        client.ping();
        reply = client.read_frame();
    });

    EXPECT_EQ(reply.type, static_cast<std::uint16_t>(MessageType::kPongNtf));
}

}  // namespace
