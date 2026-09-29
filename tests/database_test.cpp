#include "db/database.h"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "db/connection.h"
#include "net/event_loop.h"

namespace {

using namespace std::chrono_literals;

using rrs::db::Connection;
using rrs::db::Database;
using rrs::db::Result;
using rrs::net::EventLoop;

// These talk to a real PostgreSQL. Without one they skip rather than fail, so
// the suite still runs anywhere; docker/README explain how to provide one.
const char* database_url() {
    return std::getenv("RRS_TEST_DATABASE_URL");
}

class DatabaseTest : public ::testing::Test {
protected:
    void SetUp() override {
        const char* url = database_url();
        if (url == nullptr) {
            GTEST_SKIP() << "RRS_TEST_DATABASE_URL is unset, skipping";
        }
        conninfo_ = url;

        // Reruns must not collide with the rows the last one left behind. The
        // seeded rooms stay: they are part of the schema, not of a test.
        Connection connection(conninfo_);
        connection.execute("TRUNCATE chat_messages, users RESTART IDENTITY", {});
    }

    // Runs the loop until the operation under test has reported back.
    void run_until_answered() {
        loop_.run_after(30s, [this] { loop_.stop(); });  // never hang the suite
        loop_.run();
    }

    Database open(std::size_t workers = 2,
                  std::size_t queue_limit = rrs::db::WorkerPool::kDefaultQueueLimit,
                  std::size_t auths_per_worker =
                      Database::kDefaultAuthsInFlightPerWorker) {
        return Database(loop_, conninfo_, workers, queue_limit, auths_per_worker);
    }

    std::uint64_t create_user(Database& database, const std::string& username,
                              const std::string& password) {
        bool created = false;
        std::uint64_t id = 0;
        database.create_user(username, password, [&](bool ok, std::uint64_t user_id) {
            created = ok;
            id = user_id;
            loop_.stop();
        });
        run_until_answered();
        EXPECT_TRUE(created);
        return id;
    }

    std::string conninfo_;
    EventLoop loop_;
};

TEST_F(DatabaseTest, RefusesADsnItCannotConnectTo) {
    EXPECT_THROW(
        { Connection connection("host=127.0.0.1 port=1 dbname=nowhere connect_timeout=2"); },
        std::runtime_error);
}

TEST_F(DatabaseTest, ReportsAFailingStatement) {
    Connection connection(conninfo_);

    EXPECT_THROW(connection.execute("SELECT * FROM no_such_table", {}),
                 std::runtime_error);
}

TEST_F(DatabaseTest, SendsParametersApartFromTheStatement) {
    // The reason parameters are never pasted into SQL: this value would end
    // the statement and start another if it were.
    Connection connection(conninfo_);

    const Result result = connection.execute("SELECT $1::text", {"'); DROP TABLE users; --"});

    ASSERT_EQ(result.row_count(), 1);
    EXPECT_EQ(result.value(0, 0), "'); DROP TABLE users; --");
    EXPECT_EQ(connection.execute("SELECT 1 FROM users", {}).row_count(), 0);
}

TEST_F(DatabaseTest, AcceptsTheRightPassword) {
    Database database = open();
    const std::uint64_t expected_id = create_user(database, "henry", "correct horse");

    auto status = Database::AuthStatus::kUnavailable;
    std::uint64_t user_id = 0;
    database.authenticate("henry", "correct horse",
                          [&](Database::AuthStatus result, std::uint64_t id) {
                              status = result;
                              user_id = id;
                              loop_.stop();
                          });
    run_until_answered();

    EXPECT_EQ(status, Database::AuthStatus::kAccepted);
    EXPECT_EQ(user_id, expected_id);
}

TEST_F(DatabaseTest, RejectsTheWrongPassword) {
    Database database = open();
    create_user(database, "henry", "correct horse");

    auto status = Database::AuthStatus::kUnavailable;
    database.authenticate("henry", "correct horst",
                          [&](Database::AuthStatus result, std::uint64_t) {
                              status = result;
                              loop_.stop();
                          });
    run_until_answered();

    EXPECT_EQ(status, Database::AuthStatus::kRejected);
}

TEST_F(DatabaseTest, RejectsAnUnknownUser) {
    Database database = open();

    auto status = Database::AuthStatus::kUnavailable;
    database.authenticate("nobody", "whatever",
                          [&](Database::AuthStatus result, std::uint64_t) {
                              status = result;
                              loop_.stop();
                          });
    run_until_answered();

    EXPECT_EQ(status, Database::AuthStatus::kRejected);
}

TEST_F(DatabaseTest, RefusesADuplicateUsername) {
    Database database = open();
    create_user(database, "henry", "correct horse");

    bool created = true;
    database.create_user("henry", "another", [&](bool ok, std::uint64_t) {
        created = ok;
        loop_.stop();
    });
    run_until_answered();

    EXPECT_FALSE(created);
}

TEST_F(DatabaseTest, NeverStoresThePlaintextPassword) {
    Database database = open();
    create_user(database, "henry", "correct horse");

    Connection connection(conninfo_);
    const Result stored = connection.execute("SELECT password_hash FROM users", {});

    ASSERT_EQ(stored.row_count(), 1);
    EXPECT_EQ(stored.value(0, 0).find("correct horse"), std::string::npos);
}

TEST_F(DatabaseTest, LoadsTheSeededRooms) {
    Database database = open();

    bool ok = false;
    std::vector<Database::RoomRecord> rooms;
    database.load_rooms([&](bool result, std::vector<Database::RoomRecord> loaded) {
        ok = result;
        rooms = std::move(loaded);
        loop_.stop();
    });
    run_until_answered();

    ASSERT_TRUE(ok);
    ASSERT_FALSE(rooms.empty());
    EXPECT_EQ(rooms[0].id, 1u);
    EXPECT_EQ(rooms[0].name, "lobby");
    EXPECT_GT(rooms[0].capacity, 0);
}

TEST_F(DatabaseTest, StoresAChatMessage) {
    Database database = open();
    const std::uint64_t user_id = create_user(database, "henry", "correct horse");

    database.log_chat_message(1, user_id, "안녕하세요", 1723526400000LL);

    // The write reports nothing back, so wait for the row instead of a
    // callback. Its absence after this long is a failure, not slowness.
    Connection connection(conninfo_);
    for (int attempt = 0; attempt < 100; ++attempt) {
        if (connection.execute("SELECT 1 FROM chat_messages", {}).row_count() > 0) {
            break;
        }
        std::this_thread::sleep_for(50ms);
    }

    const Result rows = connection.execute(
        "SELECT room_id, user_id, body, "
        "       (EXTRACT(EPOCH FROM sent_at) * 1000)::bigint "
        "FROM chat_messages",
        {});

    ASSERT_EQ(rows.row_count(), 1);
    EXPECT_EQ(rows.value(0, 0), "1");
    EXPECT_EQ(rows.value(0, 1), std::to_string(user_id));
    EXPECT_EQ(rows.value(0, 2), "안녕하세요");
    EXPECT_EQ(rows.value(0, 3), "1723526400000");
}

TEST_F(DatabaseTest, AnswersOnTheLoopThread) {
    // Handlers touch loop-owned state, so a result arriving anywhere else
    // would need locking that nothing in this server has.
    Database database = open();

    std::thread::id answered_on;
    database.load_rooms([&](bool, std::vector<Database::RoomRecord>) {
        answered_on = std::this_thread::get_id();
        loop_.stop();
    });
    run_until_answered();

    EXPECT_EQ(answered_on, std::this_thread::get_id());
}

TEST_F(DatabaseTest, TurnsALoginAwayWhenTheWorkersAreBehind) {
    Database database = open(/*workers=*/1, /*queue_limit=*/1);
    create_user(database, "backlog", "correct horse battery");

    // Every verify is an Argon2id hash, so one worker with room for one more
    // request cannot absorb a burst -- which is the point. A login is the most
    // expensive thing an unauthenticated peer can ask for, and the refusals
    // below are what a flood of them is supposed to meet.
    constexpr int kAttempts = 40;
    int answered = 0;
    int unavailable = 0;
    for (int i = 0; i < kAttempts; ++i) {
        database.authenticate(
            "backlog", "correct horse battery",
            [&](Database::AuthStatus status, std::uint64_t) {
                if (status == Database::AuthStatus::kUnavailable) {
                    ++unavailable;
                }
                if (++answered == kAttempts) {
                    loop_.stop();
                }
            });
    }
    run_until_answered();

    // Nothing is left hanging: a request that was never queued still answers,
    // because the connection that asked is waiting on that answer to be allowed
    // to try again.
    EXPECT_EQ(answered, kAttempts);
    EXPECT_GT(unavailable, 0);
    EXPECT_GT(database.shed_logins(), 0u);
}

TEST_F(DatabaseTest, ALoginFloodDoesNotCrowdOutChatWrites) {
    // The two kinds of work are not the same size. A queue full of Argon2id
    // verifies is minutes of work where the same queue full of inserts is
    // seconds, so logins get a ceiling well under the queue's -- otherwise a
    // flood of the expensive work puts every cheap write behind it.
    //
    // The queue is sized between the two outcomes: the four logins the ceiling
    // lets in plus the hundred writes fit (104), the forty logins a missing
    // ceiling would let in plus the same writes do not (140). So it is the
    // ceiling, not a roomy queue, that keeps the writes.
    Database database = open(/*workers=*/1, /*queue_limit=*/120,
                             /*auths_per_worker=*/4);
    const std::uint64_t user = create_user(database, "flooder", "secret");

    for (int i = 0; i < 40; ++i) {
        database.authenticate("flooder", "secret",
                              [](Database::AuthStatus, std::uint64_t) {});
    }
    ASSERT_GT(database.shed_logins(), 0u);

    for (int i = 0; i < 100; ++i) {
        database.log_chat_message(1, user, "still logged", 1700000000000);
    }

    EXPECT_EQ(database.dropped_chat_writes(), 0u);
}

TEST_F(DatabaseTest, DropsAChatWriteWhenTheWorkersAreBehind) {
    Database database = open(/*workers=*/1, /*queue_limit=*/1);
    const std::uint64_t user = create_user(database, "chatter", "secret");

    // Nothing waits on a chat write -- the message went out before it was
    // queued -- so a burst that outruns the worker is dropped where it can be
    // counted, rather than growing a queue that outlives the burst.
    for (int i = 0; i < 500; ++i) {
        database.log_chat_message(1, user, "hello", 1700000000000);
    }

    EXPECT_GT(database.dropped_chat_writes(), 0u);
}

}  // namespace
