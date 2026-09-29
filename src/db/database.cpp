#include "db/database.h"

#include <cstdio>
#include <exception>
#include <utility>

#include "db/connection.h"
#include "db/password.h"

namespace rrs::db {
namespace {

// There is no logging facility yet, and swallowing a database failure without
// a trace would make it undiagnosable.
void report(const char* what, const std::exception& error) {
    std::fprintf(stderr, "database: %s failed: %s\n", what, error.what());
}

// Work turned away because the workers were behind. The first one is the news;
// after that the load is the news, so this reports the first and then every
// thousandth with the running total, rather than a line per refusal on a stderr
// somebody is trying to read.
void report_shed(const char* what, std::size_t count) {
    if (count == 1 || count % 1000 == 0) {
        std::fprintf(stderr,
                     "database: %s turned away, workers are behind (%zu so far)\n",
                     what, count);
    }
}

}  // namespace

Database::Database(net::EventLoop& loop, const std::string& conninfo,
                   std::size_t worker_count, std::size_t queue_limit,
                   std::size_t auths_in_flight_per_worker)
    : loop_(loop),
      max_auths_in_flight_(worker_count * auths_in_flight_per_worker),
      workers_(worker_count, queue_limit) {
    connections_.reserve(worker_count);
    for (std::size_t i = 0; i < worker_count; ++i) {
        connections_.push_back(std::make_unique<Connection>(conninfo));
    }
}

Database::~Database() = default;

Connection& Database::connection_for(std::size_t worker_index) {
    return *connections_[worker_index];
}

void Database::shed_login(AuthCallback on_result) {
    report_shed("a login", ++shed_logins_);

    // Posted rather than called here, so that a refusal reaches the caller the
    // same way an answer does -- on a later turn of the loop, and never partway
    // through the handler that asked for it.
    loop_.post([on_result = std::move(on_result)] {
        on_result(AuthStatus::kUnavailable, 0);
    });
}

void Database::authenticate(std::string username, std::string password,
                            AuthCallback on_result) {
    if (auths_in_flight_ >= max_auths_in_flight_) {
        shed_login(std::move(on_result));
        return;
    }

    // Copied rather than moved into the task, because the refusal path below
    // still has to answer whoever asked.
    const bool queued = workers_.try_submit([this, username = std::move(username),
                                             password = std::move(password),
                                             on_result](std::size_t worker) mutable {
        AuthStatus status = AuthStatus::kUnavailable;
        std::uint64_t user_id = 0;

        try {
            const Result rows = connection_for(worker).execute(
                "SELECT id, password_hash FROM users WHERE username = $1",
                {username});

            if (rows.empty()) {
                // A real implementation would hash against a dummy value here
                // so the reply takes the same time whether or not the name
                // exists. Left out deliberately: a login for a name that does
                // not exist costs one query today and would cost a full hash,
                // so every made-up name would hold a worker for as long as a
                // real login does. This server is not exposed to the internet.
                status = AuthStatus::kRejected;
            } else if (verify_password(password, rows.value(0, 1))) {
                user_id = std::stoull(rows.value(0, 0));
                status = AuthStatus::kAccepted;
            } else {
                status = AuthStatus::kRejected;
            }
        } catch (const std::exception& error) {
            report("authenticate", error);
            status = AuthStatus::kUnavailable;
        }

        loop_.post([this, on_result = std::move(on_result), status, user_id] {
            --auths_in_flight_;
            on_result(status, user_id);
        });
    });

    if (!queued) {
        shed_login(std::move(on_result));
        return;
    }

    // Counted after the hand-off, which is safe because the matching decrement
    // is posted back to this thread and so cannot run before this returns.
    ++auths_in_flight_;
}

void Database::load_rooms(RoomsCallback on_result) {
    workers_.submit([this, on_result = std::move(on_result)](std::size_t worker) mutable {
        bool ok = false;
        std::vector<RoomRecord> rooms;

        try {
            const Result result = connection_for(worker).execute(
                "SELECT id, name, capacity FROM rooms ORDER BY id", {});

            for (int row = 0; row < result.row_count(); ++row) {
                rooms.push_back(RoomRecord{
                    static_cast<std::uint32_t>(std::stoul(result.value(row, 0))),
                    result.value(row, 1),
                    static_cast<std::uint16_t>(std::stoul(result.value(row, 2))),
                });
            }
            ok = true;
        } catch (const std::exception& error) {
            report("load_rooms", error);
            rooms.clear();
        }

        loop_.post([on_result = std::move(on_result), ok,
                    rooms = std::move(rooms)]() mutable {
            on_result(ok, std::move(rooms));
        });
    });
}

void Database::create_user(std::string username, std::string password,
                           CreateUserCallback on_result) {
    workers_.submit([this, username = std::move(username),
                     password = std::move(password),
                     on_result = std::move(on_result)](std::size_t worker) mutable {
        bool ok = false;
        std::uint64_t user_id = 0;

        try {
            const Result result = connection_for(worker).execute(
                "INSERT INTO users (username, password_hash) VALUES ($1, $2) "
                "RETURNING id",
                {username, hash_password(password)});

            if (!result.empty()) {
                user_id = std::stoull(result.value(0, 0));
                ok = true;
            }
        } catch (const std::exception& error) {
            // A duplicate username lands here too, which is the expected way
            // for this to fail rather than an exceptional one.
            report("create_user", error);
        }

        loop_.post([on_result = std::move(on_result), ok, user_id] {
            on_result(ok, user_id);
        });
    });
}

void Database::log_chat_message(std::uint32_t room_id, std::uint64_t user_id,
                                std::string body, std::int64_t sent_at_ms) {
    const bool queued = workers_.try_submit([this, room_id, user_id,
                                             body = std::move(body),
                                             sent_at_ms](std::size_t worker) {
        try {
            connection_for(worker).execute(
                "INSERT INTO chat_messages (room_id, user_id, body, sent_at) "
                "VALUES ($1::bigint, $2::bigint, $3, "
                "        to_timestamp($4::bigint / 1000.0))",
                {std::to_string(room_id), std::to_string(user_id), body,
                 std::to_string(sent_at_ms)});
        } catch (const std::exception& error) {
            // Nothing to tell the sender, who watched the message go out
            // before this was queued. Losing the log row is the lesser loss.
            report("log_chat_message", error);
        }
    });

    if (!queued) {
        // Same lesser loss, one step earlier: the row is gone whether the write
        // fails or never starts, and the sender was never waiting on it.
        report_shed("a chat write", ++dropped_chat_writes_);
    }
}

}  // namespace rrs::db
