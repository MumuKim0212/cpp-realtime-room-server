#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "db/worker_pool.h"
#include "net/event_loop.h"

namespace rrs::db {

class Connection;

// Every database call the server makes, run on workers and answered on the
// event loop. Nothing here blocks the caller, and no callback runs anywhere
// but the loop thread, so handlers can touch loop-owned state freely.
//
// The answers a worker posts hold a reference to this object and to whoever
// asked, which is what makes the destruction order load-bearing: **the loop
// must not be run again once a Database has been destroyed.** A worker joined
// during that destruction can post one last answer, and the loop is what would
// call it. Nothing here can enforce that -- the loop outlives the Database by
// construction and has no way to know which of its queued callbacks are stale
// -- so it is the owner's to keep. main() keeps it by never running the loop
// after the drain; the tests keep it by letting the fixture's loop go away
// unrun. See the destruction note on workers_ below for the other half.
class Database {
public:
    enum class AuthStatus {
        kAccepted,     // the credentials were good
        kRejected,     // no such user, or the wrong password
        kUnavailable,  // the database could not answer at all
    };

    struct RoomRecord {
        std::uint32_t id;
        std::string name;
        std::uint16_t capacity;
    };

    using AuthCallback = std::function<void(AuthStatus, std::uint64_t user_id)>;
    using RoomsCallback = std::function<void(bool ok, std::vector<RoomRecord>)>;
    using CreateUserCallback = std::function<void(bool ok, std::uint64_t user_id)>;

    // Logins allowed to be outstanding, per worker. Picked as a wait rather
    // than a count: an Argon2id verify runs about a tenth of a second, so 300
    // of them per worker is thirty seconds of hashing however many workers
    // there are -- which is also where the server's login deadline gives up.
    // Queueing past that would be promising an answer for a connection that
    // will not be there to hear it.
    static constexpr std::size_t kDefaultAuthsInFlightPerWorker = 300;

    // Opens one connection per worker up front, so a database that cannot be
    // reached is a startup failure rather than a surprise at the first login.
    // Throws std::runtime_error if any of them fails.
    //
    // `queue_limit` is how much work may wait for a worker before further
    // requests are turned away. The defaults suit a server; tests lower them to
    // reach the refusal paths without having to bury a real database.
    Database(net::EventLoop& loop, const std::string& conninfo,
             std::size_t worker_count,
             std::size_t queue_limit = WorkerPool::kDefaultQueueLimit,
             std::size_t auths_in_flight_per_worker =
                 kDefaultAuthsInFlightPerWorker);
    ~Database();

    Database(const Database&) = delete;
    Database& operator=(const Database&) = delete;

    // Looks the user up and checks the password, both on a worker, because the
    // hash comparison alone is tens of milliseconds of deliberate work.
    //
    // Answers kUnavailable when too many are already in flight, or when the
    // workers are backed up for any other reason. A login is the most expensive
    // thing an unauthenticated peer can ask for -- an Argon2id verify is tenths
    // of a second and tens of megabytes, on purpose -- so a flood of them is
    // shed here rather than queued: the callback still runs, and it says the
    // same thing it says when the database itself cannot answer.
    //
    // Logins get a ceiling of their own, well below the queue's, because the
    // two kinds of work are not the same size. A queue full of hashes is
    // minutes of work; the same queue full of inserts is seconds. Letting
    // logins claim the whole queue would put every chat write behind them.
    void authenticate(std::string username, std::string password,
                      AuthCallback on_result);

    void load_rooms(RoomsCallback on_result);

    // Creates an account. The protocol has no sign-up message, so this exists
    // for seeding and for tests rather than for clients.
    void create_user(std::string username, std::string password,
                     CreateUserCallback on_result);

    // Recorded after the fact and with no callback: the message was broadcast
    // before this was queued, and losing a log row must not cost the sender a
    // message they already watched go out.
    //
    // Dropped outright when the workers cannot keep up, for the same reason.
    // Chat arrives as fast as clients care to send it while a row lands at
    // whatever rate the database manages, and the gap between those two has to
    // be absorbed somewhere; a queue that grows to hide it only moves the
    // failure to the moment memory runs out. See dropped_chat_writes().
    void log_chat_message(std::uint32_t room_id, std::uint64_t user_id,
                          std::string body, std::int64_t sent_at_ms);

    // What the server turned away because the workers were behind, rather than
    // queued out of sight. Both are counted on the loop thread, which is the
    // only thread that asks for either.
    std::size_t dropped_chat_writes() const { return dropped_chat_writes_; }
    std::size_t shed_logins() const { return shed_logins_; }

private:
    Connection& connection_for(std::size_t worker_index);

    // Answers a login the server declined to start.
    void shed_login(AuthCallback on_result);

    net::EventLoop& loop_;
    std::vector<std::unique_ptr<Connection>> connections_;

    // Logins handed to the workers and not yet answered, against the ceiling
    // above. Both live on the loop thread: one goes up where a login is asked
    // for, the other comes down where its answer is delivered, and those are
    // the same thread. The decrement is the reason a posted answer reaches back
    // into this object at all, and so the reason for the note on the class.
    std::size_t auths_in_flight_ = 0;
    std::size_t max_auths_in_flight_ = 0;

    std::size_t dropped_chat_writes_ = 0;
    std::size_t shed_logins_ = 0;

    // Declared last so it is destroyed first: the workers have to be joined
    // before the connections they are holding go away.
    WorkerPool workers_;
};

}  // namespace rrs::db
