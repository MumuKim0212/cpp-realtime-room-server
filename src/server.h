#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>

#include "db/database.h"
#include "net/acceptor.h"
#include "net/event_loop.h"
#include "protocol/byte_reader.h"
#include "protocol/message.h"
#include "room/room_manager.h"
#include "session/session.h"

namespace rrs {

// Ties the pieces together: the acceptor makes sessions, sessions deliver
// frames here, and this is where they become logins, room membership and
// chat. Lives on one event loop and owns everything it touches, so none of it
// is shared and none of it is locked.
class Server {
public:
    struct Config {
        std::string listen_host;
        std::uint16_t listen_port = 0;
        std::string database_url;
        std::size_t database_workers = 0;

        // Connections held at once. Every one costs a descriptor, a session and
        // its buffers, so the ceiling is a decision worth making on purpose
        // rather than discovering as EMFILE -- which the acceptor can only
        // answer by pausing and trying again, over and over. Must sit below the
        // process descriptor limit, with room for the listening socket, the
        // database connections and the signal descriptor.
        std::size_t max_connections = 4096;

        // How long a connection may go without logging in. Kept apart from the
        // session's idle timeout, which is about a connection that went quiet
        // after doing something: this is about one that never started, and the
        // two deserve very different patience. Without it an unauthenticated
        // peer holds a descriptor and a session for the whole idle window, and
        // can hold it indefinitely by pinging.
        net::Duration login_deadline = std::chrono::seconds(30);
    };

    Server(net::EventLoop& loop, const Config& config);
    ~Server();

    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    // Loads the room list, then starts listening. In that order, because a
    // client cannot be told a room does not exist while the server is still
    // finding out which ones do.
    void start();

    // Zero until the room list has loaded and the listening socket is up.
    // Asking for port 0 and reading it back here is how tests find the server.
    std::uint16_t port() const;

    // True when start() gave up instead of reaching the point of listening.
    // The loop stops either way, so this is what tells the caller whether it
    // stopped because it was asked to or because there was nothing to serve.
    bool startup_failed() const { return startup_failed_; }

    // Stops the server taking anything on: the listening socket goes, and the
    // connections that remain stop being read. What is already queued for them
    // is left alone, so a caller that keeps the loop running a moment longer
    // gives it a chance to arrive.
    void begin_shutdown();

    // True while any client still has output waiting to reach it.
    bool has_pending_output() const;

    // Connections turned away because the server was already full.
    std::size_t refused_connections() const { return refused_connections_; }

private:
    // Identifies a client across an asynchronous database call, which a raw
    // pointer could not: the connection may be gone by the time the answer
    // comes back, and its address may already belong to someone else.
    using ClientId = std::uint64_t;

    struct Client {
        std::unique_ptr<session::Session> session;
        std::uint64_t user_id = 0;
        std::string username;
        std::optional<std::uint32_t> room_id;
        bool logged_in = false;
        bool login_in_flight = false;

        // Cancelled the moment the login lands. Non-zero means one is still
        // armed and has to be called off before this client goes away, because
        // it captures an id this server would then look up in a map it no
        // longer has.
        net::TimerId login_deadline = 0;

        // Set once the deadline has been pushed out for a login in flight. One
        // extension covers the wait the server imposes -- the database sheds
        // logins it could not answer within a deadline -- and a second would
        // let a peer that keeps a failing login in flight stay forever.
        bool login_deadline_extended = false;
    };

    void accept_connection(int fd);
    void drop_client(ClientId id);
    Client* find_client(ClientId id);

    // Ends a connection that never logged in. Rearms itself once instead while
    // an answer is still on its way, so a client is never hung up on for a wait
    // the server is the one imposing.
    void enforce_login_deadline(ClientId id);
    void cancel_login_deadline(Client& client);

    void handle_message(ClientId id, session::Session& session, std::uint16_t type,
                        const std::uint8_t* payload, std::uint16_t length);
    void handle_login(ClientId id, session::Session& session, protocol::ByteReader& reader);
    void handle_join(ClientId id, session::Session& session, protocol::ByteReader& reader);
    void handle_leave(ClientId id, session::Session& session, protocol::ByteReader& reader);
    void handle_chat(ClientId id, session::Session& session, protocol::ByteReader& reader);

    // Takes no client id: a ping says nothing about the connection except that
    // it is still there, which the session has already recorded by the time
    // this runs.
    void handle_ping(session::Session& session, protocol::ByteReader& reader);

    // Takes the client out of whatever room it is in and tells the room.
    void leave_current_room(Client& client);

    net::EventLoop& loop_;
    Config config_;
    db::Database database_;
    room::RoomManager rooms_;
    std::unique_ptr<net::Acceptor> acceptor_;
    std::unordered_map<ClientId, Client> clients_;
    ClientId next_client_id_ = 1;
    std::size_t refused_connections_ = 0;
    bool startup_failed_ = false;
};

}  // namespace rrs
