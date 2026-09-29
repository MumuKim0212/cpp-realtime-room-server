#include "server.h"

#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <exception>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "protocol/frame.h"

namespace rrs {
namespace {

using protocol::Bytes;
using protocol::ErrorCode;
using protocol::FrameBuilder;
using protocol::MessageType;

// The `result` byte carried by every *_RES message.
enum Result : std::uint8_t {
    kOk = 0,
    kBadCredentials = 1,
    kNoSuchTarget = 2,
    kWrongState = 3,
    kRoomFull = 4,
};

std::int64_t now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

void send_frame(session::Session& session, const Bytes& frame) {
    session.send(frame.data(), frame.size());
}

void send_login_result(session::Session& session, std::uint8_t result,
                       std::uint64_t user_id) {
    FrameBuilder builder(MessageType::kLoginRes);
    builder.write_u8(result);
    builder.write_u64(user_id);
    send_frame(session, builder.build());
}

void send_join_result(session::Session& session, std::uint8_t result,
                      std::uint32_t room_id, std::uint16_t member_count) {
    FrameBuilder builder(MessageType::kJoinRoomRes);
    builder.write_u8(result);
    builder.write_u32(room_id);
    builder.write_u16(member_count);
    send_frame(session, builder.build());
}

void send_leave_result(session::Session& session, std::uint8_t result) {
    FrameBuilder builder(MessageType::kLeaveRoomRes);
    builder.write_u8(result);
    send_frame(session, builder.build());
}

// Says why before hanging up on a descriptor no session was made for. Spending
// a session on the apology would mean allocating the thing the server has just
// run out of room for; the frame is a few dozen bytes on a socket nothing has
// written to yet, so this does not block, and MSG_NOSIGNAL keeps a peer that
// left in the meantime from raising SIGPIPE.
void refuse_connection(int fd, std::string_view reason) {
    FrameBuilder builder(MessageType::kErrorNtf);
    builder.write_u16(static_cast<std::uint16_t>(ErrorCode::kConnectionRefused));
    builder.write_string(reason);

    const Bytes frame = builder.build();
    const ssize_t written = ::send(fd, frame.data(), frame.size(), MSG_NOSIGNAL);
    static_cast<void>(written);  // it is an explanation, not a guarantee
    ::close(fd);
}

}  // namespace

Server::Server(net::EventLoop& loop, const Config& config)
    : loop_(loop),
      config_(config),
      database_(loop, config.database_url, config.database_workers) {}

Server::~Server() {
    // The deadline timers capture this and an id to look up in a map that is
    // about to go away, and the loop outlives the server it belongs to.
    for (auto& entry : clients_) {
        cancel_login_deadline(entry.second);
    }
}

std::uint16_t Server::port() const {
    return acceptor_ == nullptr ? 0 : acceptor_->port();
}

void Server::start() {
    database_.load_rooms([this](bool ok, std::vector<db::Database::RoomRecord> rooms) {
        if (!ok) {
            std::fprintf(stderr, "server: could not load the room list\n");
            startup_failed_ = true;
            loop_.stop();
            return;
        }

        for (const db::Database::RoomRecord& record : rooms) {
            rooms_.create(record.id, record.capacity);
        }

        acceptor_ = std::make_unique<net::Acceptor>(
            loop_, config_.listen_host, config_.listen_port,
            [this](int fd) { accept_connection(fd); });

        std::fprintf(stderr, "server: listening on %s:%u with %zu rooms\n",
                     config_.listen_host.c_str(), acceptor_->port(), rooms.size());
    });
}

void Server::begin_shutdown() {
    // The listening socket goes first, so nothing arrives that would then have
    // to be turned away half-way through.
    acceptor_.reset();

    // Reading stops rather than the connections closing. A client that is
    // still owed the tail of a broadcast gets it; a client with a request
    // half-written does not get to start something the server cannot finish.
    for (auto& entry : clients_) {
        entry.second.session->stop_reading();
    }
}

bool Server::has_pending_output() const {
    for (const auto& entry : clients_) {
        if (entry.second.session->has_pending_output()) {
            return true;
        }
    }
    return false;
}

void Server::accept_connection(int fd) {
    if (clients_.size() >= config_.max_connections) {
        ++refused_connections_;
        if (refused_connections_ == 1 || refused_connections_ % 1000 == 0) {
            std::fprintf(stderr,
                         "server: at %zu connections, turned one away (%zu so far)\n",
                         clients_.size(), refused_connections_);
        }
        refuse_connection(fd, "the server is full");
        return;
    }

    const ClientId id = next_client_id_++;

    Client client;
    // Built before the map entry exists, which is safe: nothing the session
    // does can reach a handler before the loop runs again. Wrapped because the
    // acceptor has already let go of this descriptor -- registering it is what
    // can fail here, and a connection the server cannot take should cost that
    // connection and not the process.
    try {
        client.session = std::make_unique<session::Session>(
            loop_, fd,
            [this, id](session::Session& client_session, std::uint16_t type,
                       const std::uint8_t* payload, std::uint16_t length) {
                handle_message(id, client_session, type, payload, length);
            },
            [this, id](session::Session&) { drop_client(id); });
        client.login_deadline =
            loop_.run_after(config_.login_deadline,
                            [this, id] { enforce_login_deadline(id); });
    } catch (const std::exception& error) {
        std::fprintf(stderr, "server: could not take a connection: %s\n",
                     error.what());
        if (client.session == nullptr) {
            ::close(fd);  // nothing owns it yet
        }
        return;  // otherwise the session it did get made owns and closes it
    }

    clients_.emplace(id, std::move(client));
}

Server::Client* Server::find_client(ClientId id) {
    const auto it = clients_.find(id);
    return it == clients_.end() ? nullptr : &it->second;
}

void Server::cancel_login_deadline(Client& client) {
    if (client.login_deadline != 0) {
        loop_.cancel_timer(client.login_deadline);
        client.login_deadline = 0;
    }
}

void Server::enforce_login_deadline(ClientId id) {
    Client* client = find_client(id);
    if (client == nullptr) {
        return;
    }
    client->login_deadline = 0;

    if (client->logged_in) {
        return;  // cancelled at the login; here only if the two raced
    }
    if (client->login_in_flight && !client->login_deadline_extended) {
        // The answer is on its way, and under a backlog it may be the server
        // this client is waiting on. Hanging up on it here would punish the
        // peer for the server's own queue, so it gets another window -- one,
        // because a login admitted by the database finishes inside one.
        client->login_deadline_extended = true;
        client->login_deadline =
            loop_.run_after(config_.login_deadline,
                            [this, id] { enforce_login_deadline(id); });
        return;
    }

    client->session->send_error(ErrorCode::kConnectionRefused,
                                "no login before the deadline");
    client->session->close();
}

void Server::drop_client(ClientId id) {
    Client* client = find_client(id);
    if (client == nullptr) {
        return;
    }

    cancel_login_deadline(*client);
    leave_current_room(*client);

    // Destroys the session from inside its own close callback, which is what
    // that callback is for: it was moved aside before being invoked.
    clients_.erase(id);
}

void Server::leave_current_room(Client& client) {
    if (!client.room_id.has_value()) {
        return;
    }

    room::Room* room = rooms_.find(*client.room_id);
    client.room_id.reset();
    if (room == nullptr) {
        return;
    }

    room->leave(*client.session);

    FrameBuilder builder(MessageType::kUserLeftNtf);
    builder.write_u64(client.user_id);
    const Bytes frame = builder.build();
    room->broadcast(frame.data(), frame.size());
}

void Server::handle_message(ClientId id, session::Session& session, std::uint16_t type,
                            const std::uint8_t* payload, std::uint16_t length) {
    protocol::ByteReader reader(payload, length);

    switch (static_cast<MessageType>(type)) {
        case MessageType::kLoginReq:
            handle_login(id, session, reader);
            return;
        case MessageType::kJoinRoomReq:
            handle_join(id, session, reader);
            return;
        case MessageType::kLeaveRoomReq:
            handle_leave(id, session, reader);
            return;
        case MessageType::kChatReq:
            handle_chat(id, session, reader);
            return;
        case MessageType::kPingReq:
            handle_ping(session, reader);
            return;
        default:
            break;
    }

    // In the client band but not a type this server knows. A peer that cannot
    // name its own messages is not one to keep talking to.
    session.send_error(ErrorCode::kUnknownMessageType, "unknown message type");
    session.close();
}

void Server::handle_login(ClientId id, session::Session& session,
                          protocol::ByteReader& reader) {
    const std::uint16_t version = reader.read_u16();
    const std::string username(reader.read_string());
    const std::string password(reader.read_string());

    if (!reader.exhausted()) {
        session.send_error(ErrorCode::kMalformedFrame, "login payload does not parse");
        session.close();
        return;
    }
    if (version != protocol::kProtocolVersion) {
        session.send_error(ErrorCode::kProtocolVersionMismatch, "protocol version mismatch");
        session.close();
        return;
    }
    if (username.size() > protocol::kMaxUsernameBytes ||
        password.size() > protocol::kMaxPasswordBytes) {
        // Bad input from a client that is otherwise following the rules, so
        // the connection survives it.
        session.send_error(ErrorCode::kFieldValidationFailed, "username or password too long");
        return;
    }

    Client* client = find_client(id);
    if (client == nullptr) {
        return;
    }
    if (client->logged_in || client->login_in_flight) {
        send_login_result(session, kWrongState, 0);
        return;
    }
    if (username.find('\0') != std::string::npos) {
        // No account has one, and the database would never see it: the query
        // takes the name as a C string, so "alice\0x" would log in as alice
        // and then be announced to the room under a name that is not the
        // account's.
        send_login_result(session, kBadCredentials, 0);
        return;
    }

    client->login_in_flight = true;
    database_.authenticate(
        username, password,
        [this, id, username](db::Database::AuthStatus status, std::uint64_t user_id) {
            // The connection may have gone while the database was thinking.
            Client* pending = find_client(id);
            if (pending == nullptr) {
                return;
            }
            pending->login_in_flight = false;

            if (status == db::Database::AuthStatus::kUnavailable) {
                // Not a rejection: telling the user their password is wrong
                // when the database is down would send them chasing the wrong
                // problem.
                pending->session->send_error(ErrorCode::kFieldValidationFailed,
                                             "the server cannot check logins right now");
                return;
            }
            if (status != db::Database::AuthStatus::kAccepted) {
                send_login_result(*pending->session, kBadCredentials, 0);
                return;
            }

            pending->logged_in = true;
            pending->user_id = user_id;
            pending->username = username;
            cancel_login_deadline(*pending);
            send_login_result(*pending->session, kOk, user_id);
        });
}

void Server::handle_join(ClientId id, session::Session& session,
                         protocol::ByteReader& reader) {
    const std::uint32_t room_id = reader.read_u32();
    if (!reader.exhausted()) {
        session.send_error(ErrorCode::kMalformedFrame, "join payload does not parse");
        session.close();
        return;
    }

    Client* client = find_client(id);
    if (client == nullptr) {
        return;
    }
    if (!client->logged_in || client->room_id.has_value()) {
        send_join_result(session, kWrongState, room_id, 0);
        return;
    }

    room::Room* room = rooms_.find(room_id);
    if (room == nullptr) {
        send_join_result(session, kNoSuchTarget, room_id, 0);
        return;
    }

    // Announced before the joiner is added, so the notification goes to the
    // people who were already there and not to the one arriving.
    FrameBuilder joined(MessageType::kUserJoinedNtf);
    joined.write_u64(client->user_id);
    joined.write_string(client->username);
    const Bytes announcement = joined.build();

    if (!room->join(session)) {
        send_join_result(session, kRoomFull, room_id, 0);
        return;
    }
    client->room_id = room_id;

    send_join_result(session, kOk, room_id,
                     static_cast<std::uint16_t>(room->member_count()));
    room->broadcast_except(session, announcement.data(), announcement.size());
}

void Server::handle_leave(ClientId id, session::Session& session,
                          protocol::ByteReader& reader) {
    if (!reader.exhausted()) {
        session.send_error(ErrorCode::kMalformedFrame, "leave carries no payload");
        session.close();
        return;
    }

    Client* client = find_client(id);
    if (client == nullptr) {
        return;
    }
    if (!client->room_id.has_value()) {
        send_leave_result(session, kWrongState);
        return;
    }

    leave_current_room(*client);
    send_leave_result(session, kOk);
}

void Server::handle_chat(ClientId id, session::Session& session,
                         protocol::ByteReader& reader) {
    const std::string text(reader.read_string());
    if (!reader.exhausted()) {
        session.send_error(ErrorCode::kMalformedFrame, "chat payload does not parse");
        session.close();
        return;
    }

    Client* client = find_client(id);
    if (client == nullptr) {
        return;
    }

    if (!client->logged_in || !client->room_id.has_value()) {
        // CHAT_REQ has no response message, so a state violation has nowhere
        // to go but ERROR_NTF, and the error table has no code for one. 1003
        // is the only entry that leaves the connection standing, which is the
        // property that matters here.
        session.send_error(ErrorCode::kFieldValidationFailed, "not in a room");
        return;
    }
    if (text.size() > protocol::kMaxChatTextBytes) {
        session.send_error(ErrorCode::kFieldValidationFailed, "message too long");
        return;
    }

    room::Room* room = rooms_.find(*client->room_id);
    if (room == nullptr) {
        return;
    }

    const std::int64_t sent_at = now_ms();

    FrameBuilder builder(MessageType::kChatNtf);
    builder.write_u64(client->user_id);
    builder.write_string(client->username);
    builder.write_string(text);
    builder.write_i64(sent_at);
    const Bytes frame = builder.build();

    // The sender is included, so what every client displays is the room in the
    // server's order rather than each one's guess at it.
    room->broadcast(frame.data(), frame.size());

    // Logged afterwards and without waiting: the message is already delivered,
    // and the sender should not be held up by a write to disk.
    database_.log_chat_message(*client->room_id, client->user_id, text, sent_at);
}

void Server::handle_ping(session::Session& session, protocol::ByteReader& reader) {
    if (!reader.exhausted()) {
        session.send_error(ErrorCode::kMalformedFrame, "ping carries no payload");
        session.close();
        return;
    }

    // The work of a ping is already done: the session stamped the connection as
    // active when it read the bytes. Answering exists so the client can tell a
    // live server from one that is merely holding the socket open.
    send_frame(session, FrameBuilder(MessageType::kPongNtf).build());
}

}  // namespace rrs
