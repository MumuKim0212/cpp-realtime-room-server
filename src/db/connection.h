#pragma once

#include <string>
#include <vector>

// libpq's own types, forward declared so this header does not drag libpq-fe.h
// into everything that touches the database layer.
struct pg_conn;
struct pg_result;

namespace rrs::db {

// The rows a statement came back with.
class Result {
public:
    // Takes ownership, including of a null pointer, so a failed call can still
    // be handed over without leaking.
    explicit Result(pg_result* result);
    ~Result();

    Result(Result&& other) noexcept;
    Result& operator=(Result&& other) noexcept;
    Result(const Result&) = delete;
    Result& operator=(const Result&) = delete;

    int row_count() const;
    bool empty() const { return row_count() == 0; }

    // Text form of one field, empty for a NULL.
    std::string value(int row, int column) const;

private:
    pg_result* result_;
};

// One PostgreSQL connection, owned by one worker and never shared. Sharing
// would need a lock, and a lock in front of a blocking call gives back exactly
// what having several workers was for.
class Connection {
public:
    // Throws std::runtime_error when the connection cannot be established.
    explicit Connection(const std::string& conninfo);
    ~Connection();

    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

    // Runs a statement with its parameters sent apart from the SQL, which is
    // what keeps a chat message from ever being read as one. Reconnects first
    // if the link has gone down. Throws std::runtime_error on failure.
    Result execute(const std::string& sql, const std::vector<std::string>& params);

private:
    void reconnect_if_broken();

    std::string conninfo_;
    pg_conn* connection_;
};

}  // namespace rrs::db
