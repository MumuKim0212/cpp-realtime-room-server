#include "db/connection.h"

#include <libpq-fe.h>

#include <cstddef>
#include <stdexcept>
#include <utility>

namespace rrs::db {

Result::Result(pg_result* result) : result_(result) {}

Result::~Result() {
    if (result_ != nullptr) {
        ::PQclear(result_);
    }
}

Result::Result(Result&& other) noexcept
    : result_(std::exchange(other.result_, nullptr)) {}

Result& Result::operator=(Result&& other) noexcept {
    if (this != &other) {
        if (result_ != nullptr) {
            ::PQclear(result_);
        }
        result_ = std::exchange(other.result_, nullptr);
    }
    return *this;
}

int Result::row_count() const {
    return result_ == nullptr ? 0 : ::PQntuples(result_);
}

std::string Result::value(int row, int column) const {
    if (result_ == nullptr || ::PQgetisnull(result_, row, column) == 1) {
        return {};
    }
    return std::string(::PQgetvalue(result_, row, column),
                       static_cast<std::size_t>(::PQgetlength(result_, row, column)));
}

Connection::Connection(const std::string& conninfo)
    : conninfo_(conninfo), connection_(::PQconnectdb(conninfo.c_str())) {
    if (connection_ == nullptr) {
        throw std::runtime_error("libpq could not allocate a connection");
    }
    if (::PQstatus(connection_) != CONNECTION_OK) {
        const std::string message = ::PQerrorMessage(connection_);
        ::PQfinish(connection_);
        throw std::runtime_error("could not connect to PostgreSQL: " + message);
    }
}

Connection::~Connection() {
    ::PQfinish(connection_);
}

void Connection::reconnect_if_broken() {
    if (::PQstatus(connection_) == CONNECTION_OK) {
        return;
    }
    // The database restarted or the link dropped. One attempt to bring it
    // back; if that fails, the statement below reports the failure and the
    // next call tries again.
    ::PQreset(connection_);
}

Result Connection::execute(const std::string& sql,
                           const std::vector<std::string>& params) {
    reconnect_if_broken();

    std::vector<const char*> values;
    values.reserve(params.size());
    for (const std::string& param : params) {
        values.push_back(param.c_str());
    }

    pg_result* raw = ::PQexecParams(connection_, sql.c_str(),
                                    static_cast<int>(params.size()), nullptr,
                                    values.data(), nullptr, nullptr, 0);

    // Owned from here on, so every path below can throw without leaking.
    Result result(raw);

    if (raw == nullptr) {
        throw std::runtime_error("query failed: libpq ran out of memory");
    }

    const ExecStatusType status = ::PQresultStatus(raw);
    if (status != PGRES_TUPLES_OK && status != PGRES_COMMAND_OK) {
        throw std::runtime_error("query failed: " +
                                 std::string(::PQerrorMessage(connection_)));
    }

    return result;
}

}  // namespace rrs::db
