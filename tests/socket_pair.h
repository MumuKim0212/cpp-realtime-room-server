#pragma once

#include <sys/socket.h>
#include <unistd.h>

#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

namespace rrs::test {

// Two connected non-blocking sockets. One end is handed to a session, the
// other stays with the test to play the client.
class SocketPair {
public:
    SocketPair() {
        EXPECT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
                               0, fds_),
                  0);
    }

    ~SocketPair() {
        for (const int fd : fds_) {
            if (fd >= 0) {
                ::close(fd);
            }
        }
    }

    SocketPair(const SocketPair&) = delete;
    SocketPair& operator=(const SocketPair&) = delete;

    // The session owns the descriptor from here on and closes it itself.
    int release_server_fd() {
        const int fd = fds_[0];
        fds_[0] = -1;
        return fd;
    }

    void close_client() {
        ::close(fds_[1]);
        fds_[1] = -1;
    }

    void write_to_server(const std::vector<std::uint8_t>& bytes) {
        EXPECT_EQ(::send(fds_[1], bytes.data(), bytes.size(), 0),
                  static_cast<ssize_t>(bytes.size()));
    }

    // Everything the server has sent so far, without blocking.
    std::vector<std::uint8_t> read_from_server() {
        std::vector<std::uint8_t> out;
        std::uint8_t chunk[4096];
        while (true) {
            const ssize_t received = ::recv(fds_[1], chunk, sizeof(chunk), 0);
            if (received <= 0) {
                break;
            }
            out.insert(out.end(), chunk, chunk + received);
        }
        return out;
    }

private:
    int fds_[2] = {-1, -1};
};

}  // namespace rrs::test
