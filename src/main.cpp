#include <signal.h>
#include <sys/epoll.h>
#include <sys/resource.h>
#include <sys/signalfd.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>
#include <system_error>
#include <vector>

#include "db/database.h"
#include "net/event_loop.h"
#include "protocol/message.h"
#include "server.h"

namespace {

// Configuration arrives through the environment because the server is meant to
// be run by docker-compose, where that is the ordinary way to hand a container
// its settings.
std::string env_or(const char* name, const std::string& fallback) {
    const char* value = std::getenv(name);
    return value == nullptr ? fallback : std::string(value);
}

rrs::Server::Config config_from_environment() {
    rrs::Server::Config config;
    config.listen_host = env_or("RRS_LISTEN_HOST", "0.0.0.0");
    config.listen_port = static_cast<std::uint16_t>(
        std::stoul(env_or("RRS_LISTEN_PORT", "9000")));
    config.database_url = env_or("RRS_DATABASE_URL", "");
    config.database_workers = static_cast<std::size_t>(
        std::stoul(env_or("RRS_DATABASE_WORKERS", "4")));
    config.max_connections = static_cast<std::size_t>(
        std::stoul(env_or("RRS_MAX_CONNECTIONS", "4096")));

    if (config.database_url.empty()) {
        throw std::runtime_error("RRS_DATABASE_URL must be set");
    }
    if (config.max_connections == 0) {
        throw std::runtime_error("RRS_MAX_CONNECTIONS must be at least 1");
    }
    return config;
}

void print_usage() {
    std::fprintf(stderr,
                 "usage: room_server                       run the server\n"
                 "       room_server --create-user NAME    create one account\n"
                 "\n"
                 "Configuration comes from the environment either way; see the\n"
                 "README. --create-user reads the password from stdin, which is\n"
                 "meant to be piped:\n"
                 "\n"
                 "  echo -n 'the password' | room_server --create-user alice\n");
}

// Creating an account, for a server that has no client which can ask for one.
// The protocol has no sign-up message on purpose -- who gets an account is the
// operator's business, not a client's -- which otherwise leaves a freshly
// deployed server holding a room list that nobody can log in to reach.
//
// The password comes from stdin rather than argv so that it stays out of the
// process list and the shell history. It is not hidden while being typed, which
// is why the usage above pipes it in.
int create_user(const rrs::Server::Config& config, const std::string& username) {
    // Checked against the same ceilings the protocol enforces, so this cannot
    // mint an account that LOGIN_REQ would then refuse to carry.
    if (username.empty() || username.size() > rrs::protocol::kMaxUsernameBytes) {
        std::fprintf(stderr, "fatal: a username is 1 to %zu bytes\n",
                     rrs::protocol::kMaxUsernameBytes);
        return 1;
    }

    std::string password;
    if (!std::getline(std::cin, password)) {
        std::fprintf(stderr, "fatal: no password on stdin\n");
        return 1;
    }
    if (!password.empty() && password.back() == '\r') {
        password.pop_back();  // written on a machine with the other line ending
    }
    if (password.empty() || password.size() > rrs::protocol::kMaxPasswordBytes) {
        std::fprintf(stderr, "fatal: a password is 1 to %zu bytes\n",
                     rrs::protocol::kMaxPasswordBytes);
        return 1;
    }

    // One worker: there is exactly one thing to do, and it is the hash that
    // takes the time rather than anything that would benefit from a second.
    rrs::net::EventLoop loop;
    rrs::db::Database database(loop, config.database_url, 1);

    bool created = false;
    std::uint64_t user_id = 0;
    database.create_user(username, password, [&](bool ok, std::uint64_t id) {
        created = ok;
        user_id = id;
        loop.stop();
    });
    loop.run_after(std::chrono::seconds(60), [&loop] { loop.stop(); });
    loop.run();

    if (!created) {
        std::fprintf(stderr,
                     "fatal: could not create %s; the name may already be taken\n",
                     username.c_str());
        return 1;
    }

    std::fprintf(stderr, "created %s with user id %llu\n", username.c_str(),
                 static_cast<unsigned long long>(user_id));
    return 0;
}

// Makes sure the process may hold as many descriptors as the configuration
// asks for. Below that, the connection ceiling is never the thing a connection
// meets: accept() runs into EMFILE first, and all the acceptor can do about
// that is pause and try again. The soft limit is often 1024, a quarter of the
// default ceiling, so it is raised as far as the hard limit allows -- and a
// hard limit too low for the configuration is a configuration error.
void ensure_descriptor_limit(const rrs::Server::Config& config) {
    // The listening socket, one connection per database worker, the signalfd
    // and the epoll descriptor itself, with room to spare for what libraries
    // open on their own.
    const rlim_t needed = config.max_connections + config.database_workers + 64;

    rlimit limit{};
    if (::getrlimit(RLIMIT_NOFILE, &limit) != 0) {
        throw std::system_error(errno, std::system_category(), "getrlimit");
    }
    if (limit.rlim_cur == RLIM_INFINITY || limit.rlim_cur >= needed) {
        return;
    }
    if (limit.rlim_max != RLIM_INFINITY && limit.rlim_max < needed) {
        throw std::runtime_error(
            "RRS_MAX_CONNECTIONS needs " + std::to_string(needed) +
            " descriptors, but the hard limit is " + std::to_string(limit.rlim_max));
    }
    limit.rlim_cur = needed;
    if (::setrlimit(RLIMIT_NOFILE, &limit) != 0) {
        throw std::system_error(errno, std::system_category(), "setrlimit");
    }
}

// Turns the shutdown signals into a readable descriptor. Blocking them first
// takes away the default action, which is to kill the process where it stands,
// and delivering them through a descriptor means the request arrives as an
// ordinary loop event rather than in a signal handler, where next to nothing is
// safe to call.
//
// Must run before the first thread exists, so the database workers inherit the
// blocked mask and the signal cannot be delivered to one of them instead.
int install_shutdown_signals() {
    sigset_t shutdown_signals;
    ::sigemptyset(&shutdown_signals);
    ::sigaddset(&shutdown_signals, SIGINT);
    ::sigaddset(&shutdown_signals, SIGTERM);

    const int masked = ::pthread_sigmask(SIG_BLOCK, &shutdown_signals, nullptr);
    if (masked != 0) {
        throw std::system_error(masked, std::system_category(), "pthread_sigmask");
    }

    const int fd = ::signalfd(-1, &shutdown_signals, SFD_NONBLOCK | SFD_CLOEXEC);
    if (fd < 0) {
        throw std::system_error(errno, std::system_category(), "signalfd");
    }
    return fd;
}

// How long the server keeps running after being asked to stop, so that what is
// already queued for a client has a chance to reach it. Bounded, because a peer
// that has stopped reading must not be able to hold the shutdown open.
constexpr auto kDrainBudget = std::chrono::seconds(2);

// How often the drain asks whether it is finished. Polling rather than a
// notification from every session: this runs once in the life of the process,
// and the alternative is a callback threaded through code that has no other
// use for one.
constexpr auto kDrainPollInterval = std::chrono::milliseconds(20);

// Gives what is already queued for the clients a bounded chance to leave before
// their sockets close. Returns once nothing is left to send, the budget runs
// out, or a second signal arrives -- which is what the handler consuming the
// signalfd buys: the descriptor goes quiet, so this loop gets to sleep, and the
// next signal wakes it to stop early.
void drain_pending_output(rrs::net::EventLoop& loop, rrs::Server& server) {
    server.begin_shutdown();
    if (!server.has_pending_output()) {
        return;
    }

    const rrs::net::TimerId budget =
        loop.run_after(kDrainBudget, [&loop] { loop.stop(); });
    const rrs::net::TimerId poll =
        loop.run_every(kDrainPollInterval, [&loop, &server] {
            if (!server.has_pending_output()) {
                loop.stop();
            }
        });

    loop.run();

    // Cancelled rather than left to fire: the loop outlives this call, and a
    // timer holding a reference to a stack frame that has returned would not.
    loop.cancel_timer(budget);
    loop.cancel_timer(poll);
}

}  // namespace

int main(int argc, char** argv) {
    std::fprintf(stderr, "realtime-room-server %s\n", RRS_VERSION);

    try {
        const std::vector<std::string> arguments(argv + 1, argv + argc);
        if (!arguments.empty()) {
            if (arguments.size() == 2 && arguments[0] == "--create-user") {
                return create_user(config_from_environment(), arguments[1]);
            }
            print_usage();
            return 1;
        }

        const int signal_fd = install_shutdown_signals();
        const rrs::Server::Config config = config_from_environment();
        ensure_descriptor_limit(config);

        rrs::net::EventLoop loop;

        // The contents of the signal are of no interest: there is one thing to
        // do about either of them, and the descriptor being readable already
        // says to do it. It is read all the same, because a signalfd left
        // readable would make every later epoll_wait return at once, and the
        // drain that follows needs the loop to be able to sleep.
        loop.add_fd(signal_fd, EPOLLIN, [&loop, signal_fd](std::uint32_t) {
            signalfd_siginfo received{};
            while (::read(signal_fd, &received, sizeof(received)) > 0) {
            }
            loop.stop();
        });

        rrs::Server server(loop, config);
        server.start();
        loop.run();

        if (server.startup_failed()) {
            ::close(signal_fd);
            return 1;  // never listened, so there is nothing queued to drain
        }

        std::fprintf(stderr, "server: shutting down\n");
        drain_pending_output(loop, server);
        ::close(signal_fd);

        // Leaving this scope destroys the server, which joins the database
        // workers, which runs out the writes already queued behind them. With
        // the drain above, that is the whole of the graceful part: a client
        // whose message was broadcast gets it recorded, and a client the
        // broadcast had not finished reaching gets the rest of it.
        //
        // Nothing runs the loop after this point, and nothing may. Joining the
        // workers can post one last answer, and an answer reaches back into the
        // server and the database that are going away underneath it. The loop
        // outlives both, so those callbacks are destroyed unrun rather than
        // called -- which is the only reason it is safe to post them at all.
    } catch (const std::exception& error) {
        std::fprintf(stderr, "fatal: %s\n", error.what());
        return 1;
    }

    return 0;
}
