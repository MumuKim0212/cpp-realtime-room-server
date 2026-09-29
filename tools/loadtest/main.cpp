#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <functional>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

#include "client.h"
#include "db/connection.h"
#include "db/database.h"
#include "net/event_loop.h"
#include "protocol/message.h"
#include "stats.h"

namespace {

using rrs::loadtest::Client;
using rrs::loadtest::Counters;
using rrs::loadtest::Latencies;

// The accounts this tool logs in as. One password for all of them: the run
// measures broadcast, and a per-account secret would only make the seeding
// step harder to repeat.
constexpr const char* kPassword = "loadtest";

// Rooms of its own, above anything the schema seeds, so that a run sized for
// five hundred connections does not have to raise the capacity of the lobby
// people actually use. The seeding step creates them at the size the run
// needs; the server reads the room list once at startup, so seeding has to
// happen before the server does.
constexpr std::uint32_t kRoomBase = 9001;

// How long the population gets to connect, log in and join before the
// measurement starts without the stragglers. Generous because every login is
// an Argon2id verify, which is slow on purpose.
constexpr auto kRampBudget = std::chrono::seconds(120);

struct Options {
    std::string host = "127.0.0.1";
    std::uint16_t port = 9000;
    std::size_t clients = 100;
    std::uint32_t rooms = 3;
    double rate = 1.0;
    int duration_seconds = 30;
    std::size_t message_bytes = 64;
    std::string database_url;
    bool seed = false;
};

void print_usage() {
    std::fprintf(stderr,
                 "usage: room_loadtest [options]\n"
                 "\n"
                 "  --seed              create the accounts and rooms a run needs, then exit\n"
                 "  --database-url=S    libpq connection string, for --seed only\n"
                 "  --host=S            server address        (default 127.0.0.1)\n"
                 "  --port=N            server port           (default 9000)\n"
                 "  --clients=N         connections to open   (default 100)\n"
                 "  --rooms=N           rooms to spread over  (default 3)\n"
                 "  --rate=F            messages per second, per client (default 1)\n"
                 "  --duration=N        seconds to measure    (default 30)\n"
                 "  --size=N            chat text bytes       (default 64)\n"
                 "\n"
                 "Seed first, restart the server so it picks the rooms up, then run:\n"
                 "  room_loadtest --seed --clients=500 --rooms=5 \\\n"
                 "      --database-url=\"host=127.0.0.1 user=rrs password=rrs dbname=rrs\"\n"
                 "  room_loadtest --clients=500 --rooms=5 --rate=2 --duration=60\n");
}

// Splits --key=value. Returns false for anything that is not a known option,
// so a typo stops the run rather than silently measuring the default.
bool parse_args(int argc, char** argv, Options& options) {
    for (int i = 1; i < argc; ++i) {
        const std::string argument = argv[i];
        if (argument == "--help" || argument == "-h") {
            return false;
        }
        if (argument == "--seed") {
            options.seed = true;
            continue;
        }

        const std::size_t split = argument.find('=');
        if (argument.rfind("--", 0) != 0 || split == std::string::npos) {
            std::fprintf(stderr, "loadtest: unrecognised argument: %s\n",
                         argument.c_str());
            return false;
        }

        const std::string key = argument.substr(0, split);
        const std::string value = argument.substr(split + 1);

        if (key == "--host") {
            options.host = value;
        } else if (key == "--port") {
            options.port = static_cast<std::uint16_t>(std::stoul(value));
        } else if (key == "--clients") {
            options.clients = static_cast<std::size_t>(std::stoul(value));
        } else if (key == "--rooms") {
            options.rooms = static_cast<std::uint32_t>(std::stoul(value));
        } else if (key == "--rate") {
            options.rate = std::stod(value);
        } else if (key == "--duration") {
            options.duration_seconds = std::stoi(value);
        } else if (key == "--size") {
            options.message_bytes = static_cast<std::size_t>(std::stoul(value));
        } else if (key == "--database-url") {
            options.database_url = value;
        } else {
            std::fprintf(stderr, "loadtest: unrecognised option: %s\n", key.c_str());
            return false;
        }
    }

    if (options.clients == 0 || options.rooms == 0 || options.rate <= 0.0) {
        std::fprintf(stderr, "loadtest: clients, rooms and rate must be positive\n");
        return false;
    }
    if (options.message_bytes > rrs::protocol::kMaxChatTextBytes) {
        // The server would turn every message down, and the run would time
        // nothing while looking as though it had measured something.
        std::fprintf(stderr, "loadtest: --size is at most %zu bytes\n",
                     rrs::protocol::kMaxChatTextBytes);
        return false;
    }
    return true;
}

std::string account_name(std::size_t index) {
    char name[32];
    std::snprintf(name, sizeof(name), "load%06zu", index);
    return name;
}

double to_ms(std::int64_t nanoseconds) {
    return static_cast<double>(nanoseconds) / 1e6;
}

int connect_socket(const std::string& host, std::uint16_t port) {
    const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        throw std::system_error(errno, std::system_category(), "socket");
    }

    // Latency is the measurement, so the kernel must not hold a small write
    // back waiting for company.
    const int nodelay = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = ::htons(port);
    if (::inet_pton(AF_INET, host.c_str(), &address.sin_addr) != 1) {
        ::close(fd);
        throw std::invalid_argument("--host must be an IPv4 address: " + host);
    }

    if (::connect(fd, reinterpret_cast<const sockaddr*>(&address),
                  sizeof(address)) < 0 &&
        errno != EINPROGRESS) {
        const int saved = errno;
        ::close(fd);
        throw std::system_error(saved, std::system_category(), "connect");
    }
    return fd;
}

// Rooms first, then the accounts that are missing. Both are idempotent: a
// second run with a larger population adds to what the first one left.
int run_seed(const Options& options) {
    if (options.database_url.empty()) {
        std::fprintf(stderr, "loadtest: --seed needs --database-url\n");
        return 1;
    }

    const std::size_t per_room =
        (options.clients + options.rooms - 1) / options.rooms;

    std::set<std::string> existing;
    {
        rrs::db::Connection connection(options.database_url);

        for (std::uint32_t i = 0; i < options.rooms; ++i) {
            connection.execute(
                "INSERT INTO rooms (id, name, capacity) VALUES ($1::bigint, $2, $3::int) "
                "ON CONFLICT (id) DO UPDATE SET capacity = EXCLUDED.capacity",
                {std::to_string(kRoomBase + i), "loadtest" + std::to_string(i),
                 std::to_string(per_room)});
        }
        std::fprintf(stderr, "loadtest: rooms %u..%u ready, capacity %zu each\n",
                     kRoomBase, kRoomBase + options.rooms - 1, per_room);

        // Hashing a password is deliberately slow, so an account that is
        // already there is worth a query to find out about.
        const rrs::db::Result rows = connection.execute(
            "SELECT username FROM users WHERE username LIKE $1", {"load%"});
        for (int row = 0; row < rows.row_count(); ++row) {
            existing.insert(rows.value(row, 0));
        }
    }

    rrs::net::EventLoop loop;
    rrs::db::Database database(loop, options.database_url, 4);

    std::size_t pending = 0;
    std::size_t created = 0;
    std::size_t failed = 0;

    // Every create is submitted before the loop runs, so `pending` is complete
    // by the time the first answer can come back.
    for (std::size_t i = 0; i < options.clients; ++i) {
        const std::string username = account_name(i);
        if (existing.count(username) != 0) {
            continue;
        }
        ++pending;
        database.create_user(username, kPassword,
                             [&](bool ok, std::uint64_t) {
                                 if (ok) {
                                     ++created;
                                 } else {
                                     ++failed;
                                 }
                                 if (--pending == 0) {
                                     loop.stop();
                                 }
                             });
    }

    if (pending != 0) {
        std::fprintf(stderr, "loadtest: creating %zu accounts (Argon2id, this takes a moment)\n",
                     pending);
        loop.run();
    }

    std::fprintf(stderr,
                 "loadtest: %zu accounts created, %zu already there, %zu failed\n",
                 created, existing.size(), failed);
    std::fprintf(stderr,
                 "loadtest: restart the server so it loads the new rooms\n");
    return failed == 0 ? 0 : 1;
}

void report(const Options& options, Counters& counters, Latencies& latencies,
            std::chrono::nanoseconds measured_for) {
    const Latencies::Summary summary = latencies.summarise();
    const double seconds =
        std::chrono::duration<double>(measured_for).count();
    const double per_message =
        counters.messages_sent == 0
            ? 0.0
            : static_cast<double>(counters.deliveries) /
                  static_cast<double>(counters.messages_sent);

    std::printf("\nconditions\n");
    std::printf("  server        %s:%u\n", options.host.c_str(), options.port);
    std::printf("  clients       %zu joined of %zu, over %u rooms (%zu per room)\n",
                counters.joined, options.clients, options.rooms,
                (options.clients + options.rooms - 1) / options.rooms);
    std::printf("  offered load  %.2f msg/s per client, %.0f msg/s in total\n",
                options.rate, options.rate * static_cast<double>(counters.joined));
    std::printf("  message       %zu bytes of chat text\n", options.message_bytes);
    std::printf("  measured for  %.1f s\n", seconds);
    std::printf("  generator     %u hardware threads, one event loop\n",
                std::thread::hardware_concurrency());

    std::printf("\nthroughput\n");
    std::printf("  sent          %zu messages (%.0f/s)\n", counters.messages_sent,
                static_cast<double>(counters.messages_sent) / seconds);
    std::printf("  delivered     %zu (%.1f per message, %.0f/s)\n",
                counters.deliveries, per_message,
                static_cast<double>(counters.deliveries) / seconds);

    std::printf("\nbroadcast latency, sender write to receiver read\n");
    if (summary.max == 0) {
        std::printf("  no deliveries were timed\n");
    } else {
        std::printf("  p50           %8.3f ms\n", to_ms(summary.p50));
        std::printf("  p90           %8.3f ms\n", to_ms(summary.p90));
        std::printf("  p99           %8.3f ms\n", to_ms(summary.p99));
        std::printf("  max           %8.3f ms\n", to_ms(summary.max));
        std::printf("  mean          %8.3f ms\n", to_ms(static_cast<std::int64_t>(summary.mean)));
    }

    if (counters.lost != 0 || counters.protocol_errors != 0 ||
        counters.login_failed != 0 || counters.join_failed != 0) {
        std::printf("\nfailures\n");
        std::printf("  lost          %zu connections ended early\n", counters.lost);
        std::printf("  login         %zu refused\n", counters.login_failed);
        std::printf("  join          %zu refused\n", counters.join_failed);
        std::printf("  protocol      %zu ERROR_NTF\n", counters.protocol_errors);
        if (counters.join_failed != 0) {
            std::printf("  (a refused join usually means --seed has not run, or the\n"
                        "   server has not been restarted since it did)\n");
        }
    }
}

int run_load(const Options& options) {
    rrs::net::EventLoop loop;
    Latencies latencies;
    Counters counters;

    // Room for everything the run is expected to time, taken up front. The
    // ceiling is there so that a wildly oversized run asks the allocator for
    // something reasonable and grows from there instead.
    constexpr std::size_t kMaxReservedSamples = 8u * 1000u * 1000u;
    const std::size_t per_room =
        (options.clients + options.rooms - 1) / options.rooms;
    latencies.reserve(std::min(
        kMaxReservedSamples,
        static_cast<std::size_t>(options.rate *
                                 static_cast<double>(options.duration_seconds) *
                                 static_cast<double>(options.clients) *
                                 static_cast<double>(per_room))));

    const auto interval = std::chrono::nanoseconds(
        static_cast<std::int64_t>(1e9 / options.rate));

    std::vector<std::unique_ptr<Client>> clients;
    clients.reserve(options.clients);

    std::size_t joined = 0;
    bool measuring = false;
    rrs::net::TimePoint measurement_began;

    // Assigned before the loop runs, and only called from inside it.
    std::function<void()> begin_measurement;

    for (std::size_t i = 0; i < options.clients; ++i) {
        Client::Config config;
        config.username = account_name(i);
        config.password = kPassword;
        config.room_id = kRoomBase + static_cast<std::uint32_t>(i % options.rooms);
        config.send_interval = interval;

        // Spread across the interval. Starting every connection at the same
        // instant would turn the load into a series of spikes and measure the
        // queue behind each one rather than the steady state.
        config.start_offset = interval * static_cast<std::int64_t>(i) /
                              static_cast<std::int64_t>(options.clients);
        config.message_bytes = options.message_bytes;

        clients.push_back(std::make_unique<Client>(
            loop, connect_socket(options.host, options.port), std::move(config),
            latencies, counters, [&] {
                if (++joined == options.clients) {
                    begin_measurement();
                }
            }));
    }

    begin_measurement = [&] {
        if (measuring) {
            return;
        }
        measuring = true;
        measurement_began = rrs::net::Clock::now();

        std::fprintf(stderr, "loadtest: %zu clients in, measuring for %d s\n",
                     joined, options.duration_seconds);
        for (auto& client : clients) {
            client->start_sending();
        }
        loop.run_after(std::chrono::seconds(options.duration_seconds),
                       [&loop] { loop.stop(); });
    };

    // Nothing is measured through the ramp, so a population that never fully
    // arrives is reported on rather than waited for.
    loop.run_after(kRampBudget, [&] {
        if (!measuring) {
            std::fprintf(stderr, "loadtest: only %zu of %zu clients joined in time\n",
                         joined, options.clients);
            begin_measurement();
        }
    });

    std::fprintf(stderr, "loadtest: connecting %zu clients\n", options.clients);
    loop.run();

    const auto measured_for =
        measuring ? rrs::net::Clock::now() - measurement_began
                  : rrs::net::Duration::zero();
    report(options, counters, latencies, measured_for);

    // The clients have to go before the loop they registered with does.
    clients.clear();
    return counters.joined == options.clients && counters.lost == 0 ? 0 : 1;
}

}  // namespace

int main(int argc, char** argv) {
    Options options;
    if (!parse_args(argc, argv, options)) {
        print_usage();
        return 2;
    }

    try {
        return options.seed ? run_seed(options) : run_load(options);
    } catch (const std::exception& error) {
        std::fprintf(stderr, "loadtest: fatal: %s\n", error.what());
        return 1;
    }
}
