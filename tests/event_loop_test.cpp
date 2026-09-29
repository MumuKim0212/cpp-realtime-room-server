#include "net/event_loop.h"

#include <fcntl.h>
#include <sys/epoll.h>
#include <unistd.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <thread>
#include <vector>

namespace {

using namespace std::chrono_literals;

using rrs::net::Clock;
using rrs::net::EventLoop;
using rrs::net::TimerId;

// A non-blocking pipe standing in for a socket: a readiness source the test
// drives by hand.
class Pipe {
public:
    Pipe() { EXPECT_EQ(::pipe2(fds_, O_NONBLOCK | O_CLOEXEC), 0); }

    ~Pipe() {
        for (const int fd : fds_) {
            if (fd >= 0) {
                ::close(fd);
            }
        }
    }

    Pipe(const Pipe&) = delete;
    Pipe& operator=(const Pipe&) = delete;

    int read_fd() const { return fds_[0]; }
    int write_fd() const { return fds_[1]; }

    void write_byte() {
        const char byte = 'x';
        EXPECT_EQ(::write(fds_[1], &byte, 1), 1);
    }

    void drain() {
        char buffer[64];
        while (::read(fds_[0], buffer, sizeof(buffer)) > 0) {
        }
    }

private:
    int fds_[2] = {-1, -1};
};

// Guards tests that would otherwise hang rather than fail if a wakeup is lost.
constexpr auto kTestTimeout = 2s;

TEST(EventLoop, TimerFiresWithNothingToReadOrWrite) {
    // The loop's own wakeup descriptor is the only thing registered and nobody
    // is going to signal it, so this only terminates if the loop derives its
    // epoll_wait timeout from the timer queue instead of blocking on -1.
    EventLoop loop;
    bool fired = false;

    loop.run_after(10ms, [&] {
        fired = true;
        loop.stop();
    });

    const auto start = Clock::now();
    loop.run();

    EXPECT_TRUE(fired);
    EXPECT_GE(Clock::now() - start, 10ms);
}

TEST(EventLoop, DeliversReadReadiness) {
    EventLoop loop;
    Pipe pipe;
    int reads = 0;

    loop.add_fd(pipe.read_fd(), EPOLLIN, [&](std::uint32_t events) {
        EXPECT_TRUE((events & EPOLLIN) != 0);
        pipe.drain();
        ++reads;
        loop.stop();
    });
    pipe.write_byte();

    loop.run_after(kTestTimeout, [&] { loop.stop(); });
    loop.run();

    EXPECT_EQ(reads, 1);
}

TEST(EventLoop, DeliversWriteReadiness) {
    EventLoop loop;
    Pipe pipe;
    bool writable = false;

    loop.add_fd(pipe.write_fd(), EPOLLOUT, [&](std::uint32_t events) {
        writable = (events & EPOLLOUT) != 0;
        loop.stop();
    });

    loop.run_after(kTestTimeout, [&] { loop.stop(); });
    loop.run();

    EXPECT_TRUE(writable);
}

TEST(EventLoop, RemovedDescriptorStopsBeingDelivered) {
    EventLoop loop;
    Pipe pipe;
    int calls = 0;

    loop.add_fd(pipe.read_fd(), EPOLLIN, [&](std::uint32_t) { ++calls; });
    loop.remove_fd(pipe.read_fd());
    pipe.write_byte();

    loop.run_after(20ms, [&] { loop.stop(); });
    loop.run();

    EXPECT_EQ(calls, 0);
}

TEST(EventLoop, CallbackMayRemoveItsOwnDescriptor) {
    // Closing a connection from inside its own read handler is the ordinary
    // case, and it destroys the loop's copy of the callback that is running.
    EventLoop loop;
    Pipe pipe;
    int calls = 0;

    loop.add_fd(pipe.read_fd(), EPOLLIN, [&](std::uint32_t) {
        ++calls;
        loop.remove_fd(pipe.read_fd());
        loop.stop();
    });
    pipe.write_byte();

    loop.run_after(kTestTimeout, [&] { loop.stop(); });
    loop.run();

    EXPECT_EQ(calls, 1);
}

TEST(EventLoop, PeriodicTimerKeepsFiring) {
    EventLoop loop;
    int ticks = 0;

    const auto start = Clock::now();
    loop.run_every(5ms, [&] {
        if (++ticks == 3) {
            loop.stop();
        }
    });
    loop.run();

    EXPECT_EQ(ticks, 3);
    EXPECT_GE(Clock::now() - start, 15ms);
}

TEST(EventLoop, CancelledTimerNeverFires) {
    EventLoop loop;
    bool fired = false;

    const TimerId id = loop.run_after(5ms, [&] { fired = true; });
    loop.cancel_timer(id);
    loop.run_after(20ms, [&] { loop.stop(); });

    loop.run();

    EXPECT_FALSE(fired);
}

TEST(EventLoop, PostedCallbacksRunInOrder) {
    EventLoop loop;
    std::vector<int> order;

    for (int i = 0; i < 5; ++i) {
        loop.post([&order, i] { order.push_back(i); });
    }
    loop.post([&] { loop.stop(); });

    loop.run();

    EXPECT_EQ(order, (std::vector<int>{0, 1, 2, 3, 4}));
}

TEST(EventLoop, PostedCallbacksRunOnTheLoopThread) {
    EventLoop loop;
    std::thread::id ran_on;

    std::thread poster([&] {
        loop.post([&] {
            ran_on = std::this_thread::get_id();
            loop.stop();
        });
    });
    poster.join();

    loop.run();

    EXPECT_EQ(ran_on, std::this_thread::get_id());
}

TEST(EventLoop, PostWakesALoopThatIsAlreadyAsleep) {
    // No timers and no traffic, so the loop is parked in epoll_wait with
    // nothing scheduled to release it. Only the post can.
    EventLoop loop;
    std::atomic<bool> woke{false};

    std::thread poster([&] {
        std::this_thread::sleep_for(20ms);
        loop.post([&] {
            woke = true;
            loop.stop();
        });
    });

    loop.run();
    poster.join();

    EXPECT_TRUE(woke);
}

TEST(EventLoop, ACallbackMayPostMoreWork) {
    EventLoop loop;
    int depth = 0;

    loop.post([&] {
        ++depth;
        loop.post([&] {
            ++depth;
            loop.stop();
        });
    });

    loop.run();

    EXPECT_EQ(depth, 2);
}

TEST(EventLoop, TimersRunWhileDescriptorsSitIdle) {
    // The arrangement a room tick depends on: a registered but quiet connection
    // must not keep the tick from running on schedule.
    EventLoop loop;
    Pipe pipe;
    int io_calls = 0;
    int ticks = 0;

    loop.add_fd(pipe.read_fd(), EPOLLIN, [&](std::uint32_t) { ++io_calls; });
    loop.run_every(5ms, [&] {
        if (++ticks == 3) {
            loop.stop();
        }
    });

    loop.run();

    EXPECT_EQ(ticks, 3);
    EXPECT_EQ(io_calls, 0);
}

}  // namespace
