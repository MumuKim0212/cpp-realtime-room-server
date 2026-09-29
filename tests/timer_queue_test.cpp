#include "net/timer_queue.h"

#include <gtest/gtest.h>

#include <stdexcept>
#include <vector>

namespace {

using namespace std::chrono_literals;

using rrs::net::Clock;
using rrs::net::TimerId;
using rrs::net::TimerQueue;

// The queue takes absolute deadlines, so these tests drive time by hand and
// never sleep.

TEST(TimerQueue, EmptyQueueHasNothingPending) {
    TimerQueue timers;

    EXPECT_FALSE(timers.next_expiry().has_value());
}

TEST(TimerQueue, ReportsTheEarliestDeadline) {
    TimerQueue timers;
    const auto start = Clock::now();

    timers.run_at(start + 50ms, [] {});
    timers.run_at(start + 10ms, [] {});
    timers.run_at(start + 30ms, [] {});

    const auto next = timers.next_expiry();
    ASSERT_TRUE(next.has_value());
    EXPECT_EQ(*next, start + 10ms);
}

TEST(TimerQueue, FiresInDeadlineOrder) {
    TimerQueue timers;
    const auto start = Clock::now();
    std::vector<int> fired;

    timers.run_at(start + 30ms, [&] { fired.push_back(3); });
    timers.run_at(start + 10ms, [&] { fired.push_back(1); });
    timers.run_at(start + 20ms, [&] { fired.push_back(2); });

    timers.expire(start + 100ms);

    EXPECT_EQ(fired, (std::vector<int>{1, 2, 3}));
}

TEST(TimerQueue, EqualDeadlinesFireInRegistrationOrder) {
    TimerQueue timers;
    const auto start = Clock::now();
    std::vector<int> fired;

    for (int i = 0; i < 5; ++i) {
        timers.run_at(start + 10ms, [&fired, i] { fired.push_back(i); });
    }

    timers.expire(start + 10ms);

    EXPECT_EQ(fired, (std::vector<int>{0, 1, 2, 3, 4}));
}

TEST(TimerQueue, LeavesTimersThatAreNotDueYet) {
    TimerQueue timers;
    const auto start = Clock::now();
    int fired = 0;

    timers.run_at(start + 10ms, [&] { ++fired; });
    timers.run_at(start + 50ms, [&] { ++fired; });

    timers.expire(start + 20ms);

    EXPECT_EQ(fired, 1);
    const auto next = timers.next_expiry();
    ASSERT_TRUE(next.has_value());
    EXPECT_EQ(*next, start + 50ms);
}

TEST(TimerQueue, OneShotDoesNotRepeat) {
    TimerQueue timers;
    const auto start = Clock::now();
    int fired = 0;

    timers.run_at(start + 10ms, [&] { ++fired; });

    timers.expire(start + 10ms);
    timers.expire(start + 100ms);

    EXPECT_EQ(fired, 1);
    EXPECT_FALSE(timers.next_expiry().has_value());
}

TEST(TimerQueue, PeriodicTimerRearms) {
    TimerQueue timers;
    const auto start = Clock::now();
    int fired = 0;

    timers.run_every(start + 10ms, 10ms, [&] { ++fired; });

    timers.expire(start + 10ms);
    EXPECT_EQ(fired, 1);

    timers.expire(start + 15ms);
    EXPECT_EQ(fired, 1);

    timers.expire(start + 20ms);
    EXPECT_EQ(fired, 2);
}

TEST(TimerQueue, PeriodicTimerSkipsMissedTicks) {
    TimerQueue timers;
    const auto start = Clock::now();
    int fired = 0;

    timers.run_every(start + 10ms, 10ms, [&] { ++fired; });

    // The loop stalled well past several intervals. Firing once and moving on
    // is the point: a backlog of catch-up ticks would only stall it further.
    timers.expire(start + 95ms);

    EXPECT_EQ(fired, 1);
    const auto next = timers.next_expiry();
    ASSERT_TRUE(next.has_value());
    EXPECT_EQ(*next, start + 105ms);
}

TEST(TimerQueue, CancelledTimerDoesNotFire) {
    TimerQueue timers;
    const auto start = Clock::now();
    int fired = 0;

    const TimerId id = timers.run_at(start + 10ms, [&] { ++fired; });
    timers.cancel(id);

    timers.expire(start + 100ms);

    EXPECT_EQ(fired, 0);
    EXPECT_FALSE(timers.next_expiry().has_value());
}

TEST(TimerQueue, CancelledTimersDoNotPileUp) {
    // What a stream of short-lived connections does: each leaves behind a timer
    // due long after it has gone. The survivors still fire, in order.
    TimerQueue timers;
    const auto start = Clock::now();
    std::vector<int> fired;

    std::vector<TimerId> ids;
    for (int i = 0; i < 1000; ++i) {
        ids.push_back(timers.run_at(start + 1ms * (1000 - i), [&fired, i] { fired.push_back(i); }));
    }
    for (int i = 0; i < 1000; ++i) {
        if (i % 100 != 0) {
            timers.cancel(ids[i]);
        }
    }

    EXPECT_LE(timers.stored(), 2u * 10u);

    timers.expire(start + 2000ms);
    EXPECT_EQ(fired, (std::vector<int>{900, 800, 700, 600, 500, 400, 300, 200, 100, 0}));
}

TEST(TimerQueue, CallbackCanCancelALaterTimer) {
    TimerQueue timers;
    const auto start = Clock::now();
    int fired = 0;
    TimerId later = 0;

    timers.run_at(start + 10ms, [&] { timers.cancel(later); });
    later = timers.run_at(start + 20ms, [&] { ++fired; });

    timers.expire(start + 100ms);

    EXPECT_EQ(fired, 0);
}

TEST(TimerQueue, PeriodicTimerCanCancelItselfFromItsCallback) {
    TimerQueue timers;
    const auto start = Clock::now();
    int fired = 0;
    TimerId id = 0;

    id = timers.run_every(start + 10ms, 10ms, [&] {
        ++fired;
        timers.cancel(id);
    });

    timers.expire(start + 100ms);

    EXPECT_EQ(fired, 1);
    EXPECT_FALSE(timers.next_expiry().has_value());
}

TEST(TimerQueue, RejectsNonPositiveInterval) {
    TimerQueue timers;
    const auto start = Clock::now();

    EXPECT_THROW(timers.run_every(start, 0ms, [] {}), std::invalid_argument);
    EXPECT_THROW(timers.run_every(start, -1ms, [] {}), std::invalid_argument);
}

}  // namespace
