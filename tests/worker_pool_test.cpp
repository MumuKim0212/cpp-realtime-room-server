#include "db/worker_pool.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>

#include "net/event_loop.h"

namespace {

using namespace std::chrono_literals;

using rrs::db::WorkerPool;
using rrs::net::EventLoop;

TEST(WorkerPool, RunsEverySubmittedTask) {
    std::atomic<int> ran{0};
    {
        WorkerPool pool(2);
        for (int i = 0; i < 10; ++i) {
            pool.submit([&ran](std::size_t) { ++ran; });
        }
    }  // destruction drains the queue and joins

    EXPECT_EQ(ran, 10);
}

TEST(WorkerPool, RunsTasksAwayFromTheSubmittingThread) {
    // The entire reason the pool exists: whatever blocks in here must not
    // block the thread that handed the work over.
    std::mutex mutex;
    std::vector<std::thread::id> ran_on;
    {
        WorkerPool pool(2);
        for (int i = 0; i < 8; ++i) {
            pool.submit([&](std::size_t) {
                const std::lock_guard<std::mutex> lock(mutex);
                ran_on.push_back(std::this_thread::get_id());
            });
        }
    }

    ASSERT_EQ(ran_on.size(), 8u);
    for (const std::thread::id id : ran_on) {
        EXPECT_NE(id, std::this_thread::get_id());
    }
}

TEST(WorkerPool, HandsEachTaskTheIndexOfItsWorker) {
    // Per-worker resources are looked up by this index, so it has to stay in
    // range no matter which worker picks the task up.
    std::mutex mutex;
    std::vector<std::size_t> indices;
    {
        WorkerPool pool(3);
        for (int i = 0; i < 20; ++i) {
            pool.submit([&](std::size_t worker_index) {
                const std::lock_guard<std::mutex> lock(mutex);
                indices.push_back(worker_index);
            });
        }
    }

    ASSERT_EQ(indices.size(), 20u);
    for (const std::size_t index : indices) {
        EXPECT_LT(index, 3u);
    }
}

TEST(WorkerPool, FinishesQueuedWorkBeforeShuttingDown) {
    // Shutdown runs out what is queued, within the drain budget: by then the
    // queue is usually writes somebody is counting on having happened. Fifty
    // milliseconds of work sits well inside that budget, so nothing is dropped.
    std::atomic<int> ran{0};
    {
        WorkerPool pool(1);
        for (int i = 0; i < 50; ++i) {
            pool.submit([&ran](std::size_t) {
                std::this_thread::sleep_for(1ms);
                ++ran;
            });
        }
    }

    EXPECT_EQ(ran, 50);
}

TEST(WorkerPool, RefusesAPoolWithNoWorkers) {
    EXPECT_THROW({ WorkerPool pool(0); }, std::invalid_argument);
}

TEST(WorkerPool, TurnsWorkAwayOnceTheQueueIsFull) {
    // The point of the bound: work arriving faster than the pool can run it is
    // refused where the caller can see it, instead of piling up in memory.
    std::mutex gate;
    std::unique_lock<std::mutex> held(gate);

    WorkerPool pool(1, /*queue_limit=*/2);
    pool.submit([&gate](std::size_t) { const std::lock_guard<std::mutex> wait(gate); });

    // The worker is stuck on the gate, so nothing below it ever starts.
    while (pool.queued() > 0) {
        std::this_thread::sleep_for(1ms);  // wait for the blocker to be picked up
    }
    EXPECT_TRUE(pool.try_submit([](std::size_t) {}));
    EXPECT_TRUE(pool.try_submit([](std::size_t) {}));
    EXPECT_FALSE(pool.try_submit([](std::size_t) {}));

    held.unlock();
}

TEST(WorkerPool, StillTakesWorkThatHasToRunWhenTheQueueIsFull) {
    // submit() is for what the server schedules for itself, which arrives in
    // ones and twos and is never what filled the queue.
    std::mutex gate;
    std::unique_lock<std::mutex> held(gate);

    std::atomic<bool> ran{false};
    {
        WorkerPool pool(1, /*queue_limit=*/1);
        pool.submit([&gate](std::size_t) { const std::lock_guard<std::mutex> wait(gate); });
        while (pool.queued() > 0) {
            std::this_thread::sleep_for(1ms);
        }

        ASSERT_TRUE(pool.try_submit([](std::size_t) {}));
        ASSERT_FALSE(pool.try_submit([](std::size_t) {}));

        pool.submit([&ran](std::size_t) { ran = true; });
        held.unlock();
    }

    EXPECT_TRUE(ran);
}

TEST(WorkerPool, DropsWhatTheDrainBudgetDoesNotReach) {
    // A database that has gone slow must not be able to hold the process open
    // until its supervisor kills it, which finishes nothing at all.
    std::atomic<int> ran{0};
    WorkerPool pool(1, WorkerPool::kDefaultQueueLimit, /*drain_budget=*/50ms);
    for (int i = 0; i < 200; ++i) {
        pool.submit([&ran](std::size_t) {
            std::this_thread::sleep_for(5ms);
            ++ran;
        });
    }

    const std::size_t dropped = pool.stop();

    EXPECT_GT(dropped, 0u);
    EXPECT_LT(ran, 200);
    EXPECT_EQ(ran + static_cast<int>(dropped), 200);
}

TEST(WorkerPool, StoppingTwiceIsHarmless) {
    WorkerPool pool(2);
    pool.submit([](std::size_t) {});

    EXPECT_EQ(pool.stop(), 0u);
    EXPECT_EQ(pool.stop(), 0u);
}

TEST(WorkerPool, ResultsComeBackToTheLoopThread) {
    // The boundary the whole design rests on: the blocking half runs on a
    // worker, the half that touches loop-owned state runs on the loop, and
    // nothing in between needs a lock.
    EventLoop loop;
    WorkerPool pool(2);

    std::thread::id worked_on;
    std::thread::id handled_on;

    pool.submit([&](std::size_t) {
        worked_on = std::this_thread::get_id();
        loop.post([&] {
            handled_on = std::this_thread::get_id();
            loop.stop();
        });
    });

    loop.run();

    EXPECT_NE(worked_on, std::this_thread::get_id());
    EXPECT_EQ(handled_on, std::this_thread::get_id());
}

}  // namespace
