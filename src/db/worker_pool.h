#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

namespace rrs::db {

// A fixed set of threads for work that blocks, so the event loop never does.
//
// The pool knows nothing about the loop. A task that has something to report
// posts the result to the loop itself, which keeps the two independent and
// leaves the loop with no locking of its own.
//
// The queue is bounded. A pool of N threads has a throughput ceiling, and work
// arriving faster than that has to go somewhere: an unbounded queue answers
// "into memory, until there is none", which trades a fast failure for a slow
// one and takes the shutdown down with it, since a queue nobody capped is a
// queue nobody can drain in time. Callers pick their policy by which of the two
// submits they call.
class WorkerPool {
public:
    // The index identifies which worker is running the task, so per-worker
    // resources -- a database connection each, rather than one shared behind a
    // lock -- can be looked up without any synchronisation.
    using Task = std::function<void(std::size_t worker_index)>;

    // Tasks that may be waiting for a worker at once. Sized so that what the
    // queue can hold stays small -- a queued chat write carries its message
    // body, so this is single-digit megabytes -- and so that draining it at
    // shutdown fits inside the budget below.
    static constexpr std::size_t kDefaultQueueLimit = 8192;

    // How long shutdown lets the queue drain before dropping the rest. A
    // database that has gone slow must not be able to hold the process open
    // past the point where its supervisor gives up and kills it, because a
    // killed process finishes nothing at all. Work already running is not
    // interrupted, so the real bound is this plus one task per worker.
    static constexpr auto kDefaultDrainBudget = std::chrono::seconds(3);

    explicit WorkerPool(
        std::size_t worker_count, std::size_t queue_limit = kDefaultQueueLimit,
        std::chrono::milliseconds drain_budget = kDefaultDrainBudget);

    // Runs stop() for anyone who did not.
    ~WorkerPool();

    WorkerPool(const WorkerPool&) = delete;
    WorkerPool& operator=(const WorkerPool&) = delete;

    // Queues work that has to run. For work the server schedules for itself --
    // reading the room list, creating an account -- which arrives in ones and
    // twos at moments when nothing else is queued, and so is never what fills
    // the queue. Throws std::logic_error once shutdown has begun.
    void submit(Task task);

    // Queues work that may be dropped, and answers whether it was taken. For
    // anything a client can ask for as fast as it likes: the caller is left to
    // decide what a refusal means to whoever asked, rather than the queue
    // growing to hide it.
    bool try_submit(Task task);

    // Stops taking new work, lets what is already queued run for up to the
    // drain budget, drops whatever is still queued after that, joins, and
    // answers how many were dropped. Zero means the shutdown finished
    // everything. Calling it twice is harmless and answers zero the second
    // time; the destructor calls it for anyone who did not.
    std::size_t stop();

    std::size_t worker_count() const { return workers_.size(); }

    // Tasks waiting for a worker, for tests and diagnostics.
    std::size_t queued() const;

private:
    void run_worker(std::size_t worker_index);

    std::vector<std::thread> workers_;
    std::deque<Task> queue_;
    mutable std::mutex mutex_;
    std::condition_variable work_available_;

    // Raised when a worker takes the last task off the queue, which is what
    // lets shutdown wait for the queue to empty instead of polling it.
    std::condition_variable queue_drained_;

    std::size_t queue_limit_;
    std::chrono::milliseconds drain_budget_;
    bool stopping_ = false;
    bool stopped_ = false;
};

}  // namespace rrs::db
