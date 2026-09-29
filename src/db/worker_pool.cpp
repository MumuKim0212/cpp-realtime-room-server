#include "db/worker_pool.h"

#include <cstdio>
#include <exception>
#include <stdexcept>
#include <utility>

namespace rrs::db {

WorkerPool::WorkerPool(std::size_t worker_count, std::size_t queue_limit,
                       std::chrono::milliseconds drain_budget)
    : queue_limit_(queue_limit), drain_budget_(drain_budget) {
    if (worker_count == 0) {
        throw std::invalid_argument("WorkerPool needs at least one worker");
    }
    if (queue_limit == 0) {
        throw std::invalid_argument("WorkerPool needs room for at least one task");
    }

    workers_.reserve(worker_count);
    for (std::size_t index = 0; index < worker_count; ++index) {
        workers_.emplace_back([this, index] { run_worker(index); });
    }
}

WorkerPool::~WorkerPool() {
    stop();
}

std::size_t WorkerPool::stop() {
    std::size_t dropped = 0;
    {
        std::unique_lock<std::mutex> lock(mutex_);
        if (stopped_) {
            return 0;
        }
        stopped_ = true;
        stopping_ = true;

        // Waiting for the queue to empty rather than for the workers to stop is
        // the same thing one task earlier: whatever is running when this returns
        // still runs to completion before the join below.
        queue_drained_.wait_for(lock, drain_budget_,
                                [this] { return queue_.empty(); });

        dropped = queue_.size();
        queue_.clear();
    }
    work_available_.notify_all();

    for (std::thread& worker : workers_) {
        worker.join();
    }

    if (dropped > 0) {
        // Said out loud. The alternative to dropping these was being killed
        // with them still queued, and either way somebody reading the database
        // afterwards needs to know the tail is missing.
        std::fprintf(stderr,
                     "worker pool: drain budget ran out with %zu tasks still "
                     "queued; they were dropped\n",
                     dropped);
    }
    return dropped;
}

void WorkerPool::submit(Task task) {
    {
        const std::lock_guard<std::mutex> lock(mutex_);
        if (stopping_) {
            throw std::logic_error("WorkerPool::submit after shutdown began");
        }
        queue_.push_back(std::move(task));
    }
    work_available_.notify_one();
}

bool WorkerPool::try_submit(Task task) {
    {
        const std::lock_guard<std::mutex> lock(mutex_);
        if (stopping_ || queue_.size() >= queue_limit_) {
            return false;
        }
        queue_.push_back(std::move(task));
    }
    work_available_.notify_one();
    return true;
}

std::size_t WorkerPool::queued() const {
    const std::lock_guard<std::mutex> lock(mutex_);
    return queue_.size();
}

void WorkerPool::run_worker(std::size_t worker_index) {
    while (true) {
        Task task;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            work_available_.wait(lock,
                                 [this] { return stopping_ || !queue_.empty(); });
            if (queue_.empty()) {
                return;  // shutting down with nothing left to run
            }
            task = std::move(queue_.front());
            queue_.pop_front();
            if (queue_.empty()) {
                queue_drained_.notify_all();
            }
        }

        try {
            task(worker_index);
        } catch (const std::exception& error) {
            // A task that throws has nowhere to unwind to and would take the
            // process down. The database tasks catch their own failures, so
            // what reaches here is the unforeseen -- and losing one task is a
            // smaller loss than losing the server.
            std::fprintf(stderr, "worker pool: task threw: %s\n", error.what());
        }
    }
}

}  // namespace rrs::db
