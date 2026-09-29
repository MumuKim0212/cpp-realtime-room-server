#include "net/timer_queue.h"

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace rrs::net {

bool TimerQueue::by_expiry(const Timer& a, const Timer& b) {
    // std::push_heap builds a max-heap, so the comparison is reversed to put
    // the earliest deadline on top.
    if (a.when != b.when) {
        return a.when > b.when;
    }
    return a.id > b.id;
}

TimerId TimerQueue::push(TimePoint when, Duration interval, Callback cb) {
    const TimerId id = next_id_++;
    live_.insert(id);
    heap_.push_back(Timer{when, interval, id, std::move(cb)});
    std::push_heap(heap_.begin(), heap_.end(), by_expiry);
    return id;
}

TimerId TimerQueue::run_at(TimePoint when, Callback cb) {
    return push(when, Duration::zero(), std::move(cb));
}

TimerId TimerQueue::run_every(TimePoint first_fire, Duration interval, Callback cb) {
    if (interval <= Duration::zero()) {
        throw std::invalid_argument("TimerQueue::run_every needs a positive interval");
    }
    return push(first_fire, interval, std::move(cb));
}

void TimerQueue::cancel(TimerId id) {
    live_.erase(id);
    drop_cancelled_if_outnumbered();
}

void TimerQueue::drop_cancelled_if_outnumbered() {
    if (heap_.size() <= 2 * live_.size()) {
        return;
    }
    std::erase_if(heap_, [this](const Timer& timer) { return live_.count(timer.id) == 0; });
    std::make_heap(heap_.begin(), heap_.end(), by_expiry);
}

void TimerQueue::drop_cancelled_front() {
    while (!heap_.empty() && live_.count(heap_.front().id) == 0) {
        std::pop_heap(heap_.begin(), heap_.end(), by_expiry);
        heap_.pop_back();
    }
}

std::optional<TimePoint> TimerQueue::next_expiry() {
    drop_cancelled_front();
    if (heap_.empty()) {
        return std::nullopt;
    }
    return heap_.front().when;
}

void TimerQueue::expire(TimePoint now) {
    while (true) {
        drop_cancelled_front();
        if (heap_.empty() || heap_.front().when > now) {
            return;
        }

        std::pop_heap(heap_.begin(), heap_.end(), by_expiry);
        Timer timer = std::move(heap_.back());
        heap_.pop_back();

        timer.cb();

        // One-shots are done, and a periodic timer may have cancelled itself
        // from inside the callback that just ran.
        if (timer.interval == Duration::zero() || live_.count(timer.id) == 0) {
            live_.erase(timer.id);
            continue;
        }

        timer.when += timer.interval;
        if (timer.when <= now) {
            // The callback ran longer than its interval. Skip the ticks that
            // were missed instead of firing a burst to catch up, which would
            // only push the loop further behind.
            timer.when = now + timer.interval;
        }
        heap_.push_back(std::move(timer));
        std::push_heap(heap_.begin(), heap_.end(), by_expiry);
    }
}

}  // namespace rrs::net
