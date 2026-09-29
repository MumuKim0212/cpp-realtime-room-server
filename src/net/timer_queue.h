#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <unordered_set>
#include <vector>

namespace rrs::net {

using Clock = std::chrono::steady_clock;
using TimePoint = Clock::time_point;
using Duration = Clock::duration;

using TimerId = std::uint64_t;

// Timers ordered by expiry. The queue never sleeps on its own: it reports when
// the next timer is due so the event loop can wait for exactly that long, and
// runs the callbacks that have come due when the loop asks it to.
//
// Not thread-safe. It is owned by one event loop and touched only from that
// loop's thread.
class TimerQueue {
public:
    using Callback = std::function<void()>;

    // Runs `cb` once at `when`.
    TimerId run_at(TimePoint when, Callback cb);

    // Runs `cb` at `first_fire` and every `interval` after that. `interval`
    // must be positive; a zero interval would expire without ever advancing.
    TimerId run_every(TimePoint first_fire, Duration interval, Callback cb);

    // Stops a timer. Safe to call from inside a callback, including on the
    // timer that is currently running. Cancelling an id that already fired or
    // was never issued does nothing.
    void cancel(TimerId id);

    // Expiry of the earliest live timer, or nullopt when none are pending.
    std::optional<TimePoint> next_expiry();

    // Runs every callback due at `now`. Periodic timers re-arm themselves.
    void expire(TimePoint now);

    // Timers held, cancelled ones included. Cancelling never lets this grow
    // past twice the timers still pending. Nothing in the server needs it; it
    // is here so that bound can be tested rather than assumed.
    std::size_t stored() const { return heap_.size(); }

private:
    struct Timer {
        TimePoint when;
        Duration interval;  // zero for one-shot
        TimerId id;
        Callback cb;
    };

    // Orders the heap: earliest expiry first, ties broken so that timers with
    // the same deadline fire in the order they were registered.
    static bool by_expiry(const Timer& a, const Timer& b);

    // Drops cancelled timers from the top of the heap so the front is always a
    // timer that will actually run. Cancellation is lazy: `cancel` only forgets
    // the id, since removing from the middle of a heap costs more than it saves.
    void drop_cancelled_front();

    // Lazy cancellation alone keeps a cancelled timer until it reaches the
    // front -- for a session's idle timer, five minutes after the session is
    // gone, so connections that come and go quickly pile them up. Once the
    // cancelled outnumber the live, this drops them all and rebuilds the heap;
    // the cancellations it took to get there pay for the rebuild.
    void drop_cancelled_if_outnumbered();

    TimerId push(TimePoint when, Duration interval, Callback cb);

    std::vector<Timer> heap_;
    std::unordered_set<TimerId> live_;
    TimerId next_id_ = 1;
};

}  // namespace rrs::net
