#pragma once

#include <cstdint>
#include <functional>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "net/timer_queue.h"

namespace rrs::net {

// A single-threaded epoll reactor. Readiness events and timers are driven by
// the same loop, so everything the loop owns (sessions, rooms) belongs to one
// thread and needs no locking.
//
// Every method must be called from the thread running run(), which in practice
// means from inside a callback. post() is the single exception and the only
// way into the loop from anywhere else.
class EventLoop {
public:
    // Receives the epoll event mask that fired: EPOLLIN, EPOLLOUT, EPOLLHUP,
    // EPOLLERR and so on.
    using IoCallback = std::function<void(std::uint32_t events)>;
    using TimerCallback = TimerQueue::Callback;

    EventLoop();
    ~EventLoop();

    EventLoop(const EventLoop&) = delete;
    EventLoop& operator=(const EventLoop&) = delete;

    // `events` is a raw epoll mask. Pass EPOLLET to opt into edge triggering;
    // the loop does not impose a trigger mode, since that choice belongs to
    // whoever owns the fd and knows how it will be drained.
    void add_fd(int fd, std::uint32_t events, IoCallback cb);
    void mod_fd(int fd, std::uint32_t events);
    void remove_fd(int fd);

    TimerId run_after(Duration delay, TimerCallback cb);
    TimerId run_every(Duration interval, TimerCallback cb);
    void cancel_timer(TimerId id);

    // Queues `callback` to run on the loop thread, waking the loop if it is
    // asleep. Safe to call from any thread, and the way a worker hands a
    // finished piece of blocking work back without the loop ever locking
    // anything of its own.
    void post(std::function<void()> callback);

    // Blocks until stop() is called.
    void run();

    // Makes run() return once the current iteration finishes. Belongs to the
    // loop thread like everything else here; from elsewhere, post it.
    void stop();

private:
    // Milliseconds for epoll_wait. Never -1 while a timer is pending, or the
    // loop would sleep through its deadline waiting for an fd that may never
    // become ready.
    int next_timeout_ms();

    void run_posted_callbacks();

    int epoll_fd_;

    // Readable whenever another thread has queued something, which is what
    // gets epoll_wait to return.
    int wakeup_fd_;

    bool running_ = false;
    TimerQueue timers_;
    std::unordered_map<int, IoCallback> handlers_;

    std::mutex posted_mutex_;
    std::vector<std::function<void()>> posted_;
};

}  // namespace rrs::net
