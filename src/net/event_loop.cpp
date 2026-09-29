#include "net/event_loop.h"

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <system_error>
#include <utility>
#include <vector>

namespace rrs::net {
namespace {

// How many ready events a single epoll_wait call may report. Grown whenever a
// call comes back completely full, which means more were queued behind them.
constexpr std::size_t kInitialEventCapacity = 64;

// Ceiling for the epoll_wait timeout. A timer scheduled far enough ahead would
// otherwise overflow the int the syscall takes; waking early and going back to
// sleep costs nothing.
constexpr int kMaxTimeoutMs = 60 * 1000;

[[noreturn]] void throw_errno(const char* what) {
    throw std::system_error(errno, std::system_category(), what);
}

}  // namespace

EventLoop::EventLoop()
    : epoll_fd_(::epoll_create1(EPOLL_CLOEXEC)),
      wakeup_fd_(::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)) {
    if (epoll_fd_ < 0) {
        throw_errno("epoll_create1");
    }
    if (wakeup_fd_ < 0) {
        const int saved = errno;
        ::close(epoll_fd_);
        errno = saved;
        throw_errno("eventfd");
    }
    add_fd(wakeup_fd_, EPOLLIN,
           [this](std::uint32_t) { run_posted_callbacks(); });
}

EventLoop::~EventLoop() {
    ::close(wakeup_fd_);
    ::close(epoll_fd_);
}

void EventLoop::add_fd(int fd, std::uint32_t events, IoCallback cb) {
    epoll_event ev{};
    ev.events = events;
    ev.data.fd = fd;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) < 0) {
        throw_errno("epoll_ctl(EPOLL_CTL_ADD)");
    }
    handlers_[fd] = std::move(cb);
}

void EventLoop::mod_fd(int fd, std::uint32_t events) {
    epoll_event ev{};
    ev.events = events;
    ev.data.fd = fd;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev) < 0) {
        throw_errno("epoll_ctl(EPOLL_CTL_MOD)");
    }
}

void EventLoop::remove_fd(int fd) {
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr) < 0) {
        throw_errno("epoll_ctl(EPOLL_CTL_DEL)");
    }
    handlers_.erase(fd);
}

TimerId EventLoop::run_after(Duration delay, TimerCallback cb) {
    return timers_.run_at(Clock::now() + delay, std::move(cb));
}

TimerId EventLoop::run_every(Duration interval, TimerCallback cb) {
    return timers_.run_every(Clock::now() + interval, interval, std::move(cb));
}

void EventLoop::cancel_timer(TimerId id) {
    timers_.cancel(id);
}

void EventLoop::post(std::function<void()> callback) {
    {
        const std::lock_guard<std::mutex> lock(posted_mutex_);
        posted_.push_back(std::move(callback));
    }

    // The counter carries no information; writing to it only knocks on
    // epoll_wait. A knock that arrives after the queue was already drained
    // costs one harmless wakeup.
    const std::uint64_t knock = 1;
    if (::write(wakeup_fd_, &knock, sizeof(knock)) != sizeof(knock)) {
        throw_errno("write(eventfd)");
    }
}

void EventLoop::run_posted_callbacks() {
    std::uint64_t knocks = 0;
    while (::read(wakeup_fd_, &knocks, sizeof(knocks)) > 0) {
    }

    // Swapped out before running: a callback is free to post more work, which
    // would otherwise be appended to the container being walked.
    std::vector<std::function<void()>> ready;
    {
        const std::lock_guard<std::mutex> lock(posted_mutex_);
        ready.swap(posted_);
    }

    for (auto& callback : ready) {
        callback();
    }
}

void EventLoop::stop() {
    running_ = false;
}

int EventLoop::next_timeout_ms() {
    const auto next = timers_.next_expiry();
    if (!next) {
        return -1;  // nothing scheduled: sleep until an fd is ready
    }

    const Duration remaining = *next - Clock::now();
    if (remaining <= Duration::zero()) {
        return 0;
    }

    // Rounded up, because truncating would wake the loop a fraction of a
    // millisecond early and spin until the deadline actually passed.
    const auto ms = std::chrono::ceil<std::chrono::milliseconds>(remaining).count();
    return static_cast<int>(std::min<decltype(ms)>(ms, kMaxTimeoutMs));
}

void EventLoop::run() {
    std::vector<epoll_event> ready(kInitialEventCapacity);
    running_ = true;

    while (running_) {
        const int count = ::epoll_wait(epoll_fd_, ready.data(),
                                       static_cast<int>(ready.size()),
                                       next_timeout_ms());
        if (count < 0) {
            if (errno == EINTR) {
                continue;
            }
            throw_errno("epoll_wait");
        }

        // I/O is handled before timers so that a tick acts on everything that
        // has already arrived rather than on state one iteration stale.
        for (int i = 0; i < count; ++i) {
            const auto it = handlers_.find(ready[i].data.fd);
            if (it == handlers_.end()) {
                continue;  // dropped by an earlier callback in this same batch
            }
            // Copied deliberately: a callback is allowed to remove its own fd,
            // which would otherwise destroy the std::function mid-call.
            IoCallback cb = it->second;
            cb(ready[i].events);
        }

        timers_.expire(Clock::now());

        if (count == static_cast<int>(ready.size())) {
            ready.resize(ready.size() * 2);
        }
    }
}

}  // namespace rrs::net
