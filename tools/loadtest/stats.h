#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

namespace rrs::loadtest {

// Every delivery the run timed, in nanoseconds, summarised once at the end.
//
// The samples are kept rather than folded into a running estimate. A run of any
// size this tool produces fits in memory at eight bytes each, and sorting once
// costs less than maintaining an ordered structure across millions of
// deliveries -- while giving percentiles that are exact rather than estimated.
class Latencies {
public:
    struct Summary {
        std::int64_t p50 = 0;
        std::int64_t p90 = 0;
        std::int64_t p99 = 0;
        std::int64_t max = 0;
        double mean = 0.0;
    };

    void add(std::int64_t nanoseconds) { samples_.push_back(nanoseconds); }

    // Taken before the run rather than grown during it: a reallocation part
    // way through copies megabytes of samples, and the deliveries waiting on
    // that copy would be timed as though the server had been slow.
    void reserve(std::size_t samples) { samples_.reserve(samples); }

    std::size_t count() const { return samples_.size(); }

    // Sorts as it goes, so this belongs at the end of a run and not in it.
    Summary summarise();

private:
    std::vector<std::int64_t> samples_;
};

// What the run did, as opposed to how quickly it did it. Every field is
// touched only from the loop thread, which is the only thread this tool has
// besides the database workers the seeding step borrows.
struct Counters {
    std::size_t connected = 0;
    std::size_t joined = 0;
    std::size_t login_failed = 0;
    std::size_t join_failed = 0;
    std::size_t messages_sent = 0;
    std::size_t deliveries = 0;
    std::size_t protocol_errors = 0;

    // Connections that ended before the run did. Any of these means the
    // numbers describe a smaller population than the one that was asked for.
    std::size_t lost = 0;
};

}  // namespace rrs::loadtest
