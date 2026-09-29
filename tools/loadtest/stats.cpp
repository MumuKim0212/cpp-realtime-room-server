#include "stats.h"

#include <algorithm>
#include <cmath>

namespace rrs::loadtest {
namespace {

// Nearest-rank, without interpolation: every number reported is the duration
// of a delivery that actually happened rather than a point between two of them.
std::int64_t percentile(const std::vector<std::int64_t>& sorted, double share) {
    const auto rank = static_cast<std::size_t>(
        std::ceil(share * static_cast<double>(sorted.size())));
    return sorted[std::clamp<std::size_t>(rank, 1, sorted.size()) - 1];
}

}  // namespace

Latencies::Summary Latencies::summarise() {
    Summary summary;
    if (samples_.empty()) {
        return summary;
    }

    std::sort(samples_.begin(), samples_.end());

    // Summed as a double: the total of a few million nanosecond readings would
    // not overflow an int64, but the mean is the only place precision beats
    // exactness here and the cast has to happen either way.
    double total = 0.0;
    for (const std::int64_t sample : samples_) {
        total += static_cast<double>(sample);
    }

    summary.p50 = percentile(samples_, 0.50);
    summary.p90 = percentile(samples_, 0.90);
    summary.p99 = percentile(samples_, 0.99);
    summary.max = samples_.back();
    summary.mean = total / static_cast<double>(samples_.size());
    return summary;
}

}  // namespace rrs::loadtest
