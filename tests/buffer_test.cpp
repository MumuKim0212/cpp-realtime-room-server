#include "session/buffer.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <vector>

namespace {

using rrs::session::Buffer;
using Bytes = std::vector<std::uint8_t>;

Bytes readable(const Buffer& buffer) {
    return Bytes(buffer.data(), buffer.data() + buffer.size());
}

void append(Buffer& buffer, const Bytes& bytes) {
    buffer.append(bytes.data(), bytes.size());
}

struct DrainCost {
    std::size_t moved;
    std::size_t consumed;
};

// What a session's send buffer goes through while its peer drains slower than
// the room fills it: every append lands on top of a front that is only partly
// consumed. Answers what reclaiming copied against what it gave back.
DrainCost partial_drain(std::size_t rounds, std::size_t drained_per_round) {
    const Bytes frame(100, 'x');
    Buffer buffer;

    for (std::size_t i = 0; i < rounds; ++i) {
        append(buffer, frame);
        buffer.consume(drained_per_round);
    }
    return DrainCost{buffer.moved_bytes(), rounds * drained_per_round};
}

TEST(Buffer, StartsEmpty) {
    const Buffer buffer;

    EXPECT_TRUE(buffer.empty());
    EXPECT_EQ(buffer.size(), 0u);
}

TEST(Buffer, AppendedBytesBecomeReadable) {
    Buffer buffer;

    append(buffer, {1, 2, 3});

    EXPECT_FALSE(buffer.empty());
    EXPECT_EQ(readable(buffer), (Bytes{1, 2, 3}));
}

TEST(Buffer, ConsumeDropsFromTheFront) {
    Buffer buffer;
    append(buffer, {1, 2, 3, 4});

    buffer.consume(2);

    EXPECT_EQ(readable(buffer), (Bytes{3, 4}));
}

TEST(Buffer, AppendingAfterAPartialConsumeKeepsOrder) {
    // What a half-received frame looks like: the leftover has to stay in front
    // of whatever the next read brings in.
    Buffer buffer;
    append(buffer, {1, 2, 3, 4});
    buffer.consume(3);

    append(buffer, {5, 6});

    EXPECT_EQ(readable(buffer), (Bytes{4, 5, 6}));
}

TEST(Buffer, ConsumingEverythingEmptiesIt) {
    Buffer buffer;
    append(buffer, {1, 2, 3});

    buffer.consume(3);

    EXPECT_TRUE(buffer.empty());

    append(buffer, {9});
    EXPECT_EQ(readable(buffer), (Bytes{9}));
}

TEST(Buffer, KeepsOrderAcrossManyPartialConsumes) {
    // The front is now left in place for many appends at a time, so what comes
    // back out has to be unaffected by when it finally moves.
    Buffer buffer;
    Bytes expected;

    std::uint8_t next = 0;
    for (int round = 0; round < 500; ++round) {
        Bytes chunk;
        for (int i = 0; i < 4; ++i) {
            chunk.push_back(next++);
        }

        append(buffer, chunk);
        expected.insert(expected.end(), chunk.begin(), chunk.end());

        buffer.consume(2);
        expected.erase(expected.begin(), expected.begin() + 2);
    }

    EXPECT_EQ(readable(buffer), expected);
}

TEST(Buffer, ReclaimingNeverCopiesMoreThanItGivesBack) {
    // The compaction policy stated as the property that makes it amortised: a
    // move waits until the consumed front is half the bytes held, so it can
    // never copy more than it reclaims. Reclaiming on every append instead
    // copies the whole remaining backlog each time -- quadratic in the frames
    // queued, paid on the event loop thread while every other connection waits.
    //
    // Counted rather than timed. The cost is a number of bytes copied, and a
    // wall clock reads that only through whatever else the machine is doing.
    constexpr std::size_t kRounds = 5000;

    // A peer that falls far behind: the front never reaches half, so nothing
    // moves at all. This is the shape that used to be quadratic.
    const DrainCost falling_behind = partial_drain(kRounds, /*drained_per_round=*/10);
    EXPECT_LE(falling_behind.moved, falling_behind.consumed);

    // A peer keeping up with most of what is queued crosses the threshold
    // repeatedly, so the bound is doing work here rather than holding vacuously.
    const DrainCost keeping_up = partial_drain(kRounds, /*drained_per_round=*/60);
    EXPECT_GT(keeping_up.moved, 0u);
    EXPECT_LE(keeping_up.moved, keeping_up.consumed);
}

TEST(Buffer, ReleasesALargeAllocationOnceItHasDrained) {
    // A connection that queued a burst must not hold that memory for the rest
    // of its life.
    Buffer buffer;
    const Bytes burst(1024 * 1024, 'x');

    append(buffer, burst);
    ASSERT_GE(buffer.capacity(), burst.size());

    buffer.consume(burst.size());

    EXPECT_TRUE(buffer.empty());
    EXPECT_LT(buffer.capacity(), burst.size());
}

TEST(Buffer, KeepsItsAllocationForOrdinaryTraffic) {
    // The release above is for a connection that queued a burst, not for one
    // that is merely busy: a frame in and out must not free and reallocate.
    Buffer buffer;
    const Bytes frame(256, 'x');

    append(buffer, frame);
    buffer.consume(frame.size());
    const std::size_t settled = buffer.capacity();
    ASSERT_GT(settled, 0u);

    for (int i = 0; i < 100; ++i) {
        append(buffer, frame);
        buffer.consume(frame.size());
    }

    EXPECT_EQ(buffer.capacity(), settled);
}

}  // namespace
