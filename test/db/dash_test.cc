#include "db/storage/dash/dash.h"

#include <atomic>
#include <gtest/gtest.h>
#include <optional>
#include <thread>
#include <vector>

namespace {

struct FixedKey {
    uint64_t hash = 0;
    int      id   = 0;

    auto operator==(const FixedKey&) const -> bool = default;
};

struct FixedHash {
    auto operator()(const FixedKey& key) const -> size_t { return key.hash; }
};

using FixedEq = std::equal_to<FixedKey>;

auto MakeHash(uint64_t prefix, size_t prefix_len, size_t home_bucket, uint8_t salt) -> uint64_t {
    uint64_t hash = 0;
    if (prefix_len != 0) {
        hash |= prefix << (64 - prefix_len);
    }
    hash |= static_cast<uint64_t>(home_bucket) << 8U;
    hash |= salt;
    return hash;
}

auto MakeKey(int id, uint64_t prefix, size_t prefix_len, size_t home_bucket, uint8_t salt)
    -> FixedKey {
    return FixedKey{
        .hash = MakeHash(prefix, prefix_len, home_bucket, salt),
        .id   = id,
    };
}

template <size_t RegularBuckets = 4, size_t StashBuckets = 1, size_t SlotsPerBucket = 1>
using TestDash = idlekv::dash::DashEH<FixedKey, int, FixedHash, FixedEq, RegularBuckets,
                                      StashBuckets, SlotsPerBucket>;

TEST(DashEHTest, BasicInsertFindErase) {
    TestDash<4, 1, 2> table({.initial_global_depth = 0, .merge_threshold = 0.8});

    auto k1 = MakeKey(1, 0, 0, 0, 1);
    auto k2 = MakeKey(2, 0, 0, 1, 2);

    EXPECT_TRUE(table.insert(k1, 11));
    EXPECT_TRUE(table.insert(k2, 22));
    EXPECT_FALSE(table.insert(k1, 33));

    ASSERT_EQ(table.find(k1), std::optional<int>(11));
    ASSERT_EQ(table.find(k2), std::optional<int>(22));

    EXPECT_TRUE(table.erase(k1));
    EXPECT_FALSE(table.erase(k1));
    EXPECT_EQ(table.find(k1), std::nullopt);
    ASSERT_EQ(table.find(k2), std::optional<int>(22));
}

TEST(DashEHTest, SplitGrowsDirectory) {
    TestDash<> table({.initial_global_depth = 0, .merge_threshold = 0.9});

    auto k1 = MakeKey(1, 0, 1, 1, 1);
    auto k2 = MakeKey(2, 0, 1, 1, 2);
    auto k3 = MakeKey(3, 0, 1, 1, 3);
    auto k4 = MakeKey(4, 1, 1, 1, 4);

    EXPECT_TRUE(table.insert(k1, 10));
    EXPECT_TRUE(table.insert(k2, 20));
    EXPECT_TRUE(table.insert(k3, 30));
    EXPECT_TRUE(table.insert(k4, 40));

    EXPECT_EQ(table.directory_depth(), 1U);
    EXPECT_EQ(table.unique_segments(), 2U);
    EXPECT_GE(table.stats().split_count, 1U);

    EXPECT_EQ(table.find(k1), std::optional<int>(10));
    EXPECT_EQ(table.find(k2), std::optional<int>(20));
    EXPECT_EQ(table.find(k3), std::optional<int>(30));
    EXPECT_EQ(table.find(k4), std::optional<int>(40));
}

TEST(DashEHTest, MergeShrinksDirectory) {
    TestDash<> table({.initial_global_depth = 0, .merge_threshold = 0.95});

    auto k1 = MakeKey(1, 0, 1, 1, 1);
    auto k2 = MakeKey(2, 0, 1, 1, 2);
    auto k3 = MakeKey(3, 0, 1, 1, 3);
    auto k4 = MakeKey(4, 1, 1, 1, 4);

    ASSERT_TRUE(table.insert(k1, 10));
    ASSERT_TRUE(table.insert(k2, 20));
    ASSERT_TRUE(table.insert(k3, 30));
    ASSERT_TRUE(table.insert(k4, 40));
    ASSERT_EQ(table.directory_depth(), 1U);

    EXPECT_TRUE(table.erase(k2));

    EXPECT_EQ(table.directory_depth(), 0U);
    EXPECT_EQ(table.unique_segments(), 1U);
    EXPECT_GE(table.stats().merge_count, 1U);
    EXPECT_GE(table.stats().directory_shrink_count, 1U);

    EXPECT_EQ(table.find(k1), std::optional<int>(10));
    EXPECT_EQ(table.find(k3), std::optional<int>(30));
    EXPECT_EQ(table.find(k4), std::optional<int>(40));
}

TEST(DashEHTest, BalancedInsertChoosesLessLoadedNeighbor) {
    TestDash<4, 1, 2> table({.initial_global_depth = 0, .merge_threshold = 0.8});

    auto first  = MakeKey(1, 0, 0, 1, 1);
    auto second = MakeKey(2, 0, 0, 1, 2);

    ASSERT_TRUE(table.insert(first, 10));
    ASSERT_TRUE(table.insert(second, 20));

    auto first_pos  = table.debug_locate(first);
    auto second_pos = table.debug_locate(second);
    ASSERT_TRUE(first_pos.has_value());
    ASSERT_TRUE(second_pos.has_value());

    EXPECT_EQ(first_pos->bucket, 1U);
    EXPECT_EQ(second_pos->bucket, 2U);
    EXPECT_FALSE(second_pos->in_stash);
}

TEST(DashEHTest, DisplacementMovesOwnedNeighborEntryForward) {
    TestDash<> table({.initial_global_depth = 0, .merge_threshold = 0.8});

    auto home_key       = MakeKey(1, 0, 0, 1, 1);
    auto neighbor_owner = MakeKey(2, 0, 0, 2, 2);
    auto displaced_in   = MakeKey(3, 0, 0, 1, 3);

    ASSERT_TRUE(table.insert(home_key, 10));
    ASSERT_TRUE(table.insert(neighbor_owner, 20));
    ASSERT_TRUE(table.insert(displaced_in, 30));

    auto moved_pos    = table.debug_locate(neighbor_owner);
    auto inserted_pos = table.debug_locate(displaced_in);
    ASSERT_TRUE(moved_pos.has_value());
    ASSERT_TRUE(inserted_pos.has_value());

    EXPECT_EQ(moved_pos->bucket, 3U);
    EXPECT_EQ(inserted_pos->bucket, 2U);
    EXPECT_FALSE(inserted_pos->in_stash);
}

TEST(DashEHTest, StashingHandlesBlockedRegularBuckets) {
    TestDash<> table({.initial_global_depth = 0, .merge_threshold = 0.8});

    auto first  = MakeKey(1, 0, 0, 1, 1);
    auto second = MakeKey(2, 0, 0, 1, 2);
    auto third  = MakeKey(3, 0, 0, 1, 3);

    ASSERT_TRUE(table.insert(first, 10));
    ASSERT_TRUE(table.insert(second, 20));
    ASSERT_TRUE(table.insert(third, 30));

    auto first_pos  = table.debug_locate(first);
    auto second_pos = table.debug_locate(second);
    auto third_pos  = table.debug_locate(third);
    ASSERT_TRUE(first_pos.has_value());
    ASSERT_TRUE(second_pos.has_value());
    ASSERT_TRUE(third_pos.has_value());

    EXPECT_EQ(first_pos->bucket, 1U);
    EXPECT_EQ(second_pos->bucket, 2U);
    EXPECT_TRUE(third_pos->in_stash);
}

TEST(DashEHTest, ConcurrentReadersSurviveSplitAndMerge) {
    TestDash<> table({.initial_global_depth = 0, .merge_threshold = 0.95});

    auto stable = MakeKey(100, 0, 1, 1, 1);
    ASSERT_TRUE(table.insert(stable, 999));

    std::atomic<bool> stop{false};
    std::atomic<bool> failed{false};

    std::vector<std::thread> readers;
    for (size_t i = 0; i < 4; ++i) {
        readers.emplace_back([&] {
            while (!stop.load(std::memory_order_acquire)) {
                auto value = table.find(stable);
                if (!value || *value != 999) {
                    failed.store(true, std::memory_order_release);
                    stop.store(true, std::memory_order_release);
                    return;
                }
            }
        });
    }

    std::thread writer([&] {
        for (int round = 0; round < 200 && !failed.load(std::memory_order_acquire); ++round) {
            auto a = MakeKey(1000 + round * 3 + 0, 0, 1, 1, 2);
            auto b = MakeKey(1000 + round * 3 + 1, 0, 1, 1, 3);
            auto c = MakeKey(1000 + round * 3 + 2, 1, 1, 1, 4);

            EXPECT_TRUE(table.insert(a, round));
            EXPECT_TRUE(table.insert(b, round));
            EXPECT_TRUE(table.insert(c, round));

            EXPECT_TRUE(table.erase(a));
            EXPECT_TRUE(table.erase(b));
            EXPECT_TRUE(table.erase(c));
        }
        stop.store(true, std::memory_order_release);
    });

    writer.join();
    for (auto& reader : readers) {
        reader.join();
    }

    EXPECT_FALSE(failed.load(std::memory_order_acquire));
    EXPECT_EQ(table.find(stable), std::optional<int>(999));
    EXPECT_GE(table.stats().split_count, 1U);
    EXPECT_GE(table.stats().merge_count, 1U);
}

} // namespace
