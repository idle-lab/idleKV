#include "db/storage/zset.h"

#include <cstring>
#include <gtest/gtest.h>
#include <memory_resource>
#include <string>
#include <vector>

namespace {

using idlekv::ZSet;

auto EncodeScoreMember(double score, std::string_view member) -> std::string {
    std::string key;
    idlekv::ArtKeyCodec::EncodePieces(key, score, member);
    return key;
}

auto BytesToString(std::span<const idlekv::byte> bytes) -> std::string {
    return {reinterpret_cast<const char*>(bytes.data()), bytes.size()};
}

TEST(ZSetTest, UnderlyingArtEraseRemovesBinaryScoreMemberKey) {
    std::pmr::monotonic_buffer_resource mr;
    idlekv::Art<std::nullptr_t>         art(&mr);

    const std::string alice = EncodeScoreMember(1.5, "alice");
    const std::string bob   = EncodeScoreMember(3.0, "bob");

    EXPECT_EQ(
        art.Insert({reinterpret_cast<const idlekv::byte*>(alice.data()), alice.size()}, nullptr)
            .status,
        idlekv::Art<std::nullptr_t>::InsertResutl::OK);
    EXPECT_EQ(
        art.Insert({reinterpret_cast<const idlekv::byte*>(bob.data()), bob.size()}, nullptr).status,
        idlekv::Art<std::nullptr_t>::InsertResutl::OK);
    EXPECT_EQ(art.Erase({reinterpret_cast<const idlekv::byte*>(alice.data()), alice.size()}), 1U);

    std::vector<std::string> keys;
    art.Iterate([&](std::span<const idlekv::byte> key, std::nullptr_t&) -> bool {
        keys.push_back(BytesToString(key));
        return true;
    });

    ASSERT_EQ(keys.size(), 1U);
    EXPECT_EQ(keys[0], bob);
}

TEST(ZSetTest, AddUpdatesExistingMembersWithoutChangingCardinality) {
    std::pmr::monotonic_buffer_resource mr;
    ZSet                                zset(&mr);

    EXPECT_TRUE(zset.Add("alice", 1.5));
    EXPECT_TRUE(zset.Add("bob", 3.0));
    EXPECT_FALSE(zset.Add("alice", 2.0));

    const auto range = zset.Range(0, -1);
    ASSERT_EQ(range.size(), 2U);
    EXPECT_EQ(range[0].member, "alice");
    EXPECT_DOUBLE_EQ(range[0].score, 2.0);
    EXPECT_EQ(range[1].member, "bob");
    EXPECT_DOUBLE_EQ(range[1].score, 3.0);
    EXPECT_EQ(zset.Size(), 2U);
}

TEST(ZSetTest, RangeSupportsNegativeIndexesAndScoreOrdering) {
    std::pmr::monotonic_buffer_resource mr;
    ZSet                                zset(&mr);

    ASSERT_TRUE(zset.Add("bob", 2.0));
    ASSERT_TRUE(zset.Add("alice", 1.0));
    ASSERT_TRUE(zset.Add("charlie", 2.0));
    ASSERT_TRUE(zset.Add("david", 4.5));

    const auto tail = zset.Range(-3, -1);
    ASSERT_EQ(tail.size(), 3U);
    EXPECT_EQ(tail[0].member, "bob");
    EXPECT_DOUBLE_EQ(tail[0].score, 2.0);
    EXPECT_EQ(tail[1].member, "charlie");
    EXPECT_DOUBLE_EQ(tail[1].score, 2.0);
    EXPECT_EQ(tail[2].member, "david");
    EXPECT_DOUBLE_EQ(tail[2].score, 4.5);
}

TEST(ZSetTest, RemDeletesMembersAndKeepsOrderingStable) {
    std::pmr::monotonic_buffer_resource mr;
    ZSet                                zset(&mr);

    ASSERT_TRUE(zset.Add("alice", 1.0));
    ASSERT_TRUE(zset.Add("bob", 2.0));
    ASSERT_TRUE(zset.Add("charlie", 3.0));

    EXPECT_FALSE(zset.Rem("missing"));
    EXPECT_TRUE(zset.Rem("bob"));

    const auto range = zset.Range(0, -1);
    ASSERT_EQ(range.size(), 2U);
    EXPECT_EQ(range[0].member, "alice");
    EXPECT_EQ(range[1].member, "charlie");
    EXPECT_EQ(zset.Size(), 2U);
}

TEST(ZSetTest, RangeHandlesMembersWithEmbeddedZeroAndEscapeBytes) {
    std::pmr::monotonic_buffer_resource mr;
    ZSet                                zset(&mr);

    const std::string alpha = std::string("alpha\0beta", 10);
    const std::string bravo = std::string("alpha\1beta", 10);

    ASSERT_TRUE(zset.Add(alpha, 1.0));
    ASSERT_TRUE(zset.Add(bravo, 1.0));

    const auto range = zset.Range(0, -1);
    ASSERT_EQ(range.size(), 2U);
    EXPECT_EQ(range[0].member, alpha);
    EXPECT_EQ(range[1].member, bravo);
}

TEST(ZSetTest, RangeHonorsMemberTieBreakOrderingForEqualScores) {
    std::pmr::monotonic_buffer_resource mr;
    ZSet                                zset(&mr);

    ASSERT_TRUE(zset.Add("charlie", 1.0));
    ASSERT_TRUE(zset.Add("alice", 1.0));
    ASSERT_TRUE(zset.Add("bob", 1.0));

    const auto range = zset.Range(0, -1);
    ASSERT_EQ(range.size(), 3U);
    EXPECT_EQ(range[0].member, "alice");
    EXPECT_EQ(range[1].member, "bob");
    EXPECT_EQ(range[2].member, "charlie");
}

TEST(ZSetTest, RangeReturnsLargeOffsetSliceWithoutScanningPrefixInCaller) {
    std::pmr::monotonic_buffer_resource mr;
    ZSet                                zset(&mr);

    for (int i = 0; i < 256; ++i) {
        ASSERT_TRUE(zset.Add(std::string("member-") + std::to_string(i), static_cast<double>(i)));
    }

    const auto range = zset.Range(120, 125);
    ASSERT_EQ(range.size(), 6U);
    for (size_t i = 0; i < range.size(); ++i) {
        EXPECT_EQ(range[i].member, std::string("member-") + std::to_string(120 + i));
        EXPECT_DOUBLE_EQ(range[i].score, static_cast<double>(120 + i));
    }
}

} // namespace
