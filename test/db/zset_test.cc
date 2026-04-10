#include "db/storage/zset.h"

#include <gtest/gtest.h>
#include <cstring>
#include <memory_resource>
#include <string>
#include <vector>

namespace {

using idlekv::ZSet;

auto EncodeScoreMember(double score, std::string_view member) -> std::string {
    idlekv::ArtKey encoded_score(score);
    std::string    key(sizeof(uint64_t) + member.size(), '\0');
    std::memcpy(key.data(), encoded_score.Data(), sizeof(uint64_t));
    std::memcpy(key.data() + sizeof(uint64_t), member.data(), member.size());
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

    EXPECT_EQ(art.Insert({reinterpret_cast<const idlekv::byte*>(alice.data()), alice.size()},
                         nullptr)
                  .s,
              idlekv::Art<std::nullptr_t>::InsertResutl::OK);
    EXPECT_EQ(art.Insert({reinterpret_cast<const idlekv::byte*>(bob.data()), bob.size()}, nullptr).s,
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
    ZSet                               zset(&mr);

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
    ZSet                               zset(&mr);

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
    ZSet                               zset(&mr);

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

} // namespace
