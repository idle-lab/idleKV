#include "db/storage/art/art.h"

#include <algorithm>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory_resource>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace {

using idlekv::Art;
using idlekv::ArtKey;
using idlekv::InsertMode;
using InsertResutl = Art<std::string>::InsertResutl;

auto InsertValue(Art<std::string>& art, std::string_view key, std::string value,
                 InsertMode mode = InsertMode::InsertOnly) -> InsertResutl {
    std::string owned_key(key);
    auto        art_key = ArtKey::BuildFromString(std::string_view(owned_key));
    return art.Insert(art_key, std::move(value), mode);
}

auto LookupValue(Art<std::string>& art, std::string_view key) -> std::optional<std::string> {
    std::string owned_key(key);
    auto        art_key = ArtKey::BuildFromString(std::string_view(owned_key));
    auto*       value   = art.Lookup(art_key);
    if (value == nullptr) {
        return std::nullopt;
    }
    return *value;
}

auto EraseValue(Art<std::string>& art, std::string_view key) -> size_t {
    std::string owned_key(key);
    auto        art_key = ArtKey::BuildFromString(std::string_view(owned_key));
    return art.Erase(art_key);
}

auto MakeOneByteKeys(int count, char first = 'A') -> std::vector<std::string> {
    std::vector<std::string> keys;
    keys.reserve(count);
    for (int i = 0; i < count; ++i) {
        keys.push_back(std::string(1, static_cast<char>(first + i)));
    }
    return keys;
}

auto EncodeBase62(size_t value, size_t min_width = 1) -> std::string {
    static constexpr std::string_view kAlphabet =
        "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    std::string encoded;
    do {
        encoded.push_back(kAlphabet[value % kAlphabet.size()]);
        value /= kAlphabet.size();
    } while (value != 0);

    while (encoded.size() < min_width) {
        encoded.push_back(kAlphabet[0]);
    }
    std::reverse(encoded.begin(), encoded.end());
    return encoded;
}

auto MakeSharedPrefixKeys(std::string_view prefix, size_t count) -> std::vector<std::string> {
    std::vector<std::string> keys;
    keys.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        keys.push_back(std::string(prefix) + EncodeBase62(i, 4));
    }
    return keys;
}

auto MakeWideFanoutKeys(size_t count) -> std::vector<std::string> {
    static constexpr std::string_view kAlphabet =
        "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    std::vector<std::string> keys;
    keys.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        std::string key;
        key.push_back(kAlphabet[i % kAlphabet.size()]);
        key.append(EncodeBase62(i / kAlphabet.size(), 3));
        keys.push_back(std::move(key));
    }
    return keys;
}

auto BytesToString(const std::vector<idlekv::byte>& key) -> std::string {
    return {reinterpret_cast<const char*>(key.data()), key.size()};
}

TEST(ArtTest, SingleLeafInsertLookupAndErase) {
    std::pmr::monotonic_buffer_resource mr;
    Art<std::string>                    art(&mr);

    EXPECT_EQ(InsertValue(art, "a", "v1"), InsertResutl::OK);
    EXPECT_EQ(LookupValue(art, "a"), std::optional<std::string>("v1"));
    EXPECT_EQ(LookupValue(art, "missing"), std::nullopt);

    EXPECT_EQ(EraseValue(art, "a"), 1U);
    EXPECT_EQ(EraseValue(art, "a"), 0U);
    EXPECT_EQ(LookupValue(art, "a"), std::nullopt);
}

TEST(ArtTest, DuplicateInsertAndUpsertFollowExactMatchBranch) {
    std::pmr::monotonic_buffer_resource mr;
    Art<std::string>                    art(&mr);

    EXPECT_EQ(InsertValue(art, "dup", "v1"), InsertResutl::OK);
    EXPECT_EQ(InsertValue(art, "dup", "v2"), InsertResutl::DuplicateKey);
    EXPECT_EQ(InsertValue(art, "dup", "v3", InsertMode::Upsert), InsertResutl::UpsertValue);
    EXPECT_EQ(LookupValue(art, "dup"), std::optional<std::string>("v3"));
}

TEST(ArtTest, PrefixSplitHandlesShorterAndLongerKeys) {
    std::pmr::monotonic_buffer_resource mr;
    Art<std::string>                    art(&mr);

    EXPECT_EQ(InsertValue(art, "a", "short"), InsertResutl::OK);
    EXPECT_EQ(InsertValue(art, "ab", "long"), InsertResutl::OK);
    EXPECT_EQ(InsertValue(art, "ac", "peer"), InsertResutl::OK);

    EXPECT_EQ(LookupValue(art, "a"), std::optional<std::string>("short"));
    EXPECT_EQ(LookupValue(art, "ab"), std::optional<std::string>("long"));
    EXPECT_EQ(LookupValue(art, "ac"), std::optional<std::string>("peer"));
    EXPECT_EQ(LookupValue(art, "ad"), std::nullopt);
}

TEST(ArtTest, DeepPrefixSplitRewritesRemainingSuffixCorrectly) {
    std::pmr::monotonic_buffer_resource mr;
    Art<std::string>                    art(&mr);

    ASSERT_EQ(InsertValue(art, "prefix-abcdef", "v1"), InsertResutl::OK);
    ASSERT_EQ(InsertValue(art, "prefix-abcxyz", "v2"), InsertResutl::OK);
    ASSERT_EQ(InsertValue(art, "prefix-abcuvw", "v3"), InsertResutl::OK);

    EXPECT_EQ(LookupValue(art, "prefix-abcdef"), std::optional<std::string>("v1"));
    EXPECT_EQ(LookupValue(art, "prefix-abcxyz"), std::optional<std::string>("v2"));
    EXPECT_EQ(LookupValue(art, "prefix-abcuvw"), std::optional<std::string>("v3"));
    EXPECT_EQ(LookupValue(art, "prefix-abc000"), std::nullopt);
}

TEST(ArtTest, DeleteCompressesSingleRemainingChildPath) {
    std::pmr::monotonic_buffer_resource mr;
    Art<std::string>                    art(&mr);

    ASSERT_EQ(InsertValue(art, "ab", "vb"), InsertResutl::OK);
    ASSERT_EQ(InsertValue(art, "ac", "vc"), InsertResutl::OK);

    EXPECT_EQ(EraseValue(art, "ab"), 1U);
    EXPECT_EQ(LookupValue(art, "ab"), std::nullopt);
    EXPECT_EQ(LookupValue(art, "ac"), std::optional<std::string>("vc"));

    EXPECT_EQ(EraseValue(art, "ac"), 1U);
    EXPECT_EQ(LookupValue(art, "ac"), std::nullopt);
}

TEST(ArtTest, LookupAndEraseMissWhenSearchStopsAtInnerNode) {
    std::pmr::monotonic_buffer_resource mr;
    Art<std::string>                    art(&mr);

    ASSERT_EQ(InsertValue(art, "ab", "vb"), InsertResutl::OK);
    ASSERT_EQ(InsertValue(art, "ac", "vc"), InsertResutl::OK);

    EXPECT_EQ(LookupValue(art, "a"), std::nullopt);
    EXPECT_EQ(EraseValue(art, "a"), 0U);
    EXPECT_EQ(LookupValue(art, "ab"), std::optional<std::string>("vb"));
    EXPECT_EQ(LookupValue(art, "ac"), std::optional<std::string>("vc"));
}

TEST(ArtTest, InsertingFiveDistinctKeysExercisesNode4ToNode16Growth) {
    std::pmr::monotonic_buffer_resource mr;
    Art<std::string>                    art(&mr);
    const auto                          keys = MakeOneByteKeys(5, 'A');

    for (int i = 0; i < 5; ++i) {
        EXPECT_EQ(InsertValue(art, keys[i], std::to_string(i)), InsertResutl::OK);
    }

    for (int i = 0; i < 5; ++i) {
        EXPECT_EQ(LookupValue(art, keys[i]), std::optional<std::string>(std::to_string(i)));
    }

    EXPECT_EQ(EraseValue(art, keys[0]), 1U);
    EXPECT_EQ(EraseValue(art, keys[1]), 1U);
    EXPECT_EQ(LookupValue(art, keys[0]), std::nullopt);
    EXPECT_EQ(LookupValue(art, keys[1]), std::nullopt);
    for (int i = 2; i < 5; ++i) {
        EXPECT_EQ(LookupValue(art, keys[i]), std::optional<std::string>(std::to_string(i)));
    }
}

TEST(ArtTest, InsertingSeventeenDistinctKeysExercisesNode16ToNode48Growth) {
    std::pmr::monotonic_buffer_resource mr;
    Art<std::string>                    art(&mr);
    const auto                          keys = MakeOneByteKeys(17, 'A');

    for (int i = 0; i < 17; ++i) {
        ASSERT_EQ(InsertValue(art, keys[i], std::to_string(i)), InsertResutl::OK);
    }

    for (int i = 0; i < 17; ++i) {
        EXPECT_EQ(LookupValue(art, keys[i]), std::optional<std::string>(std::to_string(i)));
    }

    EXPECT_EQ(EraseValue(art, keys[0]), 1U);
    EXPECT_EQ(EraseValue(art, keys[1]), 1U);
    EXPECT_EQ(LookupValue(art, keys[0]), std::nullopt);
    EXPECT_EQ(LookupValue(art, keys[1]), std::nullopt);
    for (int i = 2; i < 17; ++i) {
        EXPECT_EQ(LookupValue(art, keys[i]), std::optional<std::string>(std::to_string(i)));
    }
}

TEST(ArtTest, InsertingFortyNineDistinctKeysExercisesNode48ToNode256Growth) {
    std::pmr::monotonic_buffer_resource mr;
    Art<std::string>                    art(&mr);
    const auto                          keys = MakeOneByteKeys(49, 'A');

    for (int i = 0; i < 49; ++i) {
        ASSERT_EQ(InsertValue(art, keys[i], std::to_string(i)), InsertResutl::OK);
    }

    for (int i = 0; i < 49; ++i) {
        EXPECT_EQ(LookupValue(art, keys[i]), std::optional<std::string>(std::to_string(i)));
    }

    EXPECT_EQ(EraseValue(art, keys[0]), 1U);
    EXPECT_EQ(EraseValue(art, keys[1]), 1U);
    EXPECT_EQ(LookupValue(art, keys[0]), std::nullopt);
    EXPECT_EQ(LookupValue(art, keys[1]), std::nullopt);
    for (int i = 2; i < 49; ++i) {
        EXPECT_EQ(LookupValue(art, keys[i]), std::optional<std::string>(std::to_string(i)));
    }
}

TEST(ArtTest, LargeSharedPrefixDatasetSupportsBulkInsertLookupDeleteAndReinsert) {
    std::pmr::monotonic_buffer_resource mr;
    Art<std::string>                    art(&mr);
    auto                                user_keys  = MakeSharedPrefixKeys("user:", 256);
    auto                                order_keys = MakeSharedPrefixKeys("order:", 256);

    for (size_t i = 0; i < user_keys.size(); ++i) {
        ASSERT_EQ(InsertValue(art, user_keys[i], "u-" + std::to_string(i)), InsertResutl::OK);
        ASSERT_EQ(InsertValue(art, order_keys[i], "o-" + std::to_string(i)), InsertResutl::OK);
    }

    for (size_t i = 0; i < user_keys.size(); ++i) {
        EXPECT_EQ(LookupValue(art, user_keys[i]),
                  std::optional<std::string>("u-" + std::to_string(i)));
        EXPECT_EQ(LookupValue(art, order_keys[i]),
                  std::optional<std::string>("o-" + std::to_string(i)));
    }

    EXPECT_EQ(LookupValue(art, "user:zzzz"), std::nullopt);
    EXPECT_EQ(LookupValue(art, "payment:0000"), std::nullopt);

    for (size_t i = 0; i < user_keys.size(); i += 3) {
        EXPECT_EQ(EraseValue(art, user_keys[i]), 1U);
        EXPECT_EQ(EraseValue(art, order_keys[i]), 1U);
        EXPECT_EQ(EraseValue(art, user_keys[i]), 0U);
        EXPECT_EQ(EraseValue(art, order_keys[i]), 0U);
    }

    for (size_t i = 0; i < user_keys.size(); ++i) {
        if (i % 3 == 0) {
            EXPECT_EQ(LookupValue(art, user_keys[i]), std::nullopt);
            EXPECT_EQ(LookupValue(art, order_keys[i]), std::nullopt);
        } else {
            EXPECT_EQ(LookupValue(art, user_keys[i]),
                      std::optional<std::string>("u-" + std::to_string(i)));
            EXPECT_EQ(LookupValue(art, order_keys[i]),
                      std::optional<std::string>("o-" + std::to_string(i)));
        }
    }

    for (size_t i = 0; i < user_keys.size(); i += 3) {
        EXPECT_EQ(InsertValue(art, user_keys[i], "u2-" + std::to_string(i)), InsertResutl::OK);
        EXPECT_EQ(InsertValue(art, order_keys[i], "o2-" + std::to_string(i)), InsertResutl::OK);
    }

    for (size_t i = 0; i < user_keys.size(); ++i) {
        const auto expected_user =
            (i % 3 == 0) ? "u2-" + std::to_string(i) : "u-" + std::to_string(i);
        const auto expected_order =
            (i % 3 == 0) ? "o2-" + std::to_string(i) : "o-" + std::to_string(i);

        EXPECT_EQ(LookupValue(art, user_keys[i]), std::optional<std::string>(expected_user));
        EXPECT_EQ(LookupValue(art, order_keys[i]), std::optional<std::string>(expected_order));
    }
}

TEST(ArtTest, LargeWideFanoutDatasetSurvivesMultiStageShrinkAndCompression) {
    std::pmr::monotonic_buffer_resource mr;
    Art<std::string>                    art(&mr);
    const auto                          keys = MakeWideFanoutKeys(200);

    for (size_t i = 0; i < keys.size(); ++i) {
        ASSERT_EQ(InsertValue(art, keys[i], "v-" + std::to_string(i)), InsertResutl::OK);
    }

    EXPECT_EQ(LookupValue(art, "z999"), std::nullopt);
    EXPECT_EQ(EraseValue(art, "z999"), 0U);

    for (size_t i = 0; i < 153; ++i) {
        EXPECT_EQ(EraseValue(art, keys[i]), 1U);
    }
    for (size_t i = 0; i < 153; ++i) {
        EXPECT_EQ(LookupValue(art, keys[i]), std::nullopt);
    }
    for (size_t i = 153; i < keys.size(); ++i) {
        EXPECT_EQ(LookupValue(art, keys[i]), std::optional<std::string>("v-" + std::to_string(i)));
    }

    for (size_t i = 153; i < 185; ++i) {
        EXPECT_EQ(EraseValue(art, keys[i]), 1U);
    }
    for (size_t i = 185; i < keys.size(); ++i) {
        EXPECT_EQ(LookupValue(art, keys[i]), std::optional<std::string>("v-" + std::to_string(i)));
    }

    for (size_t i = 185; i < 197; ++i) {
        EXPECT_EQ(EraseValue(art, keys[i]), 1U);
    }
    for (size_t i = 197; i < keys.size(); ++i) {
        EXPECT_EQ(LookupValue(art, keys[i]), std::optional<std::string>("v-" + std::to_string(i)));
    }

    EXPECT_EQ(EraseValue(art, keys[197]), 1U);
    EXPECT_EQ(EraseValue(art, keys[198]), 1U);
    EXPECT_EQ(LookupValue(art, keys[199]), std::optional<std::string>("v-199"));
    EXPECT_EQ(EraseValue(art, keys[199]), 1U);
    EXPECT_EQ(LookupValue(art, keys[199]), std::nullopt);
}

TEST(ArtTest, DeterministicMixedLargeWorkloadMatchesReferenceMap) {
    std::pmr::monotonic_buffer_resource mr;
    Art<std::string>                    art(&mr);
    std::vector<std::string>            keys      = MakeSharedPrefixKeys("tenant:", 512);
    auto                                wide_keys = MakeWideFanoutKeys(512);
    keys.insert(keys.end(), wide_keys.begin(), wide_keys.end());
    keys.push_back("a");
    keys.push_back("ab");
    keys.push_back("abc");
    keys.push_back("abd");
    keys.push_back("b");
    keys.push_back("ba");
    keys.push_back("bb");

    std::unordered_map<std::string, std::string> expected;
    std::mt19937                                 rng(0xC0FFEE);
    std::uniform_int_distribution<size_t>        key_dist(0, keys.size() - 1);
    std::uniform_int_distribution<int>           op_dist(0, 99);

    for (size_t step = 0; step < 20000; ++step) {
        const std::string& key   = keys[key_dist(rng)];
        const int          op    = op_dist(rng);
        const std::string  value = "value-" + std::to_string(step);

        if (op < 35) {
            const bool exists = expected.contains(key);
            EXPECT_EQ(InsertValue(art, key, value),
                      exists ? InsertResutl::DuplicateKey : InsertResutl::OK);
            if (!exists) {
                expected.emplace(key, value);
            }
        } else if (op < 60) {
            const bool exists = expected.contains(key);
            EXPECT_EQ(InsertValue(art, key, value, InsertMode::Upsert),
                      exists ? InsertResutl::UpsertValue : InsertResutl::OK);
            expected[key] = value;
        } else if (op < 80) {
            auto it = expected.find(key);
            if (it == expected.end()) {
                EXPECT_EQ(LookupValue(art, key), std::nullopt);
            } else {
                EXPECT_EQ(LookupValue(art, key), std::optional<std::string>(it->second));
            }
        } else {
            const bool exists = expected.contains(key);
            EXPECT_EQ(EraseValue(art, key), exists ? 1U : 0U);
            if (exists) {
                expected.erase(key);
            }
        }

        if (step % 512 == 0) {
            for (const auto& verify_key : keys) {
                auto it = expected.find(verify_key);
                if (it == expected.end()) {
                    EXPECT_EQ(LookupValue(art, verify_key), std::nullopt);
                } else {
                    EXPECT_EQ(LookupValue(art, verify_key), std::optional<std::string>(it->second));
                }
            }
        }
    }

    for (const auto& key : keys) {
        auto it = expected.find(key);
        if (it == expected.end()) {
            EXPECT_EQ(LookupValue(art, key), std::nullopt);
        } else {
            EXPECT_EQ(LookupValue(art, key), std::optional<std::string>(it->second));
        }
    }
}

TEST(ArtTest, SupportsKeysWithPrefixesLongerThanSingleNodeCapacity) {
    std::pmr::monotonic_buffer_resource mr;
    Art<std::string>                    art(&mr);

    EXPECT_EQ(InsertValue(art, "tenant:region:shard:0001", "v1"), InsertResutl::OK);
    EXPECT_EQ(InsertValue(art, "tenant:region:shard:0002", "v2"), InsertResutl::OK);
    EXPECT_EQ(InsertValue(art, "tenant:region:shard:0003", "v3"), InsertResutl::OK);

    EXPECT_EQ(LookupValue(art, "tenant:region:shard:0001"), std::optional<std::string>("v1"));
    EXPECT_EQ(LookupValue(art, "tenant:region:shard:0002"), std::optional<std::string>("v2"));
    EXPECT_EQ(LookupValue(art, "tenant:region:shard:0003"), std::optional<std::string>("v3"));

    EXPECT_EQ(EraseValue(art, "tenant:region:shard:0002"), 1U);
    EXPECT_EQ(LookupValue(art, "tenant:region:shard:0002"), std::nullopt);
    EXPECT_EQ(LookupValue(art, "tenant:region:shard:0001"), std::optional<std::string>("v1"));
    EXPECT_EQ(LookupValue(art, "tenant:region:shard:0003"), std::optional<std::string>("v3"));
}

TEST(ArtTest, RangeMinAndMaxFollowLexicographicOrder) {
    std::pmr::monotonic_buffer_resource mr;
    Art<std::string>                    art(&mr);

    ASSERT_EQ(InsertValue(art, "key:01", "value-01"), InsertResutl::OK);
    ASSERT_EQ(InsertValue(art, "key:03", "value-03"), InsertResutl::OK);
    ASSERT_EQ(InsertValue(art, "key:02", "value-02"), InsertResutl::OK);
    ASSERT_EQ(InsertValue(art, "key:05", "value-05"), InsertResutl::OK);
    ASSERT_EQ(InsertValue(art, "key:04", "value-04"), InsertResutl::OK);

    ASSERT_NE(art.Min(), nullptr);
    ASSERT_NE(art.Max(), nullptr);
    EXPECT_EQ(*art.Min(), "value-01");
    EXPECT_EQ(*art.Max(), "value-05");

    const auto range = art.Range(ArtKey::BuildFromString("key:02"), ArtKey::BuildFromString("key:04"));
    ASSERT_EQ(range.size(), 3U);
    EXPECT_EQ(BytesToString(range[0].key), "key:02");
    EXPECT_EQ(*range[0].value, "value-02");
    EXPECT_EQ(BytesToString(range[1].key), "key:03");
    EXPECT_EQ(*range[1].value, "value-03");
    EXPECT_EQ(BytesToString(range[2].key), "key:04");
    EXPECT_EQ(*range[2].value, "value-04");
}

} // namespace
