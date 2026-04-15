#include "db/storage/art/art.h"

#include <algorithm>
#include <gtest/gtest.h>
#include <memory_resource>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace idlekv {

template <class ValueType>
struct ArtTestAccess {
    static auto Root(Art<ValueType>& art) -> Node* { return art.root_; }
};

} // namespace idlekv

namespace {

using idlekv::Art;
using idlekv::ArtKey;
using idlekv::ArtKeyCodec;
using idlekv::InnerNode;
using idlekv::InsertMode;
using idlekv::Node;
using idlekv::Node4;
using idlekv::NodeType;
using InsertResutl = Art<std::string>::InsertResutl;

auto InsertValue(Art<std::string>& art, std::string_view key, std::string value,
                 InsertMode mode = InsertMode::InsertOnly) -> InsertResutl {
    std::string buffer;
    auto        art_key = ArtKeyCodec::Encode(key, buffer);
    return art.Insert(art_key, std::move(value), mode);
}

auto LookupValue(Art<std::string>& art, std::string_view key) -> std::optional<std::string> {
    std::string buffer;
    auto        art_key = ArtKeyCodec::Encode(key, buffer);
    auto*       value   = art.Lookup(art_key);
    if (value == nullptr) {
        return std::nullopt;
    }
    return *value;
}

auto EraseValue(Art<std::string>& art, std::string_view key) -> size_t {
    std::string buffer;
    auto        art_key = ArtKeyCodec::Encode(key, buffer);
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
    return ArtKeyCodec::DecodeString(std::span<const idlekv::byte>(key.data(), key.size()));
}

auto BytesToString(std::span<const idlekv::byte> key) -> std::string {
    return ArtKeyCodec::DecodeString(key);
}

auto CollectKeysByCursor(Art<std::string>& art) -> std::vector<std::string> {
    std::vector<std::string> keys;
    keys.reserve(art.Size());
    for (auto it = art.Begin(); it.Valid(); it.Next()) {
        keys.push_back(BytesToString(it.Key()));
    }
    return keys;
}

auto CollectKeysByRank(Art<std::string>& art) -> std::vector<std::string> {
    std::vector<std::string> keys;
    keys.reserve(art.Size());
    if (art.Empty()) {
        return keys;
    }

    art.IterateByRank(0, static_cast<uint32_t>(art.Size() - 1), [&](auto& it) -> bool {
        keys.push_back(BytesToString(it.Key()));
        return true;
    });
    return keys;
}

auto VerifyValueCounts(Node* node) -> uint32_t {
    if (node == nullptr) {
        return 0;
    }
    if (node->Type() == NodeType::Leaf) {
        return 1;
    }

    auto*    inner      = static_cast<InnerNode*>(node);
    uint32_t sum        = 0;
    auto     walk_child = [&](Node* child) { sum += VerifyValueCounts(child); };

    switch (node->Type()) {
    case NodeType::Node4: {
        auto* n4 = static_cast<idlekv::Node4*>(node);
        for (int i = 0; i < n4->size_; ++i) {
            walk_child(n4->next_[i]);
        }
        break;
    }
    case NodeType::Node16: {
        auto* n16 = static_cast<idlekv::Node16*>(node);
        for (int i = 0; i < n16->size_; ++i) {
            walk_child(n16->next_[i]);
        }
        break;
    }
    case NodeType::Node48: {
        auto* n48 = static_cast<idlekv::Node48*>(node);
        for (int i = 0; i <= 0xFF; ++i) {
            const auto slot = n48->keys_[i];
            if (slot != idlekv::Node48::Nothing) {
                walk_child(n48->next_[slot - 1]);
            }
        }
        break;
    }
    case NodeType::Node256: {
        auto* n256 = static_cast<idlekv::Node256*>(node);
        for (auto* child : n256->next_) {
            if (child != nullptr) {
                walk_child(child);
            }
        }
        break;
    }
    case NodeType::Leaf:
        UNREACHABLE();
    default:
        CHECK(false) << "unexpected node type";
        UNREACHABLE();
    }

    EXPECT_EQ(inner->value_count_, sum);
    return sum;
}

auto AssertValueCountInvariant(Art<std::string>& art) -> void {
    Node* root = idlekv::ArtTestAccess<std::string>::Root(art);
    if (root == nullptr) {
        EXPECT_EQ(art.Size(), 0U);
        return;
    }

    if (root->Type() == NodeType::Leaf) {
        EXPECT_EQ(art.Size(), 1U);
        return;
    }

    EXPECT_EQ(VerifyValueCounts(root), art.Size());
}

TEST(NodeTest, PrefixStorageSwitchesBetweenInlineAndHeap) {
    std::pmr::monotonic_buffer_resource mr;
    Node4                               node;

    constexpr std::string_view kInlinePrefix = "12345678";
    constexpr std::string_view kHeapPrefix   = "123456789";

    node.SetPrefix(reinterpret_cast<const idlekv::byte*>(kInlinePrefix.data()),
                   kInlinePrefix.size(), &mr);
    ASSERT_EQ(node.PrefixLen(), kInlinePrefix.size());
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(node.PrefixData()), node.PrefixLen()),
              kInlinePrefix);

    node.SetPrefix(reinterpret_cast<const idlekv::byte*>(kHeapPrefix.data()), kHeapPrefix.size(),
                   &mr);
    ASSERT_EQ(node.PrefixLen(), kHeapPrefix.size());
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(node.PrefixData()), node.PrefixLen()),
              kHeapPrefix);

    node.SetPrefix(reinterpret_cast<const idlekv::byte*>(kInlinePrefix.data()),
                   kInlinePrefix.size(), &mr);
    ASSERT_EQ(node.PrefixLen(), kInlinePrefix.size());
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(node.PrefixData()), node.PrefixLen()),
              kInlinePrefix);
}

TEST(NodeTest, MovePrefixFromTransfersHeapBackedOwnership) {
    std::pmr::monotonic_buffer_resource mr;
    Node4                               src;
    Node4                               dst;

    constexpr std::string_view kHeapPrefix = "tenant:node";
    src.SetPrefix(reinterpret_cast<const idlekv::byte*>(kHeapPrefix.data()), kHeapPrefix.size(),
                  &mr);

    dst.MovePrefixFrom(src, &mr);

    EXPECT_EQ(src.PrefixLen(), 0U);
    ASSERT_EQ(dst.PrefixLen(), kHeapPrefix.size());
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(dst.PrefixData()), dst.PrefixLen()),
              kHeapPrefix);
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

TEST(ArtTest, PrefixSplitHandlesLongerKeyBeforeShorterPrefix) {
    std::pmr::monotonic_buffer_resource mr;
    Art<std::string>                    art(&mr);

    EXPECT_EQ(InsertValue(art, "ab", "long"), InsertResutl::OK);
    EXPECT_EQ(InsertValue(art, "a", "short"), InsertResutl::OK);
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

TEST(ArtTest, ShorterKeyThanStoredPrefixReturnsMissWithoutOverread) {
    std::pmr::monotonic_buffer_resource mr;
    Art<std::string>                    art(&mr);

    ASSERT_EQ(InsertValue(art, "tenant:region:shard:0001", "v1"), InsertResutl::OK);

    EXPECT_EQ(LookupValue(art, "tenant:region"), std::nullopt);
    EXPECT_EQ(EraseValue(art, "tenant:region"), 0U);
    EXPECT_EQ(LookupValue(art, "tenant:region:shard:0001"), std::optional<std::string>("v1"));
}

TEST(ArtTest, StringKeysEscapeEmbeddedZeroAndEscapeBytes) {
    std::pmr::monotonic_buffer_resource mr;
    Art<std::string>                    art(&mr);

    const std::string plain  = "a";
    const std::string zero   = std::string("a\0b", 3);
    const std::string escape = std::string("a\1b", 3);
    const std::string tail   = "ab";

    EXPECT_EQ(InsertValue(art, plain, "plain"), InsertResutl::OK);
    EXPECT_EQ(InsertValue(art, zero, "zero"), InsertResutl::OK);
    EXPECT_EQ(InsertValue(art, escape, "escape"), InsertResutl::OK);
    EXPECT_EQ(InsertValue(art, tail, "tail"), InsertResutl::OK);

    EXPECT_EQ(LookupValue(art, plain), std::optional<std::string>("plain"));
    EXPECT_EQ(LookupValue(art, zero), std::optional<std::string>("zero"));
    EXPECT_EQ(LookupValue(art, escape), std::optional<std::string>("escape"));
    EXPECT_EQ(LookupValue(art, tail), std::optional<std::string>("tail"));

    EXPECT_EQ(EraseValue(art, zero), 1U);
    EXPECT_EQ(LookupValue(art, zero), std::nullopt);
    EXPECT_EQ(LookupValue(art, plain), std::optional<std::string>("plain"));
    EXPECT_EQ(LookupValue(art, escape), std::optional<std::string>("escape"));
    EXPECT_EQ(LookupValue(art, tail), std::optional<std::string>("tail"));
}

TEST(ArtTest, EncodePiecesMatchesManualCompositeEncoding) {
    const uint64_t    tenant = 42;
    const double      score  = -1.5;
    const std::string member = std::string("alpha\0\1omega", 12);

    ArtKeyCodec::FixedBuffer tenant_buffer{};
    ArtKeyCodec::FixedBuffer score_buffer{};

    ArtKey      encoded_tenant = ArtKeyCodec::Encode(tenant, tenant_buffer);
    ArtKey      encoded_score  = ArtKeyCodec::Encode(score, score_buffer);
    std::string expected;
    expected.reserve(encoded_tenant.Len() + encoded_score.Len() + ArtKeyCodec::EncodedSize(member));
    expected.append(reinterpret_cast<const char*>(encoded_tenant.Data()), encoded_tenant.Len());
    expected.append(reinterpret_cast<const char*>(encoded_score.Data()), encoded_score.Len());
    ArtKeyCodec::AppendEncodedString(expected, member);

    std::string actual;
    ArtKey encoded = ArtKeyCodec::EncodePieces(actual, tenant, score, std::string_view(member));

    EXPECT_EQ(actual, expected);
    EXPECT_EQ(encoded.Len(), actual.size());
    EXPECT_EQ(reinterpret_cast<const void*>(encoded.Data()),
              static_cast<const void*>(actual.data()));
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

TEST(ArtTest, CursorIteratesKeysInLexicographicOrder) {
    std::pmr::monotonic_buffer_resource mr;
    Art<std::string>                    art(&mr);

    ASSERT_EQ(InsertValue(art, "key:03", "value-03"), InsertResutl::OK);
    ASSERT_EQ(InsertValue(art, "key:01", "value-01"), InsertResutl::OK);
    ASSERT_EQ(InsertValue(art, "key:05", "value-05"), InsertResutl::OK);
    ASSERT_EQ(InsertValue(art, "key:02", "value-02"), InsertResutl::OK);
    ASSERT_EQ(InsertValue(art, "key:04", "value-04"), InsertResutl::OK);

    std::vector<std::string> keys;
    std::vector<std::string> values;
    for (auto it = art.Begin(); it.Valid(); it.Next()) {
        keys.push_back(BytesToString(it.Key()));
        values.push_back(it.Value());
    }

    EXPECT_EQ(keys, (std::vector<std::string>{"key:01", "key:02", "key:03", "key:04", "key:05"}));
    EXPECT_EQ(values, (std::vector<std::string>{"value-01", "value-02", "value-03", "value-04",
                                                "value-05"}));
}

TEST(ArtTest, LowerBoundFindsExactMatchAndNextSuccessor) {
    std::pmr::monotonic_buffer_resource mr;
    Art<std::string>                    art(&mr);

    ASSERT_EQ(InsertValue(art, "key:01", "value-01"), InsertResutl::OK);
    ASSERT_EQ(InsertValue(art, "key:03", "value-03"), InsertResutl::OK);
    ASSERT_EQ(InsertValue(art, "key:05", "value-05"), InsertResutl::OK);

    auto ExpectLowerBound = [&](std::string_view                query,
                                std::optional<std::string_view> expected_key) {
        std::string buffer;
        auto        it = art.LowerBound(ArtKeyCodec::Encode(query, buffer));
        if (!expected_key.has_value()) {
            EXPECT_FALSE(it.Valid());
            return;
        }

        ASSERT_TRUE(it.Valid());
        EXPECT_EQ(BytesToString(it.Key()), expected_key.value());
    };

    ExpectLowerBound("key:00", "key:01");
    ExpectLowerBound("key:03", "key:03");
    ExpectLowerBound("key:04", "key:05");
    ExpectLowerBound("key:99", std::nullopt);
}

TEST(ArtTest, LowerBoundHandlesCompressedPrefixGap) {
    std::pmr::monotonic_buffer_resource mr;
    Art<std::string>                    art(&mr);

    ASSERT_EQ(InsertValue(art, "tenant:region:shard:0001", "v1"), InsertResutl::OK);
    ASSERT_EQ(InsertValue(art, "tenant:region:shard:0003", "v3"), InsertResutl::OK);
    ASSERT_EQ(InsertValue(art, "tenant:region:shard:0005", "v5"), InsertResutl::OK);

    std::string buffer;
    auto        it = art.LowerBound(ArtKeyCodec::Encode("tenant:region:shard:0002", buffer));
    ASSERT_TRUE(it.Valid());
    EXPECT_EQ(BytesToString(it.Key()), "tenant:region:shard:0003");

    it = art.LowerBound(ArtKeyCodec::Encode("tenant:region:shard:", buffer));
    ASSERT_TRUE(it.Valid());
    EXPECT_EQ(BytesToString(it.Key()), "tenant:region:shard:0001");
}

TEST(ArtTest, ValueCountsStayConsistentAcrossInsertEraseShrinkAndReinsert) {
    std::pmr::monotonic_buffer_resource mr;
    Art<std::string>                    art(&mr);

    auto keys = MakeSharedPrefixKeys("tenant:", 48);
    auto wide = MakeWideFanoutKeys(48);
    keys.insert(keys.end(), wide.begin(), wide.end());

    for (size_t i = 0; i < keys.size(); ++i) {
        ASSERT_EQ(InsertValue(art, keys[i], "value-" + std::to_string(i)), InsertResutl::OK);
        AssertValueCountInvariant(art);
    }

    for (size_t i = 0; i < keys.size(); i += 3) {
        EXPECT_EQ(EraseValue(art, keys[i]), 1U);
        AssertValueCountInvariant(art);
    }

    for (size_t i = 0; i < keys.size(); i += 3) {
        ASSERT_EQ(InsertValue(art, keys[i], "reinsert-" + std::to_string(i)), InsertResutl::OK);
        AssertValueCountInvariant(art);
    }
}

TEST(ArtTest, SeekByRankAndIterateByRankMatchLexicographicOrderAndBounds) {
    std::pmr::monotonic_buffer_resource mr;
    Art<std::string>                    art(&mr);

    std::vector<std::string> keys = {
        "key:03",
        "key:01",
        "key:05",
        "key:02",
        "key:04",
        "tenant:region:shard:0003",
        "tenant:region:shard:0001",
        "tenant:region:shard:0002",
    };

    for (size_t i = 0; i < keys.size(); ++i) {
        ASSERT_EQ(InsertValue(art, keys[i], "value-" + std::to_string(i)), InsertResutl::OK);
    }

    std::vector<std::string> expected = keys;
    std::sort(expected.begin(), expected.end());

    for (size_t rank = 0; rank < expected.size(); ++rank) {
        auto it = art.SeekByRank(static_cast<uint32_t>(rank));
        ASSERT_TRUE(it.Valid());
        EXPECT_EQ(BytesToString(it.Key()), expected[rank]);
    }

    EXPECT_FALSE(art.SeekByRank(static_cast<uint32_t>(expected.size())).Valid());
    EXPECT_EQ(CollectKeysByCursor(art), expected);
    EXPECT_EQ(CollectKeysByRank(art), expected);

    std::vector<std::string> slice;
    art.IterateByRank(2, 5, [&](auto& it) -> bool {
        slice.push_back(BytesToString(it.Key()));
        return true;
    });
    EXPECT_EQ(slice,
              (std::vector<std::string>{expected[2], expected[3], expected[4], expected[5]}));
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

    std::string min_buffer;
    std::string max_buffer;
    const auto  range = art.Range(ArtKeyCodec::Encode("key:02", min_buffer),
                                  ArtKeyCodec::Encode("key:04", max_buffer));
    ASSERT_EQ(range.size(), 3U);
    EXPECT_EQ(BytesToString(range[0].key), "key:02");
    EXPECT_EQ(*range[0].value, "value-02");
    EXPECT_EQ(BytesToString(range[1].key), "key:03");
    EXPECT_EQ(*range[1].value, "value-03");
    EXPECT_EQ(BytesToString(range[2].key), "key:04");
    EXPECT_EQ(*range[2].value, "value-04");
}

} // namespace
