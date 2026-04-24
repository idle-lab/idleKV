#pragma once

#include "absl/container/flat_hash_map.h"
#include "common/logger.h"
#include "db/storage/art/art.h"

#include <cstdint>
#include <cstring>
#include <memory_resource>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace idlekv {

class ZSet {
    struct Placeholder {
        auto operator=(const Placeholder&) -> Placeholder& = default;
    };

    using ScoreTree = Art<Placeholder>;

    using Hash      = absl::container_internal::FlatHashMapPolicy<std::string, double>::DefaultHash;
    using Eq        = absl::container_internal::FlatHashMapPolicy<std::string, double>::DefaultEq;
    using Allocator = std::pmr::polymorphic_allocator<std::pair<const std::string, double>>;
    using ScoreMap  = absl::flat_hash_map<std::string, double, Hash, Eq, Allocator>;

    static constexpr size_t kScoreBytes = ArtKeyCodec::kFixedBytes;

public:
    explicit ZSet(std::pmr::memory_resource* mr) : score_tree_(mr), score_map_(mr) {}

    auto Add(std::string_view member, double score) -> bool {
        auto [member_it, inserted] = score_map_.emplace(member, score);
        if (inserted) {
            InsertToIndex(score, member_it->first);
            return true;
        }

        if (member_it->second == score) {
            return false;
        }

        RemoveFromIndex(member_it->second, member_it->first);
        InsertToIndex(score, member);
        member_it->second = score;
        return false;
    }

    auto Rem(std::string_view member) -> bool {
        auto member_it = score_map_.find(std::string(member));
        if (member_it == score_map_.end()) {
            return false;
        }

        RemoveFromIndex(member_it->second, member_it->first);
        score_map_.erase(member_it);
        return true;
    }

    auto CountRange(int64_t start, int64_t stop) const -> size_t {
        auto spec = NormalizeRange(start, stop);
        return spec.has_value() ? spec->count : 0;
    }

    template <class Fn>
    auto IterateRange(int64_t start, int64_t stop, Fn&& fn) -> size_t {
        auto spec = NormalizeRange(start, stop);
        if (!spec.has_value()) {
            return 0;
        }

        size_t visited = 0;
        score_tree_.IterateByRank(spec->start, spec->stop, [&](auto& it) -> bool {
            const auto key = it.Key();
            ++visited;
            return fn(DecodeMember(key), DecodeScore(key));
        });
        return visited;
    }

    auto Size() const -> size_t { return score_map_.size(); }

private:
    struct RangeSpec {
        uint32_t start{0};
        uint32_t stop{0};
        size_t   count{0};
    };

    static auto NormalizeIndex(int64_t index, int64_t size) -> int64_t {
        return index < 0 ? index + size : index;
    }

    auto NormalizeRange(int64_t start, int64_t stop) const -> std::optional<RangeSpec> {
        const int64_t size = static_cast<int64_t>(score_map_.size());
        if (size == 0) {
            return std::nullopt;
        }

        start = NormalizeIndex(start, size);
        stop  = NormalizeIndex(stop, size);

        if (start < 0) {
            start = 0;
        }
        if (stop < 0 || start >= size) {
            return std::nullopt;
        }
        if (stop >= size) {
            stop = size - 1;
        }
        if (start > stop) {
            return std::nullopt;
        }

        return RangeSpec{
            .start = static_cast<uint32_t>(start),
            .stop  = static_cast<uint32_t>(stop),
            .count = static_cast<size_t>(stop - start + 1),
        };
    }

    static auto EncodeKey(double score, std::string_view member) -> std::string {
        std::string key;
        ArtKeyCodec::EncodePieces(key, score, member);
        return key;
    }

    static auto DecodeScore(std::span<const byte> key) -> double {
        CHECK_GE(key.size(), kScoreBytes);
        return ArtKeyCodec::DecodeDouble(key.first(kScoreBytes));
    }

    static auto DecodeMember(std::span<const byte> key) -> std::string {
        CHECK_GE(key.size(), kScoreBytes);
        return ArtKeyCodec::DecodeString(key.subspan(kScoreBytes));
    }

    auto InsertToIndex(double score, std::string_view member) -> void {
        [[maybe_unused]] auto insert_res =
            score_tree_.Insert(EncodeKey(score, member), Placeholder{});
        CHECK_EQ(insert_res.status, ScoreTree::InsertResutl::OK);
    }

    auto RemoveFromIndex(double score, std::string_view member) -> void {
        [[maybe_unused]] auto erased = score_tree_.Erase(EncodeKey(score, member));
        CHECK_EQ(erased, size_t{1});
    }

    ScoreTree score_tree_;
    ScoreMap  score_map_;
};

} // namespace idlekv
