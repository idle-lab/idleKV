#pragma once

#include "absl/container/flat_hash_map.h"
#include "common/logger.h"
#include "db/storage/art/art.h"

#include <cstdint>
#include <cstring>
#include <memory_resource>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace idlekv {

class ZSet {
public:
    struct MemberScore {
        std::string member;
        double      score{0};
    };

    explicit ZSet(std::pmr::memory_resource* mr) : data_(mr) {}

    auto Add(std::string_view member, double score) -> bool {
        auto [member_it, inserted] = member_to_score_.emplace(member, score);
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
        auto member_it = member_to_score_.find(std::string(member));
        if (member_it == member_to_score_.end()) {
            return false;
        }

        RemoveFromIndex(member_it->second, member_it->first);
        member_to_score_.erase(member_it);
        return true;
    }

    auto Range(int64_t start, int64_t stop) -> std::vector<MemberScore> {
        std::vector<MemberScore> out;
        const int64_t            size = static_cast<int64_t>(member_to_score_.size());
        if (size == 0) {
            return out;
        }

        start = NormalizeIndex(start, size);
        stop  = NormalizeIndex(stop, size);

        if (start < 0) {
            start = 0;
        }
        if (stop < 0 || start >= size) {
            return out;
        }
        if (stop >= size) {
            stop = size - 1;
        }
        if (start > stop) {
            return out;
        }

        out.reserve(static_cast<size_t>(stop - start + 1));
        data_.IterateByRank(static_cast<uint32_t>(start), static_cast<uint32_t>(stop),
                            [&](auto& it) -> bool {
                                const auto        key    = it.Key();
                                const double      score  = DecodeScore(key);
                                const std::string member = DecodeMember(key);
                                out.push_back(MemberScore{member, score});
                                return true;
                            });
        return out;
    }

    auto Size() const -> size_t { return member_to_score_.size(); }

private:
    struct Placeholder {
        auto operator=(const Placeholder&) -> Placeholder& = default;
    };

    using ScoreIndex = Art<Placeholder>;

    static constexpr size_t kScoreBytes = ArtKeyCodec::kFixedBytes;

    static auto NormalizeIndex(int64_t index, int64_t size) -> int64_t {
        return index < 0 ? index + size : index;
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
        [[maybe_unused]] auto insert_res = data_.Insert(EncodeKey(score, member), Placeholder{});
        CHECK_EQ(insert_res.status, ScoreIndex::InsertResutl::OK);
    }

    auto RemoveFromIndex(double score, std::string_view member) -> void {
        [[maybe_unused]] auto erased = data_.Erase(EncodeKey(score, member));
        CHECK_EQ(erased, size_t{1});
    }

    ScoreIndex                               data_;
    absl::flat_hash_map<std::string, double> member_to_score_;
};

} // namespace idlekv
