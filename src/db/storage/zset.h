#pragma once

#include "absl/container/flat_hash_map.h"
#include "common/logger.h"
#include "db/storage/art/art.h"

#include <bit>
#include <cstdint>
#include <cstring>
#include <memory_resource>
#include <span>
#include <string>
#include <string_view>
#include <variant>
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
        auto member_it = member_to_score_.find(std::string(member));
        if (member_it == member_to_score_.end()) {
            InsertToIndex(score, member);
            member_to_score_.emplace(member, score);
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
        int64_t index = 0;
        data_.Iterate([&](std::span<const byte> key, std::nullptr_t&) -> bool {
            const double      score  = DecodeScore(key);
            const std::string member = DecodeMember(key);
            auto              it     = member_to_score_.find(member);
            // Treat the member map as the source of truth so outdated ART entries
            // from prior score updates do not leak into user-visible ranges.
            if (it == member_to_score_.end() || it->second != score) {
                return true;
            }

            if (index < start) {
                ++index;
                return true;
            }
            if (index > stop) {
                return false;
            }

            out.push_back(MemberScore{member, score});
            ++index;
            return true;
        });
        return out;
    }

    auto Size() const -> size_t { return member_to_score_.size(); }

private:
    using ScoreIndex = Art<std::monostate>;

    static constexpr size_t kScoreBytes = sizeof(uint64_t);

    static auto NormalizeIndex(int64_t index, int64_t size) -> int64_t {
        return index < 0 ? index + size : index;
    }

    static auto EncodeKey(double score, std::string_view member) -> std::string {
        ArtKey      encoded_score(score);
        std::string key(kScoreBytes + member.size(), '\0');
        std::memcpy(key.data(), encoded_score.Data(), kScoreBytes);
        if (!member.empty()) {
            std::memcpy(key.data() + kScoreBytes, member.data(), member.size());
        }
        return key;
    }

    static auto DecodeScore(std::span<const byte> key) -> double {
        CHECK_GE(key.size(), kScoreBytes);

        uint64_t ordered = 0;
        for (size_t i = 0; i < kScoreBytes; ++i) {
            ordered = (ordered << 8U) | static_cast<uint64_t>(key[i]);
        }

        constexpr uint64_t kSignMask = 1ULL << 63U;
        const uint64_t bits =
            (ordered & kSignMask) != 0 ? (ordered ^ kSignMask) : ~ordered;
        return std::bit_cast<double>(bits);
    }

    static auto DecodeMember(std::span<const byte> key) -> std::string {
        CHECK_GE(key.size(), kScoreBytes);
        return {reinterpret_cast<const char*>(key.data() + kScoreBytes),
                key.size() - kScoreBytes};
    }

    static auto ToArtKey(const std::string& encoded_key) -> ArtKey {
        return {reinterpret_cast<const byte*>(encoded_key.data()), encoded_key.size()};
    }

    auto InsertToIndex(double score, std::string_view member) -> void {
        const std::string encoded_key = EncodeKey(score, member);
        auto              insert_res  = data_.Insert(ToArtKey(encoded_key), nullptr);
        (void)insert_res;
        CHECK_EQ(insert_res.s, ScoreIndex::InsertResutl::OK);
    }

    auto RemoveFromIndex(double score, std::string_view member) -> void {
        const std::string encoded_key = EncodeKey(score, member);
        CHECK_EQ(data_.Erase(ToArtKey(encoded_key)), size_t{1});
    }

    ScoreIndex                            data_;
    absl::flat_hash_map<std::string, double> member_to_score_;
};

} // namespace idlekv
