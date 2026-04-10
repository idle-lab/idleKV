#pragma once

#include "db/storage/art/node.h"

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <string_view>
namespace idlekv {

struct ArtKey {
public:
    ArtKey(const byte* data, size_t len) : external_data_(data), len_(len) {}

    static auto BuildFromString(std::string_view value) -> ArtKey {
        return {reinterpret_cast<const byte*>(value.data()), value.size()};
    }

    explicit ArtKey(uint64_t value) : len_(sizeof(uint64_t)) { StoreBigEndian(value); }

    explicit ArtKey(double value) : len_(sizeof(uint64_t)) {
        uint64_t bits = std::bit_cast<uint64_t>(value);
        if ((bits << 1U) == 0) {
            bits = 0;
        }

        constexpr uint64_t kSignMask    = 1ULL << 63U;
        const uint64_t     ordered_bits = (bits & kSignMask) != 0 ? ~bits : (bits ^ kSignMask);
        StoreBigEndian(ordered_bits);
    }

    auto Cut(size_t n) -> void { depth_ += n; }

    auto Data() const -> const byte* {
        return (external_data_ != nullptr ? external_data_ : inline_data_.data()) + depth_;
    }
    auto Len() const -> size_t { return len_ - depth_; }

private:
    auto StoreBigEndian(uint64_t value) -> void {
        for (size_t i = 0; i < sizeof(uint64_t); ++i) {
            inline_data_[sizeof(uint64_t) - 1 - i] = static_cast<byte>(value & 0xFFU);
            value >>= 8U;
        }
    }

    std::array<byte, sizeof(uint64_t)> inline_data_{};
    const byte*                        external_data_{nullptr};
    size_t                             len_{0}, depth_{0};
};

} // namespace idlekv
