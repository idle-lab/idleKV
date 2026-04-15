#pragma once

#include "common/logger.h"
#include "db/storage/art/node.h"

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>

namespace idlekv {

struct ArtKey {
public:
    ArtKey() = default;
    ArtKey(const byte* data, size_t len) : data_(data), len_(len) {}
    ArtKey(const std::string& str)
        : data_(reinterpret_cast<const byte*>(str.data())), len_(str.size()) {}
    ArtKey(std::string_view sv)
        : data_(reinterpret_cast<const byte*>(sv.data())), len_(sv.size()) {}

    auto Cut(size_t n) -> void { depth_ += n; }

    auto Data() const -> const byte* { return data_ == nullptr ? nullptr : data_ + depth_; }
    auto Len() const -> size_t { return len_ - depth_; }

private:
    const byte* data_{nullptr};
    size_t      len_{0}, depth_{0};
};

struct ArtKeyCodec {
public:
    // Strings are encoded into an order-preserving byte stream for ART:
    // - 0x00 terminates the string
    // - bytes 0x00/0x01 inside the payload are escaped with 0x01
    // This keeps the encoded form binary-safe while preserving lexicographic order.
    static constexpr byte kTerminator = 0;
    static constexpr byte kEscape     = 1;
    // Fixed-width numeric components are stored as 8 big-endian bytes so they sort
    // correctly under raw byte comparison inside the ART.
    static constexpr size_t kFixedBytes = sizeof(uint64_t);
    using FixedBuffer                   = std::array<byte, kFixedBytes>;

    static auto EncodedSize(std::string_view value) -> size_t {
        size_t encoded_size = 1; // terminator
        for (unsigned char cur : value) {
            encoded_size += cur <= kEscape ? 2 : 1;
        }
        return encoded_size;
    }

    static auto Encode(std::string_view value, std::string& buffer) -> ArtKey {
        buffer.clear();
        buffer.reserve(EncodedSize(value));
        AppendEncodedString(buffer, value);
        return Wrap(buffer);
    }

    template <typename... Pieces>
    static auto EncodePieces(std::string& buffer, const Pieces&... pieces) -> ArtKey {
        // Composite ART keys such as (score, member) are encoded into one contiguous
        // buffer so callers can avoid temporary per-piece buffers and append chains.
        buffer.clear();
        buffer.reserve((EncodedPieceSize(pieces) + ... + size_t{0}));
        (AppendEncodedPiece(buffer, pieces), ...);
        return Wrap(buffer);
    }

    static auto AppendEncodedString(std::string& out, std::string_view value) -> void {
        for (unsigned char cur : value) {
            if (cur <= kEscape) {
                out.push_back(static_cast<char>(kEscape));
            }
            out.push_back(static_cast<char>(cur));
        }
        out.push_back(static_cast<char>(kTerminator));
    }

    static auto DecodeString(std::span<const byte> encoded) -> std::string {
        std::string decoded;
        decoded.reserve(encoded.size());

        bool terminated = false;
        for (size_t i = 0; i < encoded.size(); ++i) {
            byte cur = encoded[i];
            if (cur == kTerminator) {
                CHECK_EQ(i + 1, encoded.size());
                terminated = true;
                break;
            }

            if (cur == kEscape) {
                CHECK_LT(i + 1, encoded.size());
                cur = encoded[++i];
            }
            decoded.push_back(static_cast<char>(cur));
        }

        CHECK(terminated);
        return decoded;
    }

    static auto Encode(uint64_t value, FixedBuffer& buffer) -> ArtKey {
        StoreBigEndian(value, buffer);
        return {buffer.data(), buffer.size()};
    }

    static auto Encode(double value, FixedBuffer& buffer) -> ArtKey {
        return Encode(OrderedBits(value), buffer);
    }

    static auto DecodeDouble(std::span<const byte> encoded) -> double {
        CHECK_EQ(encoded.size(), kFixedBytes);

        uint64_t ordered = 0;
        for (byte cur : encoded) {
            ordered = (ordered << 8U) | static_cast<uint64_t>(cur);
        }

        constexpr uint64_t kSignMask = 1ULL << 63U;
        const uint64_t     bits = (ordered & kSignMask) != 0 ? (ordered ^ kSignMask) : ~ordered;
        return std::bit_cast<double>(bits);
    }

private:
    static constexpr auto EncodedPieceSize(uint64_t) noexcept -> size_t { return kFixedBytes; }

    static constexpr auto EncodedPieceSize(double) noexcept -> size_t { return kFixedBytes; }

    static auto EncodedPieceSize(std::string_view value) noexcept -> size_t {
        return EncodedSize(value);
    }

    static auto AppendEncodedPiece(std::string& out, uint64_t value) -> void {
        AppendBigEndian(out, value);
    }

    static auto AppendEncodedPiece(std::string& out, double value) -> void {
        AppendBigEndian(out, OrderedBits(value));
    }

    static auto AppendEncodedPiece(std::string& out, std::string_view value) -> void {
        AppendEncodedString(out, value);
    }

    static auto OrderedBits(double value) noexcept -> uint64_t {
        uint64_t bits = std::bit_cast<uint64_t>(value);
        if ((bits << 1U) == 0) {
            bits = 0;
        }

        // IEEE-754 bit patterns do not sort numerically as raw bytes. ART compares
        // keys lexicographically, so negatives are bit-inverted and non-negatives
        // have the sign bit flipped to produce a total order on the encoded bytes.
        constexpr uint64_t kSignMask = 1ULL << 63U;
        return (bits & kSignMask) != 0 ? ~bits : (bits ^ kSignMask);
    }

    static auto AppendBigEndian(std::string& out, uint64_t value) -> void {
        const size_t base = out.size();
        out.resize(base + kFixedBytes);
        for (size_t i = 0; i < kFixedBytes; ++i) {
            out[base + kFixedBytes - 1 - i] = static_cast<char>(value & 0xFFU);
            value >>= 8U;
        }
    }

    static auto StoreBigEndian(uint64_t value, FixedBuffer& buffer) -> void {
        for (size_t i = 0; i < buffer.size(); ++i) {
            buffer[buffer.size() - 1 - i] = static_cast<byte>(value & 0xFFU);
            value >>= 8U;
        }
    }

    static auto Wrap(const std::string& buffer) -> ArtKey {
        return {reinterpret_cast<const byte*>(buffer.data()), buffer.size()};
    }
};

} // namespace idlekv
