#pragma once


#include "db/storage/art/node.h"
#include <cstddef>
#include <cstdint>
#include <string_view>
namespace idlekv {


struct ArtKey {
public:
    ArtKey(const byte* data, size_t len) : data_(data), len_(len) { }

    // caller should guaranteed lifecycle of @s and @s ends with '\0'.
    // now ArtKey not support string with '\0' character.
    static auto BuildFromString(std::string_view s) -> ArtKey;

    static auto BuildFromUint32(uint32_t& n) -> ArtKey;
    // you can extend other types of art keys here(i.e. IEEE float, integer)


    auto Cut(size_t n) -> void {
        depth_ += n;
    }

    auto Data() -> const byte* { return data_ + depth_; }
    auto Len() -> size_t { return len_ - depth_; }
private:
    const byte* data_;
    size_t len_, depth_{0};
};

} // namespace idlekv
