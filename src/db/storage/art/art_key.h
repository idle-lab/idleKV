#pragma once

#include "db/storage/art/node.h"

#include <cstddef>
#include <cstdint>
#include <string_view>
namespace idlekv {

struct ArtKey {
public:
    ArtKey(const byte* data, size_t len) : data_(data), len_(len) {}

    auto Cut(size_t n) -> void { depth_ += n; }

    auto Data() -> const byte* { return data_ + depth_; }
    auto Len() -> size_t { return len_ - depth_; }

private:
    const byte* data_;
    size_t      len_, depth_{0};
};

} // namespace idlekv
