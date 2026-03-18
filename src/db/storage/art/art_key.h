#pragma once


#include "db/storage/art/node.h"
#include <cstddef>
#include <string_view>
namespace idlekv {



struct ArtKey {
public:
    // caller should guaranteed lifecycle
    static auto BuildFromString(std::string_view s) -> ArtKey;

    // you can extend other types of art keys here(i.e. IEEE float, integer)

    const byte* data;
    size_t len;
};

} // namespace idlekv