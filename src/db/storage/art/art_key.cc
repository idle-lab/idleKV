#include "db/storage/art/art_key.h"
#include <cstddef>
#include <cstdint>
#include <string_view>


namespace idlekv {

auto ArtKey::BuildFromString(std::string_view s) -> ArtKey {
    return ArtKey(reinterpret_cast<const byte*>(s.data()), s.size() + 1);
}

auto ArtKey::BuildFromUint32(uint32_t& n) -> ArtKey {
    return ArtKey(reinterpret_cast<const byte*>(&n), 4);
}


} // namespace idlekv
