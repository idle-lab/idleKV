#include "db/storage/art/art_key.h"
#include <cstddef>
#include <string_view>


namespace idlekv {

auto ArtKey::BuildFromString(std::string_view s) -> ArtKey {
    return ArtKey{
        .data = reinterpret_cast<const byte*>(s.data()),
        .len = s.size(),
    };
}

} // namespace idlekv
