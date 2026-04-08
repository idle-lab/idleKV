#include "db/db.h"
#include "db/storage/value.h"

#include <optional>
#include <string_view>

namespace idlekv {

auto DB::Locks(const std::vector<std::string>& ws, const std::vector<std::string>& rs) -> bool {
    (void)ws;
    (void)rs;
    return true;
}

auto DB::Set(std::string key, PrimeValue value) -> Result<void> {
    return prime_.Set(std::move(key), std::move(value));
}

auto DB::Get(std::string_view key) -> Result<PrimeValue> {
    return prime_.Get(key);
}

auto DB::Del(std::string_view key) -> Result<void> { return prime_.Del(key); }

} // namespace idlekv
