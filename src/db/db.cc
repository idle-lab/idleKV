#include "db/db.h"

#include "db/storage/result.h"
#include "db/storage/value.h"

#include <optional>
#include <string_view>

namespace idlekv {

auto DB::Locks(const std::vector<std::string>& ws, const std::vector<std::string>& rs) -> bool {
    (void)ws;
    (void)rs;
    return true;
}

auto DB::Set(std::string_view key, PrimeValue value) -> Result<void> {
    return prime_.Set(key, std::move(value));
}

auto DB::Get(std::string_view key, Value::TypeEnum type) -> Result<PrimeValue> {
    auto res = prime_.Get(key);
    if (!res.Ok()) {
        return res;
    }

    if (res.payload->Type() != type) {
        return OpStatus::WrongType;
    }
    return res;
}

auto DB::Del(std::string_view key) -> Result<void> { return prime_.Del(key); }

} // namespace idlekv
