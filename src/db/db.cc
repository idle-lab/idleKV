#include "db/db.h"
#include <memory_resource>

namespace idlekv {

DB::DB(std::pmr::memory_resource* mr) : prime_(mr) {}

auto DB::locks(const std::vector<std::string>& ws, const std::vector<std::string>& rs) -> bool {
    (void)ws;
    (void)rs;
    return true;
}

auto DB::set(std::string key, DataEntity value) -> Result<bool> {
    return prime_.set(std::move(key), std::move(value));
}

auto DB::get(const std::string& key) -> Result<std::optional<DataEntity>> { return prime_.get(key); }

auto DB::del(const std::string& key) -> Result<bool> { return prime_.del(key); }

} // namespace idlekv
