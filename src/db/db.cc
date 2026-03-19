#include "db/db.h"

#include <memory>
#include <memory_resource>

namespace idlekv {

DB::DB(std::pmr::memory_resource* mr) : prime_(mr) {}

auto DB::Locks(const std::vector<std::string>& ws, const std::vector<std::string>& rs) -> bool {
    (void)ws;
    (void)rs;
    return true;
}

auto DB::Set(std::string key, DataEntity value) -> Result<bool> {
    return prime_.Set(std::move(key), std::make_shared<DataEntity>(std::move(value)));
}

auto DB::Get(const std::string& key) -> Result<std::shared_ptr<DataEntity>> {
    return prime_.Get(key);
}

auto DB::Del(const std::string& key) -> Result<bool> { return prime_.Del(key); }

} // namespace idlekv
