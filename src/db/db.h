#pragma once

#include "db/storage/data_entity.h"
#include "db/storage/kvstore.h"
#include "db/storage/result.h"
#include "db/storage/value.h"

#include <memory>
#include <memory_resource>
#include <mimalloc.h>
#include <string>
#include <vector>

namespace idlekv {

// DB stores data and execute user's commands
class DB {
public:
    using PrimeTable = KvStore<DummyImpl<std::string, PrimeValue>>;

    explicit DB(std::pmr::memory_resource* mr) : prime_(mr) {}

    auto Locks(const std::vector<std::string>& ws, const std::vector<std::string>& rs) -> bool;

    auto Set(std::string key, DataEntity value) -> Result<bool>;

    auto Get(std::string_view key) -> Result<std::shared_ptr<DataEntity>>;

    auto Del(std::string_view key) -> Result<bool>;

    // TODO(cyb)
    auto MemoryUsage() -> size_t;

private:
    PrimeTable prime_;
};

} // namespace idlekv
