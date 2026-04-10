#pragma once

#include "db/storage/kvstore.h"
#include "db/storage/result.h"
#include "db/storage/value.h"

#include <memory_resource>
#include <mimalloc.h>
#include <string>
#include <vector>

namespace idlekv {

using PrimeTable = KvStore<PrimeValue>;

// DB stores data and execute user's commands
class DB {
public:
    explicit DB(std::pmr::memory_resource* mr) : prime_(mr) {}

    auto Locks(const std::vector<std::string>& ws, const std::vector<std::string>& rs) -> bool;

    auto Set(std::string key, PrimeValue value) -> Result<void>;

    auto Get(std::string_view key, Value::TypeEnum type) -> Result<PrimeValue>;

    auto Del(std::string_view key) -> Result<void>;

    // TODO(cyb)
    auto MemoryUsage() -> size_t;

private:
    PrimeTable prime_;
};

} // namespace idlekv
