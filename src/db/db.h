#pragma once

#include "common/asio_no_exceptions.h"
#include "db/storage/kvstore.h"
#include "db/storage/result.h"
#include "db/storage/data_entity.h"

#include <asio/awaitable.hpp>
#include <memory>
#include <memory_resource>
#include <mimalloc.h>
#include <string>
#include <vector>

namespace idlekv {

// DB stores data and execute user's commands
class DB {
public:
    using StoreType = KvStore<ArtImpl<std::string, std::shared_ptr<DataEntity>>>;

    explicit DB(std::pmr::memory_resource* mr);

    auto Locks(const std::vector<std::string>& ws, const std::vector<std::string>& rs) -> bool;

    auto Set(std::string key, DataEntity value) -> Result<bool>;

    auto Get(const std::string& key) -> Result<std::shared_ptr<DataEntity>>;

    auto Del(const std::string& key) -> Result<bool>;

    // TODO(cyb)
    auto MemoryUsage() -> size_t;

private:
    StoreType prime_;
};

} // namespace idlekv
