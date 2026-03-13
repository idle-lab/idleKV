#pragma once

#include "common/asio_no_exceptions.h"
#include "db/storage/kvstore.h"
#include "db/storage/result.h"
#include "db/xmalloc.h"

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
    using StoreType = KvStore<DummyImpl<std::string, DataEntity>>;

    explicit DB(std::pmr::memory_resource* mr);

    auto locks(const std::vector<std::string>& ws, const std::vector<std::string>& rs) -> bool;

    auto set(std::string key, DataEntity value) -> Result<bool>;

    auto get(const std::string& key) -> Result<std::optional<DataEntity>>;

    auto del(const std::string& key) -> Result<bool>;

private:
    StoreType prime_;
};

} // namespace idlekv
