#pragma once

#include "db/storage/kvstore.h"
#include "db/storage/result.h"
#include <asio/awaitable.hpp>
#include <memory_resource>
#include <string>
#include <vector>

namespace idlekv {

// DB stores data and execute user's commands
class DB {
public:
    DB(std::pmr::memory_resource* mr) : prime_(mr) { }

    auto locks(const std::vector<std::string>& ws, const std::vector<std::string>& rs) -> bool;

    auto set(const std::string& key, DataEntity value) -> Result<bool> {
        return prime_.set(key, value);
    }

private:
    KvStore<DummyImpl<std::string, DataEntity>> prime_;

};

} // namespace idlekv
