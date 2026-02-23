#pragma once

#include "db/index/index.h"
#include "redis/connection.h"
#include "redis/protocol/error.h"
#include "redis/protocol/reply.h"

#include <asio/awaitable.hpp>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace idlekv {

// DB stores data and execute user's commands
class DB : std::enable_shared_from_this<DB> {
public:
    auto exec(std::shared_ptr<Connection> conn, const std::vector<std::string>& args) noexcept
        -> asio::awaitable<std::pair<std::string, std::unique_ptr<Err>>>;


    auto Locks(const std::vector<std::string>& ws, const std::vector<std::string>& rs) -> bool;

private:
    Index                                        data_;
    std::mutex                                   data_mu_;
    std::unordered_map<std::string, std::string> kv_;
};

} // namespace idlekv
