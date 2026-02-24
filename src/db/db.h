#pragma once

#include "db/index/index.h"
#include "redis/connection.h"
#include "redis/protocol/error.h"

#include <asio/awaitable.hpp>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace idlekv {

// DB stores data and execute user's commands
class DB {
public:
    auto exec(std::shared_ptr<Connection> conn, const std::vector<std::string>& args) noexcept
        -> asio::awaitable<std::pair<std::string, std::unique_ptr<Err>>>;

    auto locks(const std::vector<std::string>& ws, const std::vector<std::string>& rs) -> bool;

private:
    Index                                        data_;
    std::mutex                                   data_mu_;
};

} // namespace idlekv
