#pragma once

#include "db/index/index.h"
#include "redis/connection.h"
#include "redis/protocol/error.h"

#include <asio/awaitable.hpp>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace idlekv {

// DB stores data and execute user's commands
class DB : std::enable_shared_from_this<DB> {
public:
    auto exec(std::shared_ptr<Connection> conn, const std::vector<std::string>& args)
        -> asio::awaitable<std::pair<std::string, std::unique_ptr<Err>>>;

private:

    Index data_;
};

} // namespace idlekv
