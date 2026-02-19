#pragma once

#include "db/command.h"
#include "redis/protocol/error.h"
#include <asio/awaitable.hpp>
#include <memory>
#include <string>
#include <utility>

namespace idlekv {

class DB : std::enable_shared_from_this<DB> {
public:
    auto exec() -> asio::awaitable<std::pair<std::string, std::unique_ptr<Err>>>;

private:
    std::unordered_map<std::string, Cmd*> cmd_map_;
};

} // namespace idlekv
