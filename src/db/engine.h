#pragma once

#include "db/command.h"
#include "db/db.h"
#include <cstddef>
#include <memory>
#include <vector>

namespace idlekv {

// IdleEngine is a kv store engine with full capabilities including multiple database, rdb loader
class IdleEngine {
public:
    IdleEngine() {
        init_command();
    }

    // return the database at the specified index. If the index is out of bounds, return null
    auto select_db(size_t idx) -> std::shared_ptr<DB>;

private:
    auto init_command() -> void;

    std::unordered_map<std::string, Cmd> cmd_map_;

    std::vector<std::shared_ptr<DB>> db_set_;
};

} // namespace idlekv
