#pragma once

#include "common/config.h"
#include "db/command.h"
#include "db/db.h"
#include "db/shard.h"
#include "redis/connection.h"

#include <cstddef>
#include <memory>
#include <mimalloc.h>
#include <vector>

namespace idlekv {

// IdleEngine is idlekv store engine with full capabilities including multiple database.
class IdleEngine {
public:
    IdleEngine(const Config& cfg, mi_heap_t* heap) : heap_(heap) {
        init_command();

    }

    auto exec(Connection*, const std::vector<std::string>& args) noexcept -> std::string;

    // return the database at the specified index. If the index is out of bounds, return null
    auto select_db(size_t idx) -> std::shared_ptr<DB>;

    auto get_cmd(const std::string& name) -> Cmd*;

    auto register_cmd(const std::string& name, int32_t arity, int32_t first_key, int32_t last_key,
                      Exector exector, Prepare prepare) -> void;

private:
    auto init_command() -> void;

    mi_heap_t* heap_;

    // read-only
    std::unordered_map<std::string, Cmd>    cmd_map_;
    std::vector<std::unique_ptr<Shard>>     shard_set_;
};

auto init_systemcmd(IdleEngine*) -> void;
auto init_strings(IdleEngine*) -> void;
auto init_hash(IdleEngine*) -> void;
auto init_list(IdleEngine*) -> void;

} // namespace idlekv
