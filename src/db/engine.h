#pragma once

#include "common/config.h"
#include "db/command.h"
#include "db/context.h"
#include "db/db.h"
#include "db/xmalloc.h"

#include <cstddef>
#include <memory>
#include <memory_resource>
#include <mimalloc.h>
#include <vector>

namespace idlekv {

// IdleEngine is a kv store engine with full capabilities including multiple database.
class IdleEngine {
public:
    IdleEngine(const Config& cfg, mi_heap_t* heap) : alloc_(heap) {
        init_command();

        db_set_.resize(cfg.db_num_);

        for (auto& db : db_set_) {
            db = std::make_shared<DB>(&alloc_);
        }
    }

    auto exec(Context& ctx, const std::vector<std::string>& args) noexcept
        -> std::string;

    // return the database at the specified index. If the index is out of bounds, return null
    auto select_db(size_t idx) -> std::shared_ptr<DB>;

    auto get_cmd(const std::string& name) -> Cmd*;

    auto register_cmd(const std::string& name, int32_t arity, int32_t first_key, int32_t last_key, Exector exector, Prepare prepare)
        -> void;
private:
    auto init_command() -> void;

    XAlloctor alloc_;

    // read-only
    std::unordered_map<std::string, Cmd> cmd_map_;
    std::vector<std::shared_ptr<DB>> db_set_;
};

auto init_systemcmd(IdleEngine*) -> void;
auto init_strings(IdleEngine*) -> void;
auto init_hash(IdleEngine*) -> void;
auto init_list(IdleEngine*) -> void;


} // namespace idlekv
