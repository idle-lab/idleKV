#pragma once

#include "common/config.h"
#include "db/command.h"
#include "db/db.h"
#include "db/result.h"
#include "db/shard.h"
#include "redis/connection.h"
#include "server/el_pool.h"

#include <cstddef>
#include <memory>
#include <mimalloc.h>
#include <absl/container/flat_hash_map.h>
#include <string>
#include <vector>

namespace idlekv {

// IdleEngine is idlekv store engine with full capabilities including multiple database.
class IdleEngine {
public:
    IdleEngine(const Config& cfg);

    auto init(EventLoopPool* elp) -> void;
    auto calculate_shard_id(std::string_view key) -> ShardId;
    auto dispatch_cmd(Connection*, const std::vector<std::string>& args) noexcept -> ExecResult;

    auto db_num() const -> size_t { return db_num_; }
    auto get_cmd(const std::string& name) -> Cmd*;
    auto register_cmd(const std::string& name, int32_t arity, int32_t first_key, int32_t last_key,
                      Exector exector, Prepare prepare,
                      CmdFlags flags = CmdFlags::None) -> void;

private:
    auto init_command() -> void;

    size_t db_num_;
    // read-only
    absl::flat_hash_map<std::string, Cmd> cmd_map_;
    std::vector<std::unique_ptr<Shard>>  shard_set_;
    size_t                               shard_num_;
};

extern std::unique_ptr<IdleEngine> engine;

auto init_systemcmd(IdleEngine*) -> void;
auto init_strings(IdleEngine*) -> void;
auto init_hash(IdleEngine*) -> void;
auto init_list(IdleEngine*) -> void;

} // namespace idlekv
