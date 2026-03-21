#pragma once

#include "common/config.h"
#include "db/command.h"
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

    auto Init(EventLoopPool* elp) -> void;
    auto CalculateShardId(std::string_view key) -> ShardId;
    auto DispatchCmd(Connection*, const std::vector<std::string>& args) noexcept -> ExecResult;

    auto DbNum() const -> size_t { return db_num_; }
    auto GetCmd(const std::string& name) -> Cmd*;
    auto RegisterCmd(const std::string& name, int32_t arity, int32_t FirstKey, int32_t LastKey,
                     Exector exector, Prepare prepare,
                     CmdFlags flags = CmdFlags::None) -> void;

private:
    auto InitCommand() -> void;

    size_t db_num_;
    // read-only
    absl::flat_hash_map<std::string, Cmd> cmd_map_;
    std::vector<std::unique_ptr<Shard>>  shard_set_;
    size_t                               shard_num_;
};

extern std::unique_ptr<IdleEngine> engine;

auto InitSystemCmd(IdleEngine*) -> void;
auto InitStrings(IdleEngine*) -> void;
auto InitHash(IdleEngine*) -> void;
auto InitList(IdleEngine*) -> void;

} // namespace idlekv
