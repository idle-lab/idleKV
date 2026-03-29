#pragma once

#include "common/config.h"
#include "db/command.h"
#include "db/shard.h"
#include "redis/connection.h"
#include "server/el_pool.h"

#include <absl/container/flat_hash_map.h>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mimalloc.h>
#include <string>
#include <string_view>
#include <vector>

namespace idlekv {

// IdleEngine is idlekv store engine with full capabilities including multiple database.
class IdleEngine {
public:
    IdleEngine(const Config& cfg);

    auto Init(EventLoopPool* elp) -> void;
    auto DispatchCmd(Connection*, CmdArgs& args) noexcept -> void;

    auto DbNum() const -> size_t { return cfg_.db_num_; }
    auto GetCmd(std::string_view name) -> Cmd*;
    auto RegisterCmd(const std::string& name, int32_t arity, int32_t FirstKey, int32_t LastKey,
                     Exector exector, Prepare prepare, CmdFlags flags = CmdFlags::None) -> void;

    auto ShardAt(size_t index) -> Shard* { return shards_[index]; }

private:
    static constexpr uint64_t kSeed = 0x9E3779B97F4A7C15ULL;

    auto CalculateShardId(std::string_view s) -> ShardId {
        return static_cast<ShardId>(XXH64(s.data(), s.size(), kSeed) % cfg_.shard_num_);
    }

    auto InitCommand() -> void;

    std::vector<std::shared_ptr<DB>> db_slice_;

    std::vector<Shard*> shards_;

    // read-only
    absl::flat_hash_map<std::string, Cmd> cmd_map_;
    const Config&                         cfg_;
};

extern std::unique_ptr<IdleEngine> engine;

auto InitSystemCmd(IdleEngine*) -> void;
auto InitStrings(IdleEngine*) -> void;
auto InitHash(IdleEngine*) -> void;
auto InitList(IdleEngine*) -> void;

} // namespace idlekv
