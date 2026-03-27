#pragma once

#include "common/config.h"
#include "db/command.h"
#include "db/shard.h"
#include "redis/connection.h"
#include "server/el_pool.h"

#include <absl/container/flat_hash_map.h>
#include <cstddef>
#include <functional>
#include <memory>
#include <mimalloc.h>
#include <string>
#include <vector>

namespace idlekv {

// IdleEngine is idlekv store engine with full capabilities including multiple database.
class IdleEngine {
public:
    IdleEngine(const Config& cfg);

    auto Init(EventLoopPool* elp) -> void;
    auto DispatchCmd(Connection*, std::vector<std::string>& args) noexcept -> void;

    auto DbNum() const -> size_t { return cfg_.db_num_; }
    auto GetCmd(const std::string& name) -> Cmd*;
    auto RegisterCmd(const std::string& name, int32_t arity, int32_t FirstKey, int32_t LastKey,
                     Exector exector, Prepare prepare, CmdFlags flags = CmdFlags::None) -> void;

    auto DbAt(size_t index) -> DB* {
        CHECK_LT(index, db_slice_.size());
        return db_slice_[index].get();
    }

private:
    auto InitCommand() -> void;

    std::vector<std::shared_ptr<DB>> db_slice_;

    std::vector<Shard*> shards_;

    // read-only
    absl::flat_hash_map<std::string, Cmd> cmd_map_;
    const Config& cfg_;
};

extern std::unique_ptr<IdleEngine> engine;

auto InitSystemCmd(IdleEngine*) -> void;
auto InitStrings(IdleEngine*) -> void;
auto InitHash(IdleEngine*) -> void;
auto InitList(IdleEngine*) -> void;

} // namespace idlekv
