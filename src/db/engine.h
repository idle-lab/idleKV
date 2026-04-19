#pragma once

#include "common/config.h"
#include "db/command.h"
#include "db/shard.h"
#include "server/el_pool.h"
#include "utils/coroutine/generator.h"

#include <absl/container/flat_hash_map.h>
#include <absl/functional/function_ref.h>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mimalloc.h>
#include <string>
#include <string_view>
#include <vector>

namespace idlekv {

class ExecContext;

// IdleEngine is idlekv store engine with full capabilities including multiple database.
class IdleEngine {
public:
    IdleEngine(const Config& cfg);

    auto Init(EventLoopPool* elp) -> void;
    auto DispatchCmd(ExecContext*, CmdArgs&) noexcept -> void;
    auto DispatchManyCmd(ExecContext* ctx, utils::Generator<PendingRequest>& gen,
                         size_t limit) noexcept -> size_t;

    auto DbNum() const -> size_t { return cfg_.db_num_; }
    auto RegisterCmd(const std::string& name, int32_t arity, int32_t FirstKey, int32_t LastKey,
                     Exector exector, Prepare prepare, CmdFlags flags = CmdFlags::None) -> void;
    auto NextTxnId() -> TxnId { return next_txn_id_.fetch_add(1, std::memory_order_relaxed); }

    auto ShardAt(size_t index) -> Shard* { return shards_[index]; }
    auto ShardNum() -> size_t { return shards_.size(); }
    auto LocalShard() -> Shard* { return local_shard_; }

private:
    auto GetCmd(std::string_view name) -> Cmd*;
    auto InitCommand() -> void;

    std::vector<Shard*> shards_;

    inline static thread_local Shard* local_shard_{nullptr};

    // read-only
    absl::flat_hash_map<std::string, Cmd> cmd_map_;
    const Config&                         cfg_;
    std::atomic<TxnId>                    next_txn_id_{1};
};

extern std::unique_ptr<IdleEngine> engine;

auto InitSystemCmd(IdleEngine*) -> void;
auto InitStrings(IdleEngine*) -> void;
auto InitHash(IdleEngine*) -> void;
auto InitList(IdleEngine*) -> void;
auto InitZSet(IdleEngine*) -> void;

} // namespace idlekv
