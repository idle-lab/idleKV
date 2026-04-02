#pragma once

#include "common/config.h"
#include "common/logger.h"
#include "db/command.h"
#include "db/engine.h"
#include "db/shard.h"
#include "utils/coroutine/generator.h"
#include "utils/fiber/block_counter.h"

#include <absl/container/inlined_vector.h>
#include <boost/fiber/barrier.hpp>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

namespace idlekv {

using TxnId = uint64_t;

enum class MultiMode : uint8_t {
    Default,
    Squash,
};

class MultiCmd {
public:
    explicit MultiCmd(MultiMode mode = MultiMode::Default) : mode_(mode) {}

    auto Size() -> size_t { return cmds_.size(); }

    auto Append(Cmd* cmd, CmdArgs args, WRSet keys) -> void {
        cmds_.emplace_back(cmd, std::move(args), keys);
    }

    auto Mode() -> MultiMode { return mode_; }

    auto Next() -> CommandContext {
        CHECK(!cmds_.empty());
        CommandContext ctx = std::move(cmds_[progress_]);
        progress_++;
        return ctx;
    }

private:
    std::vector<CommandContext> cmds_;
    size_t                      progress_{0};
    MultiMode                   mode_;
};
/*
1. normal: 单指令事务

2. multi：
    - multi...exec 的多指令原子事务

    - pipeline 的多指令执行，不需要保证整个 pipeline 的原子性，只需要按顺序执行。

        pipeline 需要创建多个 MultiSub 的子事务（取决于当前批次中指令涉及的 shard 数量），每个子事务创建 Squash 模式的 MultiCmd

        MultiSub 类型的子事务直接运行在对应的 shard 的 worker fiber 上，不需要进行调度

*/
enum class TxnType : uint8_t {
    Uninitialized,
    Normal,
    Multi,
    MultiSub,
};

class Transaction {
public:
    Transaction() = default;

    DISABLE_COPY_MOVE(Transaction);

    auto InitMulti(MultiMode mode = MultiMode::Default) -> void {
        type_ = TxnType::Multi;
        multi_ = std::make_unique<MultiCmd>(mode);
    }

    auto InitMultiSub(MultiMode mode = MultiMode::Default) -> void {
        type_ = TxnType::MultiSub;
        multi_ = std::make_unique<MultiCmd>(mode);
    }

    auto CollectCmd(Cmd* cmd, CmdArgs args, WRSet keys) -> void {
        CHECK(multi_ != nullptr);
        multi_->Append(cmd, std::move(args), keys);
    }

    auto MultiNext() -> CommandContext {
        auto cmdctx = multi_->Next();

        InitByArgs(cmdctx.args, cmdctx.keys);
        return cmdctx;
    }

    auto InitSingle(Cmd* cmd, CmdArgs args, WRSet keys) -> void {
        type_ = TxnType::Normal;
        single_ = std::make_unique<CommandContext>(cmd, std::move(args), keys);
        InitByArgs(single_->args, single_->keys);
    }

    template <class Fn>
        requires std::invocable<Fn, Transaction*, Shard*>
    auto Execute(Fn&& task) -> void {
        CHECK_NE(type_, TxnType::Uninitialized);
        if (multi_ && multi_->Mode() == MultiMode::Squash) {
            task(this, engine->LocalShard());
            return;
        }

        // TODO(cyb): support multi-shard transaction.
        CHECK(shards_.size() == 1); 

        if (active_shard_count == 1 && unique_shard_ == engine->LocalShard()) {
            task(this, unique_shard_);
            return;
        }

        // we should schedule the task to the shard fiber and wait for it to finish before returning.
        block_counter_.Start(active_shard_count);

        for (auto* shard : ActiveShards()) {
            shard->Add([task = std::move(task), this]() mutable {
                task(this, engine->LocalShard());
                block_counter_.Done();
            });
        }
        block_counter_.Wait();
    }

private:
    auto ActiveShards() -> utils::Generator<Shard*> {
        for (auto* shard : shards_) {
            if (shard) {
                co_yield shard;
            }
        }
    }

    auto InitByArgs(const CmdArgs& args, const WRSet& wr_set) -> void {
        shards_.resize(engine->ShardNum());
        InitByKeySet(args, wr_set.read_keys);
        InitByKeySet(args, wr_set.write_keys);
    }

    auto InitByKeySet(const CmdArgs& args, const KeySet& kset) -> void {
        for (auto& i : kset) {
            ShardId shard_id = CalculateShardId(args[i], engine->ShardNum());

            if (!shards_[shard_id]) {
                shards_[shard_id] = engine->ShardAt(shard_id);
                active_shard_count++;
                unique_shard_ = shards_[shard_id];
            }
        }
    }

    TxnId txn_id_;
    TxnType type_{TxnType::Uninitialized};

    utils::SingleWaiterBlockCounter block_counter_;

    absl::InlinedVector<Shard*, 6> shards_;
    size_t                         active_shard_count{0};
    // only valid when active_shard_count == 1
    Shard* unique_shard_{nullptr};

    std::unique_ptr<MultiCmd> multi_;
    std::unique_ptr<CommandContext> single_;
};

} // namespace idlekv