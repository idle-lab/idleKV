#include "db/squasher.h"
#include "common/logger.h"
#include "db/engine.h"
#include "db/shard.h"
#include "db/transaction.h"
#include "db/client.h"
#include "redis/parser.h"
#include "utils/fiber/block_counter.h"

#include <variant>

namespace idlekv {

auto CmdSquasher::Squash(std::vector<CommandContext>& cmds, Sender* sender, ExecContext* client) -> size_t {
    CmdSquasher cs{client};
    cs.Squash(cmds, sender);
    return cs.processed_;
}

auto CmdSquasher::Squash(std::vector<CommandContext>& cmds, Sender* sender) -> void {
    for (auto& cmd : cmds) {
        auto res = TrySquash(cmd);

        if (res == DetermineResult::CanNotSquash) {
            // TODO(cyb): multi-shard cmd should execute stand alone.
            CHECK(false) << "idlekv doesn't support multi-shard now";
        }
    }

    ExecuteSquash(sender);
}

auto CmdSquasher::ExecuteSquash(Sender* sender) -> void {
    utils::SingleWaiterBlockCounter bc;
    bc.Start(active_shard_count_);

    auto cb = [&]() {
        auto* shard = engine->LocalShard();
        auto& si = shards_info_[shard->GetShardId()];
        auto& sub_ctx = si.sub_ctx;
        si.results.reserve(sub_ctx.txn->MultiSize());
        ReplyCapturer capturer;
        sub_ctx.sender = &capturer;

        while(!sub_ctx.txn->IsFinished()) {
            auto cmdctx = sub_ctx.txn->MultiNext();

            cmdctx.cmd->Exec(&sub_ctx, *cmdctx.args);   
            si.results.emplace_back(capturer.TakePayload());
        }

        bc.Done();
    };

    for (size_t i = 0;i < shards_info_.size();i++) {
        if (shards_info_[i].sub_ctx.txn) {
            engine->ShardAt(i)->Add(cb);
        }
    }

    bc.Wait();

    PayloadVisitor pv(sender);
    for (auto i : order_) {
        auto& si = shards_info_[i];

        std::visit(pv, si.results[si.send_idx]);
        si.send_idx++;
    }

    shards_info_.clear();
    processed_ += order_.size();
    order_.clear();
    active_shard_count_ = 0;
}


auto CmdSquasher::ShardInfo(ShardId id) -> ShardExecInfo& {
    if (shards_info_.size() == 0) {
        shards_info_.resize(engine->ShardNum());
    }

    if (!shards_info_[id].sub_ctx.txn) {
        shards_info_[id].sub_ctx.txn = parent_ctx_->txn->CreateMultiSub(engine->ShardAt(id));
        shards_info_[id].sub_ctx.db_index = parent_ctx_->db_index;
        active_shard_count_++;
    }

    return shards_info_[id];
}

auto CmdSquasher::TrySquash(CommandContext& cmd) -> DetermineResult {
    ShardId last_shard_id = kInvalidShardId;

    for (auto i : cmd.keys.AllKeys()) {
        auto key = cmd.args->at(i);

        ShardId id = CalculateShardId(key, engine->ShardNum());
        if (last_shard_id == kInvalidShardId || id == last_shard_id) {
            last_shard_id = id;
        } else {
            return DetermineResult::CanNotSquash;
        }
    }

    if (last_shard_id == kInvalidShardId) {
        last_shard_id = 0;
    }

    auto& sf = ShardInfo(last_shard_id);

    sf.sub_ctx.txn->CollectCmd(CommandContext{cmd.cmd, cmd.args, {}, cmd.start_at});
    order_.push_back(last_shard_id);
    return DetermineResult::OK;
}


} // namespace idlekv
