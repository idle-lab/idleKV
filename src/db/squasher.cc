#include "db/squasher.h"

#include "common/logger.h"
#include "db/context.h"
#include "db/engine.h"
#include "db/shard.h"
#include "redis/parser.h"
#include "utils/fiber/block_counter.h"

#include <boost/fiber/barrier.hpp>
#include <memory>
#include <variant>

namespace idlekv {

auto CmdSquasher::DebugCheckState(std::string_view where) const -> void {
    // CHECK_EQ(debug_canary_head_, 0xC0DEC0DEC0DEC0DEULL)
    //     << "CmdSquasher head canary corrupted at " << where;
    // CHECK_EQ(debug_canary_tail_, 0xFEE1DEADFEE1DEADULL)
    //     << "CmdSquasher tail canary corrupted at " << where;
    CHECK(parent_ctx_ != nullptr) << "CmdSquasher lost parent_ctx_ at " << where;
    CHECK_LE(active_shard_count_, shards_info_.size())
        << "CmdSquasher active_shard_count_ overflow at " << where;
    CHECK(shards_info_.empty() || shards_info_.size() == engine->ShardNum())
        << "CmdSquasher shards_info_ size mismatch at " << where;

    for (size_t shard_id = 0; shard_id < shards_info_.size(); ++shard_id) {
        const auto& si = shards_info_[shard_id];
        if (si.sub_ctx.txn) {
            CHECK_GE(si.results.size(), si.send_idx)
                << "CmdSquasher send_idx overflow at " << where << ", shard=" << shard_id;
        } else {
            CHECK_EQ(si.send_idx, 0U) << "CmdSquasher inactive shard has send progress at " << where
                                      << ", shard=" << shard_id;
            CHECK(si.results.empty()) << "CmdSquasher inactive shard has buffered replies at "
                                      << where << ", shard=" << shard_id;
        }
    }

    for (auto shard_id : order_) {
        CHECK_LT(shard_id, shards_info_.size())
            << "CmdSquasher order_ corrupted at " << where << ", shard=" << shard_id;
        CHECK(shards_info_[shard_id].sub_ctx.txn)
            << "CmdSquasher order_ points to inactive shard at " << where << ", shard=" << shard_id;
    }
}

auto CmdSquasher::Squash(std::vector<CommandContext>& cmds, Sender* sender, ExecContext* client)
    -> size_t {
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

    for (size_t i = 0; i < shards_info_.size(); i++) {
        if (shards_info_[i].sub_ctx.txn) {
            auto* si = &shards_info_[i];
            engine->ShardAt(i)->Add([bc, si]() mutable -> void {
                auto& sub_ctx = si->sub_ctx;
                si->results.reserve(sub_ctx.txn->MultiSize());
                ReplyCapturer capturer;
                sub_ctx.sender = &capturer;

                while (!sub_ctx.txn->IsFinished()) {
                    auto cmdctx = sub_ctx.txn->MultiNext();

                    cmdctx.cmd->Exec(&sub_ctx, *cmdctx.args);
                    si->results.emplace_back(capturer.TakePayload());
                }

                bc.Done();
            });
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
        shards_info_[id].sub_ctx.txn      = parent_ctx_->txn->CreateMultiSub(engine->ShardAt(id));
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
    DebugCheckState("after-order-push");
    return DetermineResult::OK;
}

} // namespace idlekv
