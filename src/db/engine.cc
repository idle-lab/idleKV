#include "db/engine.h"

#include "common/asio_no_exceptions.h"
#include "common/logger.h"
#include "db/command.h"
#include "db/shard.h"
#include "db/squasher.h"
#include "db/transaction.h"
#include "redis/connection.h"
#include "redis/error.h"
#include "server/el_pool.h"
#include "server/thread_state.h"

#include <boost/fiber/buffered_channel.hpp>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mimalloc.h>
#include <spdlog/spdlog.h>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>
#include <xxhash.h>

namespace idlekv {

std::unique_ptr<IdleEngine> engine = nullptr;

IdleEngine::IdleEngine(const Config& cfg) : cfg_(cfg) {}

auto IdleEngine::Init(EventLoopPool* elp) -> void {
    // TODO(cyb): recovery data.

    // Init shard_set
    InitCommand();

    shards_.resize(cfg_.shard_num_);

    elp->AwaitForeach([this](size_t i, EventLoop* el) {
        if (i >= cfg_.shard_num_) {
            return;
        }
        auto* heap = ThreadState::Tlocal()->DataHeap();

        void*  ptr   = mi_heap_malloc_aligned(heap, sizeof(Shard), alignof(Shard));
        local_shard_ = new (ptr) Shard(cfg_, el, heap);

        shards_[i] = local_shard_;
    });
}

auto IdleEngine::InitCommand() -> void {
    InitSystemCmd(this);
    InitStrings(this);
    InitHash(this);
    InitList(this);
}

auto IdleEngine::DispatchCmd(ExecContext* ctx, CmdArgs& args) noexcept -> void {
    auto* conn = ctx->conn;
    auto&  sender = conn->GetSender();

    auto cmd = GetCmd(args[0]);
    if (cmd == nullptr) {
        return sender.SendError(fmt::format(kUnknownCmdErrFmt, args[0]));
    }

    if (!cmd->Verification(args)) {
        return sender.SendError(fmt::format(kArgNumErrFmt, cmd->Name()));
    }

    ctx->sender = &sender;
    if (cmd->CanExecInPlace()) {
        cmd->Exec(ctx, args);
        return;
    }

    if (cmd->IsTransactional()) {
        if (ctx->txn == nullptr) {
            ctx->txn = std::make_unique<Transaction>();
        }

        ctx->txn->InitSingle(cmd, &args, cmd->PrepareKeys(args));
    }

    cmd->Exec(ctx, args);

    if (cmd->IsTransactional()) {
        ctx->txn->Done();
    }
}

auto IdleEngine::DispatchManyCmd(ExecContext* ctx, utils::Generator<PendingRequest>& gen, size_t limit) noexcept
    -> size_t {
    std::vector<CommandContext> pipeline_cmds;
    pipeline_cmds.reserve(limit);
    auto* conn = ctx->conn;
    size_t count = 0;
    
    if (ctx->txn == nullptr) {
        ctx->txn = std::make_unique<Transaction>();
    }
    ctx->txn->InitMulti(MultiMode::Squash);

    auto squash = [&]() {
        if (pipeline_cmds.empty()) {
            return;
        }

        size_t processed = CmdSquasher::Squash(pipeline_cmds, &ctx->conn->GetSender(), ctx);

        CHECK_EQ(processed, pipeline_cmds.size());
        pipeline_cmds.clear();
    };

    for (auto& req : gen) {
        if (count >= limit) {
            break;
        }

        auto& args = *req.args;
        auto  cmd  = GetCmd(args[0]);


        bool verify_failure = cmd != nullptr && !cmd->Verification(args);
        bool should_squash = cmd == nullptr || verify_failure || cmd->IsStateChange();

        count++;
        if (!should_squash) {
            pipeline_cmds.emplace_back(cmd, req.args, cmd->PrepareKeys(args),
                                       req.started_at);
            continue;
        }

        squash();

        if (cmd == nullptr) {
            conn->GetSender().SendError(fmt::format(kUnknownCmdErrFmt, args[0]));
            continue;
        }

        if (verify_failure) {
            conn->GetSender().SendError(fmt::format(kArgNumErrFmt, cmd->Name()));
            continue;
        }

        DispatchCmd(ctx, args);
    }

    squash();

    ctx->txn->Done();
    return count;
}

auto IdleEngine::RegisterCmd(const std::string& name, int32_t arity, int32_t FirstKey,
                             int32_t LastKey, Exector exector, Prepare prepare, CmdFlags flags)
    -> void {
    cmd_map_.emplace(
        std::piecewise_construct, std::forward_as_tuple(name),
        std::forward_as_tuple(name, arity, FirstKey, LastKey, exector, prepare, flags));
}

auto IdleEngine::GetCmd(std::string_view name) -> Cmd* {
    auto it = cmd_map_.find(name);
    if (it == cmd_map_.end()) {
        return nullptr;
    }

    return &it->second;
}

} // namespace idlekv
