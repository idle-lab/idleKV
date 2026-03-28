#include "db/engine.h"

#include "common/asio_no_exceptions.h"
#include "common/logger.h"
#include "db/command.h"
#include "db/shard.h"
#include "db/storage/alloctor.h"
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
#include <tuple>
#include <utility>
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
        Shard* shard = new (ptr) Shard(cfg_, el, heap);

        shards_[i] = shard;
    });
}

auto IdleEngine::InitCommand() -> void {
    InitSystemCmd(this);
    InitStrings(this);
    InitHash(this);
    InitList(this);
}

auto IdleEngine::DispatchCmd(Connection* conn, std::vector<std::string>& args) noexcept -> void {
    size_t id     = ThreadState::Tlocal()->PoolIndex();
    auto&  sender = conn->GetSender();

    auto cmd = GetCmd(args[0]);
    if (cmd == nullptr) {
        return sender.SendError(fmt::format(kUnknownCmdErrFmt, args[0]));
    }

    if (!cmd->Verification(args)) {
        return sender.SendError(fmt::format(kArgNumErrFmt, cmd->Name()));
    }

    if (cmd->CanExecInline()) {
        ExecContext cmdctx(conn, id);
        cmd->Exec(&cmdctx, args);
        return;
    }

    ShardId shard_id = id;

    if (cmd->FirstKey() > 0) {
        shard_id = CalculateShardId(args[cmd->FirstKey()]);
    }

    ExecContext cmdctx(conn, 0);
    cmdctx.InitShard(ShardAt(shard_id));

    cmd->Exec(&cmdctx, args); 
}

auto IdleEngine::RegisterCmd(const std::string& name, int32_t arity, int32_t FirstKey,
                             int32_t LastKey, Exector exector, Prepare prepare, CmdFlags flags)
    -> void {
    cmd_map_.emplace(
        std::piecewise_construct, std::forward_as_tuple(name),
        std::forward_as_tuple(name, arity, FirstKey, LastKey, exector, prepare, flags));
}

auto IdleEngine::GetCmd(const std::string& name) -> Cmd* {
    auto it = cmd_map_.find(name);
    if (it == cmd_map_.end()) {
        return nullptr;
    }

    return &it->second;
}

} // namespace idlekv
