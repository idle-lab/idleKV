#include "db/engine.h"

#include "common/asio_no_exceptions.h"
#include "common/logger.h"
#include "db/command.h"
#include "db/result.h"
#include "db/shard.h"
#include "metric/request_stage.h"
#include "redis/connection.h"
#include "redis/error.h"
#include "server/el_pool.h"
#include "server/thread_state.h"

#include <asio/as_tuple.hpp>
#include <asio/awaitable.hpp>
#include <asio/dispatch.hpp>
#include <asio/error.hpp>
#include <asio/steady_timer.hpp>
#include <asio/use_awaitable.hpp>
#include <asio/use_future.hpp>
#include <asiochan/channel.hpp>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <spdlog/spdlog.h>
#include <string_view>
#include <tuple>
#include <utility>
#include <xxhash.h>

namespace idlekv {

std::unique_ptr<IdleEngine> engine = nullptr;

IdleEngine::IdleEngine(const Config& cfg) : db_num_(cfg.db_num_) {}

auto IdleEngine::Init(EventLoopPool* elp) -> void {
    InitCommand();

    shard_num_ = elp->PoolSize();
    shard_set_.resize(elp->PoolSize());
    LOG(info, "init shared set [size:{}]", shard_num_);

    elp->AwaitForeach([this](size_t i, [[maybe_unused]] EventLoop* el) {
        auto* DataHeap = ThreadState::Tlocal()->DataHeap();

        shard_set_[i] = std::make_unique<Shard>(DataHeap, i, db_num_);
    });
}

auto IdleEngine::InitCommand() -> void {
    InitSystemCmd(this);
    InitStrings(this);
    InitHash(this);
    InitList(this);
}

auto IdleEngine::CalculateShardId(std::string_view key) -> ShardId {
    return XXH64(key.data(), key.size(), 114514) % shard_num_;
}

auto IdleEngine::DispatchCmd(Connection* conn, const std::vector<std::string>& args) noexcept
    -> ExecResult {
    size_t id = ThreadState::Tlocal()->PoolIndex();

    auto cmd = GetCmd(args[0]);
    if (cmd == nullptr) {
        return ExecResult::Error(fmt::format(kUnknownCmdErrFmt, args[0]));
    }

    if (!cmd->Verification(args)) {
        return ExecResult::Error(fmt::format(kArgNumErrFmt, cmd->Name()));
    }

    if (cmd->CanExecInline()) {
        CmdContext cmdctx(conn, nullptr, id);
        return cmd->Exec(&cmdctx, args);
    }

    ShardId shard_id = id;

    if (cmd->FirstKey() != -1) {
        // now only support single-key command, so directly check if the first key is a key
        if (args.size() <= static_cast<size_t>(cmd->FirstKey())) {
            return ExecResult::Error(fmt::format(kArgNumErrFmt, cmd->Name()));
        }

        shard_id = CalculateShardId(args[cmd->FirstKey()]);
    }

    auto db_ptr = shard_set_[shard_id]->DbAt(conn->DbIndex()); // keep shared_ptr alive
    CmdContext cmdctx(conn, db_ptr.get(), 0);
    return cmd->Exec(&cmdctx, args);
}

auto IdleEngine::RegisterCmd(const std::string& name, int32_t arity, int32_t FirstKey,
                             int32_t LastKey, Exector exector, Prepare prepare,
                             CmdFlags flags) -> void {
    cmd_map_.emplace(std::piecewise_construct, std::forward_as_tuple(name),
                     std::forward_as_tuple(name, arity, FirstKey, LastKey, exector, prepare,
                                           flags));
}

auto IdleEngine::GetCmd(const std::string& name) -> Cmd* {
    auto it = cmd_map_.find(name);
    if (it == cmd_map_.end()) {
        return nullptr;
    }

    return &it->second;
}

} // namespace idlekv
