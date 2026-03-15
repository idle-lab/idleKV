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

auto IdleEngine::init(EventLoopPool* elp) -> void {
    init_command();

    shard_num_ = elp->pool_size();
    shard_set_.resize(elp->pool_size());
    LOG(info, "init shared set [size:{}]", shard_num_);

    elp->await_foreach([this](size_t i, [[maybe_unused]] EventLoop* el) {
        auto* data_heap = ThreadState::tlocal()->data_heap();

        shard_set_[i] = std::make_unique<Shard>(data_heap, i, db_num_);
    });
}

auto IdleEngine::init_command() -> void {
    init_systemcmd(this);
    init_strings(this);
    init_hash(this);
    init_list(this);
}

auto IdleEngine::calculate_shard_id(std::string_view key) -> ShardId {
    return XXH64(key.data(), key.size(), 114514) % shard_num_;
}

auto IdleEngine::dispatch_cmd(Connection* conn, const std::vector<std::string>& args) noexcept
    -> void {
    size_t id = ThreadState::tlocal()->pool_index();
    auto promise = std::make_shared<PromiseResult>(conn->get_executor());
    conn->enqueue_result(promise);

    auto cmd = get_cmd(args[0]);
    if (cmd == nullptr) {
        promise->set_reslute(ExecResult::error(fmt::format(kUnknownCmdErrFmt, args[0])));
        promise->notify();
        return;
    }

    if (!cmd->verification(args)) {
        promise->set_reslute(ExecResult::error(fmt::format(kArgNumErrFmt, cmd->name())));
        promise->notify();
        return;
    }

    if (cmd->can_exec_inline()) {
        CmdContext cmdctx(conn, nullptr, id);
        promise->set_reslute(cmd->exec(&cmdctx, args));
        promise->notify();
        return;
    }

    ShardId shard_id = id;

    if (cmd->first_key() != -1) {
        // now only support single-key command, so directly check if the first key is a key
        if (args.size() <= static_cast<size_t>(cmd->first_key())) {
            promise->set_reslute(ExecResult::error(fmt::format(kArgNumErrFmt, cmd->name())));
            promise->notify();
            return;
        }

        shard_id = calculate_shard_id(args[cmd->first_key()]);
    }

    auto db_ptr = shard_set_[shard_id]->db_at(conn->db_index()); // keep shared_ptr alive
    promise->mark_shard_enqueued();

    shard_set_[shard_id]->dispatch([conn, cmd, args = args, promise, db_ptr, owner_id = id] {
        const auto shard_start = PromiseResult::clock::now();
        if (promise->has_stage_tracking()) {
            RequestStageMetrics::instance().observe_queue_to_shard(shard_start -
                                                                   promise->shard_enqueued_at());
        }

        CmdContext cmdctx(conn, db_ptr.get(), owner_id);

        // Execute the command on the shard's worker loop, but do not write the reply here.
        // Connection/Sender owns socket and write-buffer state that belongs to the connection's
        // event loop, so touching sender from a shard loop would cross thread boundaries and can
        // race with other connection-side operations. We therefore compute an ExecResult here and
        // send it only after the caller resumes on the original connection loop.
        const auto exec_start = PromiseResult::clock::now();
        promise->set_reslute(cmd->exec(&cmdctx, args));
        RequestStageMetrics::instance().observe_exec_on_shard(PromiseResult::clock::now() -
                                                              exec_start);
        promise->mark_send_ready();
        asio::post(promise->get_executor(), [promise] { promise->notify(); });
    });
}



auto IdleEngine::register_cmd(const std::string& name, int32_t arity, int32_t first_key,
                              int32_t last_key, Exector exector, Prepare prepare,
                              CmdFlags flags) -> void {
    cmd_map_.emplace(std::piecewise_construct, std::forward_as_tuple(name),
                     std::forward_as_tuple(name, arity, first_key, last_key, exector, prepare,
                                           flags));
}

auto IdleEngine::get_cmd(const std::string& name) -> Cmd* {
    auto it = cmd_map_.find(name);
    if (it == cmd_map_.end()) {
        return nullptr;
    }

    return &it->second;
}

} // namespace idlekv
