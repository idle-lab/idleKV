#include "common/asio_no_exceptions.h"
#include "db/engine.h"

#include "common/logger.h"
#include "db/command.h"
#include "db/result.h"
#include "db/shard.h"
#include "redis/connection.h"
#include "redis/error.h"
#include "server/el_pool.h"
#include "server/thread_state.h"

#include <asio/as_tuple.hpp>
#include <asio/awaitable.hpp>
#include <asio/steady_timer.hpp>
#include <asio/use_future.hpp>
#include <asiochan/channel.hpp>
#include <cstddef>
#include <cstdint>
#include <exception>
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

auto IdleEngine::exec(Connection* conn, const std::vector<std::string>& args) noexcept
    -> asio::awaitable<void> {
    auto cmd = get_cmd(args[0]);
    if (cmd == nullptr) {
        co_await conn->sender().send_error(fmt::format(kUnknownCmdErrFmt, args[0]));
        co_return ;
    }

    if (!cmd->verification(args)) {
        co_await conn->sender().send_error(fmt::format(kArgNumErrFmt, cmd->name()));
        co_return ;
    }

    ShardId shard_id = ThreadState::tlocal()->pool_index();

    if (cmd->first_key() != -1) {
        // now only support single-key command, so directly check if the first key is a key
        if (args.size() <= static_cast<size_t>(cmd->first_key())) {
            co_await conn->sender().send_error(fmt::format(kArgNumErrFmt, cmd->name()));
            co_return ;
        }

        shard_id = calculate_shard_id(args[cmd->first_key()]);
    }

    LOG(debug, "should exec cmd in shard: {}", shard_id);
    auto cmdctx = CmdContext(conn, shard_set_[shard_id]->db_at(conn->db_index()).get());

    asiochan::channel<ExecResult> res_ch;
    LOG(debug, "dispatch cmd: {}", cmd->name());

    shard_set_[shard_id]->dispatch([&cmdctx, cmd, &args, &res_ch] () -> asio::awaitable<void> {
        // Execute the command on the shard's worker loop, but do not write the reply here.
        // Connection/Sender owns socket and write-buffer state that belongs to the connection's
        // event loop, so touching sender from a shard loop would cross thread boundaries and can
        // race with other connection-side operations. We therefore compute an ExecResult here and
        // send it only after the caller resumes on the original connection loop.
        LOG(debug, "[{}] start cmd: {}", ThreadState::tlocal()->pool_index(), cmd->name());
        co_await res_ch.write(cmd->exec(&cmdctx, args));
    });

    LOG(debug, "wait cmd: {}", cmd->name());
    auto res = co_await res_ch.read();
    LOG(debug, "finish cmd: {}", cmd->name());

    switch (res.type()) {
    case ExecResult::kOk:
        co_await conn->sender().send_ok();
        break;
    case ExecResult::kSimpleString:
        co_await conn->sender().send_simple_string(res.string());
        break;
    case ExecResult::kBulkString:
        co_await conn->sender().send_bulk_string(res.string());
        break;
    case ExecResult::kNull:
        co_await conn->sender().send_null_bulk_string();
        break;
    case ExecResult::kInteger:
        co_await conn->sender().send_integer(res.integer());
        break;
    case ExecResult::kError:
        co_await conn->sender().send_error(res.string());
        break;
    }
}

auto IdleEngine::register_cmd(const std::string& name, int32_t arity, int32_t first_key,
                              int32_t last_key, Exector exector, Prepare prepare) -> void {
    cmd_map_.emplace(std::piecewise_construct, std::forward_as_tuple(name),
                     std::forward_as_tuple(name, arity, first_key, last_key, exector, prepare));
}

auto IdleEngine::get_cmd(const std::string& name) -> Cmd* {
    auto it = cmd_map_.find(name);
    if (it == cmd_map_.end()) {
        return nullptr;
    }

    return &it->second;
}

} // namespace idlekv
