#include "db/engine.h"

#include "common/asio_no_exceptions.h"
#include "db/command.h"
#include "db/result.h"
#include "db/storage/alloctor.h"
#include "redis/connection.h"
#include "redis/error.h"
#include "server/el_pool.h"
#include "server/thread_state.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <spdlog/spdlog.h>
#include <boost/fiber/buffered_channel.hpp>
#include <tuple>
#include <utility>
#include <xxhash.h>

namespace idlekv {

std::unique_ptr<IdleEngine> engine = nullptr;

IdleEngine::IdleEngine(const Config& cfg) : db_num_(cfg.db_num_) {}

auto IdleEngine::Init(EventLoopPool* elp) -> void {
    InitCommand();
    ebr_mgr_ = std::make_unique<EBRManager>();
    ebr_mgr_->Init(elp);
    for (size_t i = 0; i < db_num_; ++i) {
        db_slice_.emplace_back(std::make_shared<DB>());
    }
}

auto IdleEngine::InitCommand() -> void {
    InitSystemCmd(this);
    InitStrings(this);
    InitHash(this);
    InitList(this);
}


auto IdleEngine::DispatchCmd(Connection* conn, std::vector<std::string>& args) noexcept -> void {
    size_t id = ThreadState::Tlocal()->PoolIndex();
    auto& sender = conn->GetSender();

    auto cmd = GetCmd(args[0]);
    if (cmd == nullptr) {
        return sender.SendError(fmt::format(kUnknownCmdErrFmt, args[0]));
    }

    if (!cmd->Verification(args)) {
        return sender.SendError(fmt::format(kArgNumErrFmt, cmd->Name()));
    }

    if (cmd->CanExecInline()) {
        CmdContext cmdctx(conn, nullptr, id);
        cmd->Exec(&cmdctx, args);
        return;
    }

    // asio::steady_timer timer();
    // timer.expires_at(std::chrono::steady_clock::time_point::max());
    // works_.Post([&](){
        // timer.cancel();
    // });

    auto db_ptr = DbAt(conn->DbIndex());
    CmdContext cmdctx(conn, db_ptr, 0);

    cmd->Exec(&cmdctx, args);
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
