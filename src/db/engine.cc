#include "db/engine.h"

#include <tuple>
#include <utility>

namespace idlekv {

auto IdleEngine::init_command() -> void {}

auto IdleEngine::exec(std::shared_ptr<Connection>     conn,
                      const std::vector<std::string>& args) noexcept
    -> asio::awaitable<std::string> {
    auto cmd = get_cmd(args[0]);
    if (cmd == nullptr) {
        co_return UnknownCmdErr::make_reply(args[0]);
    }

    if (cmd->is_systemcmd()) {
        co_return cmd->exec(nullptr, conn, args)->to_bytes();
    }

    auto db = select_db(conn->db_index());
    // auto [ws, rs] = cmd->prepare(args);
    co_return cmd->exec(db, nullptr, args)->to_bytes();
}

auto IdleEngine::select_db(size_t idx) -> std::shared_ptr<DB> {
    if (idx < 0 || idx >= db_set_.size()) {
        return nullptr;
    }
    return db_set_[idx];
}

auto IdleEngine::register_cmd(const std::string& name, int32_t arity, bool is_sys, Exector exector,
                              Prepare prepare) -> void {
    cmd_map_.emplace(std::piecewise_construct, std::forward_as_tuple(name),
                     std::forward_as_tuple(name, arity, is_sys, exector, prepare));
}

auto IdleEngine::get_cmd(const std::string& name) -> Cmd* {
    auto it = cmd_map_.find(name);
    if (it == cmd_map_.end()) {
        return nullptr;
    }

    return &it->second;
}


} // namespace idlekv
