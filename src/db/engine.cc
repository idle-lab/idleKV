#include "db/engine.h"
#include "redis/protocol/error.h"

#include <tuple>
#include <utility>

namespace idlekv {

auto IdleEngine::init_command() -> void {
    init_systemcmd(this);
    init_strings(this);
    init_hash(this);
    init_list(this);
}

auto to_lower(std::string s) -> std::string {
    std::transform(s.begin(), s.end(), s.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return s;
}


auto IdleEngine::exec(std::shared_ptr<Connection>     conn,
                      const std::vector<std::string>& args) noexcept
    -> asio::awaitable<std::string> {

    auto cmd_name = to_lower(args[0]);
    auto cmd = get_cmd(cmd_name);
    if (cmd == nullptr) {
        co_return UnknownCmdErr::make_reply(cmd_name);
    }

    if (!cmd->verification(args)) {
        co_return ArgNumErr::make_reply(cmd_name);
    }

    if (cmd->is_systemcmd()) {
        co_return cmd->exec(nullptr, conn, args);
    }

    auto db = select_db(conn->db_index());
    // auto [ws, rs] = cmd->prepare(args);
    co_return cmd->exec(db, conn, args);
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
