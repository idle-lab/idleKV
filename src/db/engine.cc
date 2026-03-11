#include "db/engine.h"

#include "redis/connection.h"
#include "redis/error.h"

#include <cstdint>
#include <tuple>
#include <utility>

namespace idlekv {

auto IdleEngine::init_command() -> void {
    init_systemcmd(this);
    init_strings(this);
    init_hash(this);
    init_list(this);
}

auto IdleEngine::exec(Connection* conn, const std::vector<std::string>& args) noexcept
    -> ExecResult {

    auto cmd = get_cmd(args[0]);
    if (cmd == nullptr) {
        return ExecResult(ExecResult::Status::ERR, fmt::format(kUnknownCmdErrFmt, args[0]));
    }

    // if (!cmd->verification(args)) {
    //     return ArgNumErr::make_reply(cmd_name);
    // }

    // auto [ws, rs] = cmd->prepare(args);

    return cmd->exec(conn, args);
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
