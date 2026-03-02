#include "db/engine.h"
#include "db/context.h"
#include "redis/protocol/error.h"

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

auto to_lower(std::string s) -> std::string {
    std::transform(s.begin(), s.end(), s.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return s;
}



auto IdleEngine::exec(Context& ctx,
                      const std::vector<std::string>& args) noexcept
    -> std::string {
    auto cmd_name = to_lower(args[0]);

    auto cmd = get_cmd(cmd_name);
    if (cmd == nullptr) {
        return UnknownCmdErr::make_reply(cmd_name);
    }

    if (!cmd->verification(args)) {
        return ArgNumErr::make_reply(cmd_name);
    }

    // auto [ws, rs] = cmd->prepare(args);

    return cmd->exec(ctx, args);
}

auto IdleEngine::select_db(size_t idx) -> std::shared_ptr<DB> {
    if (idx >= db_set_.size()) {
        return nullptr;
    }
    return db_set_[idx];
}

auto IdleEngine::register_cmd(const std::string& name, int32_t arity, int32_t first_key, int32_t last_key, Exector exector,
                              Prepare prepare) -> void {
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
