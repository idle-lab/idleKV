#include "db/context.h"
#include "db/db.h"
#include "db/engine.h"
#include "redis/connection.h"
#include "redis/protocol/reply.h"

namespace idlekv {

auto ping(Context& ctx, std::vector<std::string>& args) -> void {
    switch (args.size()) {
        case 1:
            ctx.sender().send_pong();
        case 2:
            ctx.sender().send_simple_string(std::move(args[1]));
        // default:
        //     return ArgNumErr::make_reply(args[0]);
    }
}

auto select(Context& ctx, const std::vector<std::string>& args) -> std::string {}

auto init_systemcmd(IdleEngine* eng) -> void { eng->register_cmd("ping", -1, 0, 0, ping, nullptr); }

} // namespace idlekv
