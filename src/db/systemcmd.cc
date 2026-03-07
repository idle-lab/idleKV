#include "db/context.h"
#include "db/db.h"
#include "db/engine.h"
#include "redis/connection.h"
#include "redis/protocol/reply.h"
#include <string>

namespace idlekv {

auto ping(Context& ctx, const std::vector<std::string>& args) -> std::string {
    switch (args.size()) {
        case 1:
            return PongReply::make_reply();
        case 2:
            return SimpleString::make_reply(args[0]); 
        default:
            return ArgNumErr::make_reply(args[0]);
    }
}

auto select(Context& ctx, const std::vector<std::string>& args) -> std::string {}

auto init_systemcmd(IdleEngine* eng) -> void { eng->register_cmd("ping", -1, 0, 0, ping, nullptr); }

} // namespace idlekv
