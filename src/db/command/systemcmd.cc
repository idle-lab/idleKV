#include "db/command.h"
#include "db/engine.h"
#include "db/context.h"
#include "redis/error.h"

#include <charconv>
#include <string>

namespace idlekv {

namespace {

auto NoKeys(const CmdArgs& args) -> WRSet {
    (void)args;
    return {};
}

auto Ping(ExecContext* ctx, CmdArgs& args) -> void {
    auto* sender = ctx->sender;
    switch (args.size()) {
    case 1:
        return sender->SendPong();
    case 2:
        return sender->SendSimpleString(args[1]);
    default:
        return sender->SendError("ERR wrong number of arguments for 'ping' command");
    }
}

auto Select(ExecContext* ctx, CmdArgs& args) -> void {
    auto* sender = ctx->sender;
    size_t      db_index = 0;
    const auto* begin   = args[1].data();
    const auto* end     = begin + args[1].size();
    auto [ptr, ec]      = std::from_chars(begin, end, db_index);
    if (ec != std::errc{} || ptr != end) {
        return sender->SendError(fmt::format(kProtocolErrFmt, "invalid DB index"));
    }

    if (db_index >= engine->DbNum()) {
        return sender->SendError("ERR DB index is out of range");
    }

    ctx->db_index = db_index;
    sender->SendOk();
}

} // namespace

auto InitSystemCmd(IdleEngine* eng) -> void {
    eng->RegisterCmd("ping", -1, -1, -1, Ping, NoKeys, CmdFlags::CanExecInPlace | CmdFlags::NoKey);
    eng->RegisterCmd("select", 2, -1, -1, Select, NoKeys,
                     CmdFlags::CanExecInPlace | CmdFlags::NoKey | CmdFlags::StateChange);
}

} // namespace idlekv
