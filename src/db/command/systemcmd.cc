#include "db/command.h"
#include "db/context.h"
#include "db/engine.h"
#include "db/info.h"
#include "redis/error.h"

#include <charconv>
#include <string>

namespace idlekv {

namespace {

auto NoKeys(const CmdArgs& args) -> WRSet {
    (void)args;
    return {};
}

auto SendArgNumErr(SenderBase* sender, std::string_view cmd_name) -> void {
    sender->SendError(fmt::format(kArgNumErrFmt, cmd_name));
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

auto Echo(ExecContext* ctx, CmdArgs& args) -> void {
    auto* sender = ctx->sender;
    if (args.size() != 2) {
        return SendArgNumErr(sender, "echo");
    }

    sender->SendBulkString(std::string(args[1]));
}

auto Select(ExecContext* ctx, CmdArgs& args) -> void {
    auto*       sender   = ctx->sender;
    size_t      db_index = 0;
    const auto* begin    = args[1].data();
    const auto* end      = begin + args[1].size();
    auto [ptr, ec]       = std::from_chars(begin, end, db_index);
    if (ec != std::errc{} || ptr != end) {
        return sender->SendError(fmt::format(kProtocolErrFmt, "invalid DB index"));
    }

    if (db_index >= engine->DbNum()) {
        return sender->SendError("ERR DB index is out of range");
    }

    ctx->db_index = db_index;
    sender->SendOk();
}

auto Info(ExecContext* ctx, CmdArgs& args) -> void {
    auto* sender = ctx->sender;
    if (args.size() > 2) {
        return SendArgNumErr(sender, "info");
    }

    if (args.size() == 2 && ResolveInfoSection(args[1]) == InfoSection::Unsupported) {
        return sender->SendError("ERR unsupported INFO section");
    }

    sender->SendBulkString(FormatInfoMemorySection(CollectInfoMemoryStats(engine.get())));
}

} // namespace

auto InitSystemCmd(IdleEngine* eng) -> void {
    eng->RegisterCmd("echo", 2, -1, -1, Echo, NoKeys, CmdFlags::CanExecInPlace | CmdFlags::NoKey);
    eng->RegisterCmd("info", -1, -1, -1, Info, NoKeys, CmdFlags::CanExecInPlace | CmdFlags::NoKey);
    eng->RegisterCmd("ping", -1, -1, -1, Ping, NoKeys, CmdFlags::CanExecInPlace | CmdFlags::NoKey);
    eng->RegisterCmd("select", 2, -1, -1, Select, NoKeys,
                     CmdFlags::CanExecInPlace | CmdFlags::NoKey | CmdFlags::StateChange);
}

} // namespace idlekv
