#include "db/engine.h"
#include "db/result.h"
#include "redis/connection.h"
#include "redis/error.h"

#include <charconv>
#include <string>
#include <vector>

namespace idlekv {

namespace {

auto NoKeys(const std::vector<std::string>& args)
    -> std::pair<std::vector<std::string>, std::vector<std::string>> {
    (void)args;
    return {};
}

auto Ping(CmdContext*, const std::vector<std::string>& args) -> ExecResult {
    switch (args.size()) {
    case 1:
        return ExecResult::Pong();
    case 2:
        return ExecResult::SimpleString(args[1]);
    default:
        return ExecResult::Error("ERR wrong number of arguments for 'ping' command");
    }
}

auto Select(CmdContext* ctx, const std::vector<std::string>& args) -> ExecResult {
    size_t      DbIndex = 0;
    const auto* begin    = args[1].data();
    const auto* end      = begin + args[1].size();
    auto [ptr, ec]       = std::from_chars(begin, end, DbIndex);
    if (ec != std::errc{} || ptr != end) {
        return ExecResult::Error(fmt::format(kProtocolErrFmt, "invalid DB index"));
    }

    if (DbIndex >= engine->DbNum()) {
        return ExecResult::Error("ERR DB index is out of range");
    }

    ctx->GetConnection()->SetDbIndex(DbIndex);
    return ExecResult::Ok();
}

} // namespace

auto InitSystemCmd(IdleEngine* eng) -> void {
    eng->RegisterCmd("ping", -1, -1, -1, Ping, NoKeys, CmdFlags::CanExecInline);
    eng->RegisterCmd("select", 2, -1, -1, Select, NoKeys, CmdFlags::CanExecInline);
}

} // namespace idlekv
