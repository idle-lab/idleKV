#include "db/engine.h"
#include "db/result.h"
#include "redis/connection.h"
#include "redis/error.h"

#include <charconv>
#include <string>
#include <vector>

namespace idlekv {

namespace {

auto no_keys(const std::vector<std::string>& args)
    -> std::pair<std::vector<std::string>, std::vector<std::string>> {
    (void)args;
    return {};
}

auto ping(CmdContext*, const std::vector<std::string>& args) -> ExecResult {
    switch (args.size()) {
    case 1:
        return ExecResult::pong();
    case 2:
        return ExecResult::simple_string(args[1]);
    default:
        return ExecResult::error("ERR wrong number of arguments for 'ping' command");
    }
}

auto select(CmdContext* ctx, const std::vector<std::string>& args) -> ExecResult {
    size_t      db_index = 0;
    const auto* begin    = args[1].data();
    const auto* end      = begin + args[1].size();
    auto [ptr, ec]       = std::from_chars(begin, end, db_index);
    if (ec != std::errc{} || ptr != end) {
        return ExecResult::error(fmt::format(kProtocolErrFmt, "invalid DB index"));
    }

    if (db_index >= engine->db_num()) {
        return ExecResult::error("ERR DB index is out of range");
    }

    ctx->connection()->set_db_index(db_index);
    return ExecResult::ok();
}

} // namespace

auto init_systemcmd(IdleEngine* eng) -> void {
    eng->register_cmd("ping", -1, -1, -1, ping, no_keys, CmdFlags::CanExecInline);
    eng->register_cmd("select", 2, -1, -1, select, no_keys, CmdFlags::CanExecInline);
}

} // namespace idlekv
