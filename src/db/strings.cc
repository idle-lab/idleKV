#include "db/command.h"
#include "db/engine.h"
#include "db/result.h"
#include "db/storage/kvstore.h"
#include "redis/connection.h"
#include "redis/error.h"
#include "server/thread_state.h"

#include <string>
#include <utility>
#include <vector>

namespace idlekv {

namespace {

auto single_read_key(const std::vector<std::string>& args)
    -> std::pair<std::vector<std::string>, std::vector<std::string>> {
    if (args.size() < 2) {
        return {};
    }
    return {{}, {args[1]}};
}

auto single_write_key(const std::vector<std::string>& args)
    -> std::pair<std::vector<std::string>, std::vector<std::string>> {
    if (args.size() < 2) {
        return {};
    }
    return {{args[1]}, {}};
}

} // namespace

auto set(CmdContext* ctx, const std::vector<std::string>& args) -> ExecResult {
    auto res = ctx->db()->set(args[1], DataEntity::from_string(args[2]));
    if (!res.ok()) {
        return ExecResult::error(kStandardErr);
    }

    return ExecResult::ok();
}

auto get(CmdContext* ctx, const std::vector<std::string>& args) -> ExecResult {
    auto res = ctx->db()->get(args[1]);
    if (!res.ok()) {
        return ExecResult::error(kStandardErr);
    }

    const auto& value = res.get();
    if (!value) {
        return ExecResult::null();
    }

    if (!value->is_string()) {
        return ExecResult::error(kWrongTypeErr);
    }

    return ExecResult::bulk_string(value->as_string());
}

auto del(CmdContext* ctx, const std::vector<std::string>& args) -> ExecResult {
    auto res = ctx->db()->del(args[1]);
    if (!res.ok()) {
        return ExecResult::error(kStandardErr);
    }

    return ExecResult::integer(res.get() ? 1 : 0);
}

auto init_strings(IdleEngine* eng) -> void {
    eng->register_cmd("set", 3, 1, 1, set, single_write_key);
    eng->register_cmd("get", 2, 1, 1, get, single_read_key);
    eng->register_cmd("del", 2, 1, 1, del, single_write_key);
}

} // namespace idlekv
