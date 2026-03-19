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

auto SingleReadKey(const std::vector<std::string>& args)
    -> std::pair<std::vector<std::string>, std::vector<std::string>> {
    if (args.size() < 2) {
        return {};
    }
    return {{}, {args[1]}};
}

auto SingleWriteKey(const std::vector<std::string>& args)
    -> std::pair<std::vector<std::string>, std::vector<std::string>> {
    if (args.size() < 2) {
        return {};
    }
    return {{args[1]}, {}};
}

} // namespace

auto Set(CmdContext* ctx, const std::vector<std::string>& args) -> ExecResult {
    auto res = ctx->GetDb()->Set(args[1], DataEntity::FromString(std::move(args[2])));
    if (!res.Ok()) {
        return ExecResult::Error(kStandardErr);
    }

    return ExecResult::Ok();
}

auto Get(CmdContext* ctx, const std::vector<std::string>& args) -> ExecResult {
    auto res = ctx->GetDb()->Get(args[1]);
    if (res == OpStatus::NoSuchKey) {
        return ExecResult::Null();
    }

    const auto& value = res.Get();
    if (!value) {
        return ExecResult::Null();
    }

    if (!value->IsString()) {
        return ExecResult::Error(kWrongTypeErr);
    }

    return ExecResult::BulkString(value);
}

auto Del(CmdContext* ctx, const std::vector<std::string>& args) -> ExecResult {
    auto res = ctx->GetDb()->Del(args[1]);
    if (res == OpStatus::NoSuchKey) {
        return ExecResult::Integer(0);
    }

    return ExecResult::Integer(res.Get() ? 1 : 0);
}

auto InitStrings(IdleEngine* eng) -> void {
    eng->RegisterCmd("set", 3, 1, 1, Set, SingleWriteKey);
    eng->RegisterCmd("get", 2, 1, 1, Get, SingleReadKey);
    eng->RegisterCmd("del", 2, 1, 1, Del, SingleWriteKey);
}

} // namespace idlekv
