#include "db/command.h"
#include "db/engine.h"
#include "redis/connection.h"
#include "redis/error.h"

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

auto Set(ExecContext* ctx, std::vector<std::string>& args) -> void {
    auto& sender = ctx->GetConnection()->GetSender();
    auto  res    = ctx->GetDb()->Set(args[1], DataEntity::FromString(std::move(args[2])));
    if (!res.Ok()) {
        return sender.SendError(res.Message());
    }

    sender.SendOk();
}

auto Get(ExecContext* ctx, std::vector<std::string>& args) -> void {
    auto& sender = ctx->GetConnection()->GetSender();
    auto  res    = ctx->GetDb()->Get(args[1]);
    if (res == OpStatus::NoSuchKey) {
        sender.SendNullBulkString();
    }

    const auto& value = res.Get();
    if (!value) {
        return sender.SendNullBulkString();
    }

    if (!value->IsString()) {
        return sender.SendError(kWrongTypeErr);
    }

    sender.SendBulkString(value);
}

auto Del(ExecContext* ctx, std::vector<std::string>& args) -> void {
    auto& sender = ctx->GetConnection()->GetSender();
    auto  res    = ctx->GetDb()->Del(args[1]);
    if (res == OpStatus::NoSuchKey) {
        return sender.SendInteger(0);
    }

    sender.SendInteger(res.Get() ? 1 : 0);
}

auto InitStrings(IdleEngine* eng) -> void {
    eng->RegisterCmd("set", 3, 1, 1, Set, SingleWriteKey);
    eng->RegisterCmd("get", 2, 1, 1, Get, SingleReadKey);
    eng->RegisterCmd("del", 2, 1, 1, Del, SingleWriteKey);
}

} // namespace idlekv
