#include "common/result.h"
#include "db/command.h"
#include "db/engine.h"
#include "db/storage/result.h"
#include "redis/connection.h"
#include "redis/error.h"

#include <boost/fiber/future/future.hpp>
#include <boost/fiber/future/promise.hpp>
#include <memory>
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
    using namespace boost::fibers;
    auto& sender = ctx->GetConnection()->GetSender();


    auto prom = std::make_shared<promise<Result<bool>>>();
    auto fut = prom->get_future();
    DB* db = ctx->GetDb();

    ctx->GetShard()->Add([prom, db, key = std::move(args[1]), value = std::move(args[2])]() mutable {
        prom->set_value(db->Set(std::move(key), DataEntity::FromString(std::move(value))));
    });

    auto res = fut.get();
    if (!res.Ok()) {
        return sender.SendError(res.Message());
    }
    sender.SendOk();
}

auto Get(ExecContext* ctx, std::vector<std::string>& args) -> void {
    using namespace boost::fibers;
    auto& sender = ctx->GetConnection()->GetSender();
    auto prom = std::make_shared<promise<Result<std::shared_ptr<DataEntity>>>>();
    auto fut = prom->get_future();
    DB* db = ctx->GetDb();

    ctx->GetShard()->Add([db, prom, value = std::move(args[1])] {
        prom->set_value(db->Get(value));
    });

    auto res = fut.get();
    if (res == OpStatus::NoSuchKey) {
        return sender.SendNullBulkString();
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
