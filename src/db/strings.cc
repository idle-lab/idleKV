#include "common/result.h"
#include "db/command.h"
#include "db/context.h"
#include "db/engine.h"
#include "db/storage/result.h"
#include "redis/connection.h"
#include "redis/error.h"

#include <boost/fiber/future/future.hpp>
#include <boost/fiber/future/promise.hpp>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace idlekv {

namespace {

auto SingleReadKey(const CmdArgs& args)
    -> std::pair<std::vector<std::string_view>, std::vector<std::string_view>> {
    if (args.size() < 2) {
        return {};
    }
    return {{}, {args[1]}};
}

auto SingleWriteKey(const CmdArgs& args)
    -> std::pair<std::vector<std::string_view>, std::vector<std::string_view>> {
    if (args.size() < 2) {
        return {};
    }
    return {{args[1]}, {}};
}

} // namespace

auto Set(ExecContext* ctx, CmdArgs& args) -> void {
    using namespace boost::fibers;
    auto& sender = ctx->GetConnection()->GetSender();

    auto prom = std::make_shared<promise<Result<bool>>>();
    auto fut  = prom->get_future();
    DB*  db   = ctx->GetDb();

    ctx->GetShard()->Add(
    [prom, db, ags = std::move(args)]() mutable {
            prom->set_value(db->Set(std::string(ags[1]), DataEntity::FromString(std::string(ags[2]))));
    });

    auto res = fut.get();
    if (!res.Ok()) {
        return sender.SendError(res.Message());
    }
    sender.SendOk();
}

auto Get(ExecContext* ctx, CmdArgs& args) -> void {
    using namespace boost::fibers;
    auto& sender = ctx->GetConnection()->GetSender();
    auto  prom   = std::make_shared<promise<Result<std::shared_ptr<DataEntity>>>>();
    auto  fut    = prom->get_future();
    DB*   db     = ctx->GetDb();

    ctx->GetShard()->Add(
        [db, prom, ags = std::move(args)] {
            prom->set_value(db->Get((ags[1]))); 
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

auto Del(ExecContext* ctx, CmdArgs& args) -> void {
    using namespace boost::fibers;
    auto& sender = ctx->GetConnection()->GetSender();
    auto  prom   = std::make_shared<promise<Result<bool>>>();
    auto  fut    = prom->get_future();
    DB*   db     = ctx->GetDb();

    ctx->GetShard()->Add(
        [db, prom, ags = std::move(args)] {
            prom->set_value(db->Del(ags[1])); 
    });

    auto res = fut.get();
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
