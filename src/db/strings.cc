#include "common/result.h"
#include "db/command.h"
#include "db/context.h"
#include "db/engine.h"
#include "db/shard.h"
#include "db/storage/result.h"
#include "redis/connection.h"
#include "redis/error.h"

#include <absl/functional/function_ref.h>
#include <boost/fiber/future/future.hpp>
#include <boost/fiber/future/promise.hpp>
#include <functional>
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

template<class Fn>
auto Schedule(Shard* shard, ShardId curr_shard_id, Fn&& task) -> decltype(task()) {
    using namespace boost::fibers;

    if (curr_shard_id == shard->GetShardId()) {
        return task();
    } else {
        auto prom = std::make_shared<promise<decltype(task())>>();
        auto fut  = prom->get_future();
        shard->Add([task = std::move(task), prom]() mutable {
            prom->set_value(task());
        });
        return fut.get();
    }
}

} // namespace

auto Set(ExecContext* ctx, CmdArgs& args) -> void {
    auto& sender = ctx->GetConnection()->GetSender();

    DB*  db   = ctx->GetDb();

    auto res = Schedule(ctx->GetShard(), ctx->OwnerId(),  [db, ags = std::move(args)]() mutable {
        return db->Set(std::string(ags[1]), DataEntity::FromString(std::string(ags[2])));
    });

    if (!res.Ok()) {
        return sender.SendError(res.Message());
    }
    sender.SendOk();
}

auto Get(ExecContext* ctx, CmdArgs& args) -> void {
    auto& sender = ctx->GetConnection()->GetSender();
    DB*   db     = ctx->GetDb();

    auto res = Schedule(
        ctx->GetShard(), ctx->OwnerId(), [db, ags = std::move(args)]() mutable {
            return db->Get(ags[1]);
        });

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
    DB*   db     = ctx->GetDb();

    auto res =Schedule(
        ctx->GetShard(), ctx->OwnerId(), [db, ags = std::move(args)]() mutable {
            return db->Del(ags[1]);
        });

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
