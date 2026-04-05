#include "db/command.h"
#include "db/engine.h"
#include "db/client.h"
#include "db/shard.h"
#include "db/storage/result.h"
#include "redis/error.h"
#include "db/transaction.h"

#include <absl/functional/function_ref.h>
#include <boost/fiber/future/future.hpp>
#include <boost/fiber/future/promise.hpp>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

namespace idlekv {

namespace {

auto SingleReadKey(const CmdArgs& args) -> WRSet {
    if (args.size() < 2) {
        return {};
    }
    return {{1}, {}};
}

auto SingleWriteKey(const CmdArgs& args) -> WRSet {
    if (args.size() < 2) {
        return {};
    }
    return {{}, {1}};
}

template <class Fn>
auto Schedule(Shard* shard, ShardId curr_shard_id, Fn&& task) -> decltype(task()) {
    using namespace boost::fibers;

    if (curr_shard_id == shard->GetShardId()) {
        return task();
    } else {
        auto prom = std::make_shared<promise<decltype(task())>>();
        auto fut  = prom->get_future();
        shard->Add([task = std::move(task), prom]() mutable { prom->set_value(task()); });
        return fut.get();
    }
}

} // namespace

auto Set(ExecContext* ctx, CmdArgs& args) -> void {
    auto* sender = ctx->sender;
    Result<bool> res;

    ctx->CurTxn()->Execute([&](Transaction*, Shard* shard) {
        auto* db = shard->DbAt(ctx->db_index);
        res = db->Set(std::string(args[1]), DataEntity::FromString(std::string(args[2])));
    });

    if (!res.Ok()) {
        return sender->SendError(res.Message());
    }
    sender->SendOk();
}

auto Get(ExecContext* ctx, CmdArgs& args) -> void {
    auto* sender = ctx->sender;
    Result<std::shared_ptr<DataEntity>> res;

    ctx->CurTxn()->Execute([&](Transaction*, Shard* shard) {
        auto* db = shard->DbAt(ctx->db_index);
        res = db->Get(args[1]);
    });

    if (res == OpStatus::NoSuchKey) {
        return sender->SendNullBulkString();
    }

    const auto& value = res.Get();
    if (!value) {
        return sender->SendNullBulkString();
    }

    if (!value->IsString()) {
        return sender->SendError(kWrongTypeErr);
    }

    sender->SendBulkString(value->AsString(), value);
}

auto Del(ExecContext* ctx, CmdArgs& args) -> void {
    using namespace boost::fibers;
    auto* sender = ctx->sender;

    Result<bool> res;

    ctx->CurTxn()->Execute([&](Transaction*, Shard* shard) {
        auto* db = shard->DbAt(ctx->db_index);
        res = db->Del(args[1]);
    });

    if (res == OpStatus::NoSuchKey) {
        return sender->SendInteger(0);
    }

    sender->SendInteger(res.Get() ? 1 : 0);
}

auto InitStrings(IdleEngine* eng) -> void {
    eng->RegisterCmd("set", 3, 1, 1, Set, SingleWriteKey, CmdFlags::Transactional);
    eng->RegisterCmd("get", 2, 1, 1, Get, SingleReadKey, CmdFlags::Transactional);
    eng->RegisterCmd("del", 2, 1, 1, Del, SingleWriteKey, CmdFlags::Transactional);
}

} // namespace idlekv
