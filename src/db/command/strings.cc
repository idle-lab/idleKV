#include "db/command.h"
#include "db/command/base.h"
#include "db/context.h"
#include "db/engine.h"
#include "db/shard.h"
#include "db/storage/result.h"
#include "db/storage/value.h"
#include "db/transaction.h"
#include "redis/error.h"

#include <absl/functional/function_ref.h>
#include <boost/fiber/future/future.hpp>
#include <boost/fiber/future/promise.hpp>
#include <string>
#include <string_view>

namespace idlekv {

auto Set(ExecContext* ctx, CmdArgs& args) -> void {
    auto*        sender = ctx->sender;
    Result<void> res;

    ctx->CurTxn()->Execute([&](Transaction*, Shard* shard) {
        auto* db = shard->DbAt(ctx->db_index);

        res = db->Set(std::string(args[1]), MakeValue<Value::STR_TAG>(args[2]));
    });

    if (!res.Ok()) {
        return sender->SendError(res.Message());
    }
    sender->SendOk();
}

auto Get(ExecContext* ctx, CmdArgs& args) -> void {
    auto*              sender = ctx->sender;
    Result<PrimeValue> res;

    ctx->CurTxn()->Execute([&](Transaction*, Shard* shard) {
        auto* db = shard->DbAt(ctx->db_index);
        res      = db->Get(args[1]);
    });

    if (res == OpStatus::NoSuchKey) {
        return sender->SendNullBulkString();
    }

    if (!res.payload->IsStr()) {
        return sender->SendError(kWrongTypeErr);
    }

    sender->SendBulkString(res.payload->GetString(), res.payload);
}

auto Del(ExecContext* ctx, CmdArgs& args) -> void {
    using namespace boost::fibers;
    auto* sender = ctx->sender;

    Result<void> res;

    ctx->CurTxn()->Execute([&](Transaction*, Shard* shard) {
        auto* db = shard->DbAt(ctx->db_index);
        res      = db->Del(args[1]);
    });

    sender->SendInteger(res == OpStatus::NoSuchKey ? 0 : 1);
}

auto InitStrings(IdleEngine* eng) -> void {
    eng->RegisterCmd("set", 3, 1, 1, Set, SingleWriteKey, CmdFlags::Transactional);
    eng->RegisterCmd("get", 2, 1, 1, Get, SingleReadKey, CmdFlags::Transactional);
    eng->RegisterCmd("del", 2, 1, 1, Del, SingleWriteKey, CmdFlags::Transactional);
}

} // namespace idlekv
