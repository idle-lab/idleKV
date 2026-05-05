#include "db/command.h"
#include "db/command/base.h"
#include "db/context.h"
#include "db/engine.h"
#include "db/shard.h"
#include "db/storage/result.h"
#include "db/storage/value.h"
#include "db/transaction.h"
#include "redis/error.h"

#include <numeric>
#include <optional>
#include <spdlog/fmt/bundled/format.h>
#include <string>
#include <string_view>
#include <vector>

namespace idlekv {

namespace {

auto SendArgNumErr(SenderBase* sender, std::string_view cmd_name) -> void {
    sender->SendError(fmt::format(kArgNumErrFmt, cmd_name));
}

auto MSetKeys(const CmdArgs& args) -> WRSet {
    WRSet keys;
    if (args.size() < 2) {
        return keys;
    }

    keys.write_keys.reserve(args.size() / 2);
    for (size_t i = 1; i < args.size(); i += 2) {
        keys.write_keys.push_back(i);
    }
    return keys;
}

} // namespace

auto Set(ExecContext* ctx, CmdArgs& args) -> void {
    auto*        sender = ctx->sender;
    Result<void> res;

    ctx->CurTxn()->Execute([&](Transaction*, Shard* shard) {
        auto* db = shard->DbAt(ctx->db_index);

        res = db->Set(args[1], MakeValue<Value::STR>(args[2]));
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
        res      = db->Get(args[1], Value::STR);
    });

    if (res == OpStatus::NoSuchKey) {
        return sender->SendNullBulkString();
    }

    if (!res.Ok()) {
        return sender->SendError(res.Message());
    }

    sender->SendBulkString(res.payload->GetString(), res.payload);
}

auto Del(ExecContext* ctx, CmdArgs& args) -> void {
    auto*                sender = ctx->sender;
    std::vector<int64_t> removed(engine->ShardNum(), 0);

    ctx->CurTxn()->Execute([&](Transaction* txn, Shard* shard) {
        auto*   db       = shard->DbAt(ctx->db_index);
        ShardId shard_id = shard->Id();

        for (uint32_t arg_index : txn->GetShardArgs(shard_id)) {
            auto res = db->Del(args[arg_index]);
            if (res.Ok()) {
                removed[shard_id]++;
                continue;
            }

            CHECK(res == OpStatus::NoSuchKey);
        }
    });

    sender->SendInteger(std::accumulate(removed.begin(), removed.end(), int64_t{0}));
}

auto MGet(ExecContext* ctx, CmdArgs& args) -> void {
    auto* sender = ctx->sender;

    std::vector<std::optional<std::string>> values(args.size() - 1);
    std::vector<OpStatus>                   shard_status(engine->ShardNum(), OpStatus::OK);

    ctx->CurTxn()->Execute([&](Transaction* txn, Shard* shard) {
        auto*   db       = shard->DbAt(ctx->db_index);
        ShardId shard_id = shard->Id();

        for (uint32_t arg_index : txn->GetShardArgs(shard_id)) {
            auto res = db->Get(args[arg_index], Value::STR);
            if (res == OpStatus::NoSuchKey) {
                continue;
            }

            if (!res.Ok()) {
                shard_status[shard_id] = res.status;
                return;
            }
            values[arg_index - 1] = std::string(res.payload->GetString());
        }
    });

    for (auto status : shard_status) {
        if (status != OpStatus::OK) {
            return sender->SendError(OpStatusToString(status));
        }
    }

    sender->SendBulkStringArray(std::move(values));
}

auto MSet(ExecContext* ctx, CmdArgs& args) -> void {
    auto* sender = ctx->sender;
    if ((args.size() & 1U) == 0) {
        return SendArgNumErr(sender, "mset");
    }

    ctx->CurTxn()->Execute([&](Transaction* txn, Shard* shard) {
        auto*   db       = shard->DbAt(ctx->db_index);
        ShardId shard_id = shard->Id();

        for (uint32_t arg_index : txn->GetShardArgs(shard_id)) {
            auto res = db->Set(args[arg_index], MakeValue<Value::STR>(args[arg_index + 1]));
            CHECK(res.Ok());
        }
    });

    sender->SendOk();
}

auto InitStrings(IdleEngine* eng) -> void {
    eng->RegisterCmd("set", 3, 1, 1, Set, SingleWriteKey, CmdFlags::Transactional);
    eng->RegisterCmd("get", 2, 1, 1, Get, SingleReadKey, CmdFlags::Transactional);
    eng->RegisterCmd("del", -2, 1, 1, Del, MultiWriteKeys, CmdFlags::Transactional);
    eng->RegisterCmd("mget", -2, 1, 1, MGet, MultiReadKeys, CmdFlags::Transactional);
    eng->RegisterCmd("mset", -3, 1, 1, MSet, MSetKeys, CmdFlags::Transactional);
}

} // namespace idlekv
