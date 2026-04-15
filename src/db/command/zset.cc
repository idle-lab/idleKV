#include "common/config.h"
#include "db/command.h"
#include "db/command/base.h"
#include "db/context.h"
#include "db/engine.h"
#include "db/shard.h"
#include "db/storage/result.h"
#include "db/storage/value.h"
#include "db/transaction.h"
#include "redis/error.h"

#include <boost/type_traits/integral_constant.hpp>
#include <cctype>
#include <cerrno>
#include <charconv>
#include <cmath>
#include <cstdlib>
#include <string>
#include <string_view>
#include <vector>

namespace idlekv {

namespace {

constexpr std::string_view kInvalidFloatErr = "ERR value is not a valid float";
constexpr std::string_view kInvalidIntErr   = "ERR value is not an integer or out of range";

struct ZAddEntry {
    double           score{0};
    std::string_view member;
};

auto SendArgNumErr(SenderBase* sender, std::string_view cmd_name) -> void {
    sender->SendError(fmt::format(kArgNumErrFmt, cmd_name));
}

auto ParseInt64(std::string_view raw, int64_t* out) -> bool {
    const char* begin = raw.data();
    const char* end   = raw.data() + raw.size();
    auto [ptr, ec]    = std::from_chars(begin, end, *out);
    return ec == std::errc{} && ptr == end;
}

auto ParseDouble(std::string_view raw, double* out) -> bool {
    std::string owned(raw);
    char*       end = nullptr;

    errno = 0;
    *out  = std::strtod(owned.c_str(), &end);
    return errno == 0 && end == owned.data() + owned.size() && !std::isnan(*out);
}

auto EqualsIgnoreCase(std::string_view lhs, std::string_view rhs) -> bool {
    if (lhs.size() != rhs.size()) {
        return false;
    }

    for (size_t i = 0; i < lhs.size(); ++i) {
        if (std::tolower(static_cast<unsigned char>(lhs[i])) !=
            std::tolower(static_cast<unsigned char>(rhs[i]))) {
            return false;
        }
    }
    return true;
}

} // namespace

auto ZAdd(ExecContext* ctx, CmdArgs& args) -> void {
    auto* sender = ctx->sender;
    if ((args.size() & 1) != 0) {
        return SendArgNumErr(sender, "zadd");
    }

    std::vector<ZAddEntry> entries;
    entries.reserve((args.size() - 2) / 2);
    for (size_t i = 2; i < args.size(); i += 2) {
        double score = 0;
        if (!ParseDouble(args[i], &score)) {
            return sender->SendError(kInvalidFloatErr);
        }
        entries.push_back(ZAddEntry{score, args[i + 1]});
    }

    bool    wrong_type = false;
    int64_t added      = 0;

    ctx->CurTxn()->Execute([&](Transaction*, Shard* shard) {
        auto*      db  = shard->DbAt(ctx->db_index);
        auto       res = db->Get(args[1], Value::ZSET);
        PrimeValue key_value;

        switch (res.status) {
        case OpStatus::OK:
            key_value = res.payload;
            break;
        case OpStatus::NoSuchKey: {
            key_value                     = MakeValue<Value::ZSET>();
            [[maybe_unused]] auto set_res = db->Set(std::string(args[1]), key_value);
            CHECK(set_res.Ok());
            break;
        }
        case OpStatus::WrongType:
            wrong_type = true;
            return;
        default:
            UNREACHABLE();
        }

        auto* zset = key_value->GetZSet();
        for (const auto& entry : entries) {
            if (zset->Add(entry.member, entry.score)) {
                ++added;
            }
        }
    });

    if (wrong_type) {
        return sender->SendError(kWrongTypeErr);
    }
    sender->SendInteger(added);
}

auto ZRem(ExecContext* ctx, CmdArgs& args) -> void {
    auto*    sender  = ctx->sender;
    int64_t  removed = 0;
    OpStatus status  = OpStatus::OK;

    ctx->CurTxn()->Execute([&](Transaction*, Shard* shard) {
        auto* db  = shard->DbAt(ctx->db_index);
        auto  res = db->Get(args[1], Value::ZSET);
        if (!res.Ok()) {
            status = res.status;
            return;
        }

        auto* zset = res.payload->GetZSet();
        for (size_t i = 2; i < args.size(); ++i) {
            if (zset->Rem(args[i])) {
                ++removed;
            }
        }

        bool delete_key = removed != 0 && zset->Size() == 0;
        if (delete_key) {
            [[maybe_unused]] auto del_res = db->Del(args[1]);
            CHECK(del_res.Ok());
        }
    });

    if (status == OpStatus::WrongType) {
        return sender->SendError(kWrongTypeErr);
    }

    sender->SendInteger(removed);
}

auto ZRange(ExecContext* ctx, CmdArgs& args) -> void {
    auto* sender = ctx->sender;
    if (args.size() != 4 && args.size() != 5) {
        return SendArgNumErr(sender, "zrange");
    }

    int64_t start = 0;
    int64_t stop  = 0;
    if (!ParseInt64(args[2], &start) || !ParseInt64(args[3], &stop)) {
        return sender->SendError(kInvalidIntErr);
    }

    const bool with_scores = args.size() == 5;
    if (with_scores && !EqualsIgnoreCase(args[4], "withscores")) {
        return sender->SendError(kSyntaxErr);
    }

    std::vector<ZSet::MemberScore> members;
    OpStatus                       status = OpStatus::OK;

    ctx->CurTxn()->Execute([&](Transaction*, Shard* shard) {
        auto* db  = shard->DbAt(ctx->db_index);
        auto  res = db->Get(args[1], Value::ZSET);
        if (!res.Ok()) {
            status = res.status;
            return;
        }

        members = res.payload->GetZSet()->Range(start, stop);
    });

    if (status == OpStatus::WrongType) {
        return sender->SendError(kWrongTypeErr);
    }

    std::vector<std::string> response;
    response.reserve(members.size() * (with_scores ? 2U : 1U));
    for (const auto& member : members) {
        response.push_back(member.member);
        if (with_scores) {
            response.push_back(fmt::format("{}", member.score));
        }
    }

    sender->SendBulkStringArray(std::move(response));
}

auto InitZSet(IdleEngine* eng) -> void {
    eng->RegisterCmd("zadd", -4, 1, 1, ZAdd, SingleWriteKey, CmdFlags::Transactional);
    eng->RegisterCmd("zrem", -3, 1, 1, ZRem, SingleWriteKey, CmdFlags::Transactional);
    eng->RegisterCmd("zrange", -4, 1, 1, ZRange, SingleReadKey, CmdFlags::Transactional);
}

} // namespace idlekv
