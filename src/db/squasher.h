#pragma once

#include "common/config.h"
#include "db/context.h"
#include "db/engine.h"
#include "db/shard.h"
#include "db/storage/value.h"
#include "redis/parser.h"

#include <absl/container/inlined_vector.h>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace idlekv {

struct Ok {};
struct Pong {};
struct SimpleString : std::string {};
struct BulkString : PrimeValue {};
using BulkStringArray = std::vector<std::string>;
using Integer         = int64_t;
struct Error : std::string {};
using Null = std::nullptr_t;

using Payload = std::variant<std::monostate, Ok, Pong, SimpleString, BulkString, BulkStringArray,
                             Integer, Error, Null>;

class PayloadVisitor {
public:
    PayloadVisitor(Sender* sender) : sender_(sender) {}

    auto operator()(std::monostate) -> void { UNREACHABLE(); }
    auto operator()(const Ok&) -> void { sender_->SendOk(); }
    auto operator()(const Pong&) -> void { sender_->SendPong(); }
    auto operator()(const SimpleString& s) -> void { sender_->SendSimpleString(s); }
    auto operator()(const BulkString& s) -> void {
        sender_->SendBulkString(s->GetString(), std::move(s));
    }
    auto operator()(const BulkStringArray& values) -> void { sender_->SendBulkStringArray(values); }
    auto operator()(const Integer& i) -> void { sender_->SendInteger(i); }
    auto operator()(const Error& e) -> void { sender_->SendError(e); }
    auto operator()(const Null&) -> void { sender_->SendNullBulkString(); }

private:
    Sender* sender_;
};

class ReplyCapturer : public SenderBase {
public:
    auto SendSimpleString(std::string_view s) -> void override {
        payload_ = SimpleString(std::string(s));
    }
    auto SendOk() -> void override { payload_ = Ok{}; }
    auto SendPong() -> void override { payload_ = Pong{}; }
    auto SendBulkString(std::string_view, PrimeValue holder) -> void override {
        payload_ = BulkString(std::move(holder));
    }
    auto SendBulkStringArray(std::vector<std::string> values) -> void override {
        payload_ = BulkStringArray(std::move(values));
    }
    auto SendNullBulkString() -> void override { payload_ = Null{}; }
    auto SendInteger(int64_t value) -> void override { payload_ = Integer(value); }
    auto SendError(std::string_view s) -> void override { payload_ = Error(std::string(s)); }

    auto TakePayload() -> Payload {
        Payload payload = std::move(payload_);
        payload_        = std::monostate{};
        return payload;
    }

private:
    Payload payload_;
};

class CmdSquasher {
public:
    struct ShardExecInfo {
        std::vector<Payload> results;
        size_t               send_idx{0};
        ExecContext          sub_ctx;
    };

    explicit CmdSquasher(ExecContext* client) : parent_ctx_(client) {}

    static auto Squash(std::vector<CommandContext>& cmds, Sender* sender, ExecContext* client)
        -> size_t;

    auto Squash(std::vector<CommandContext>& cmds, Sender* sender) -> void;

    auto ExecuteSquash(Sender* sender) -> void;

    enum struct DetermineResult : uint8_t {
        OK,
        CanNotSquash,
        Full, // TODO(cyb): control squash batch size.
    };
    auto TrySquash(CommandContext& cmd) -> DetermineResult;
    auto ShardInfo(ShardId id) -> ShardExecInfo&;

private:
    auto DebugCheckState(std::string_view where) const -> void;

    std::vector<ShardExecInfo> shards_info_;
    size_t                     active_shard_count_{0};
    std::vector<size_t>        order_;

    ExecContext* parent_ctx_;

    size_t processed_{0};
};

} // namespace idlekv
