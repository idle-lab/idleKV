#pragma once

#include "common/logger.h"
#include "db/storage/data_entity.h"
#include "db/transaction.h"
#include "redis/parser.h"

#include <cstddef>
#include <memory>
#include <string>
#include <variant>
#include <vector>

namespace idlekv {

struct SimpleString : std::string {};
struct BulkString {
    std::string                       data;
    std::shared_ptr<const DataEntity> holder;
};
using Integer = int64_t;
struct Error : std::string {};
using Null = std::nullptr_t;

using Payload = std::variant<SimpleString, BulkString, Integer, Error, Null>;

class PayloadVisitor {
public:
    PayloadVisitor(Sender* sender) : sender_(sender) {}

    auto operator()(const SimpleString& s) -> void { sender_->SendSimpleString(s); }
    auto operator()(const BulkString& s) -> void {
        if (s.holder) {
            sender_->SendBulkString(s.holder);
        } else {
            sender_->SendBulkString(s.data);
        }
    }
    auto operator()(const Integer& i) -> void { sender_->SendInteger(i); }
    auto operator()(const Error& e) -> void { sender_->SendError(e); }
    auto operator()(const Null&) -> void { sender_->SendNullBulkString(); }

private:
    Sender* sender_;
};

class ResultCapturer : public SenderBase {
public:
    auto SendSimpleString(std::string_view s) -> void override {
        payload_ = SimpleString(std::string(s));
    }
    auto SendOk() -> void override { payload_ = SimpleString("OK"); }
    auto SendPong() -> void override { payload_ = SimpleString("PONG"); }
    auto SendBulkString(std::string_view s) -> void override {
        payload_ = BulkString(std::string(s));
    }
    auto SendBulkString(const std::shared_ptr<const DataEntity>& data) -> void override {
        CHECK(data);
        payload_ = BulkString{.holder = data};
    }
    auto SendNullBulkString() -> void override { payload_ = Null{}; }
    auto SendInteger(int64_t value) -> void override { payload_ = Integer(value); }
    auto SendError(std::string_view s) -> void override { payload_ = Error(std::string(s)); }

    auto GetPayload() -> Payload& { return payload_; }

private:
    Payload payload_;
};

class CmdSquasher {
public:
    struct ShardExecInfo {
        std::vector<CommandContext> pending_cmds;
        std::vector<Payload>        results;
    };

    explicit CmdSquasher() {

    }

    static auto Squash(std::vector<CommandContext>& cmds, Sender& sender) -> void;

private:
    ResultCapturer capturer_;

    std::vector<ShardExecInfo> shard_infos_;

    Transaction pipeline_txn_;
};

} // namespace idlekv