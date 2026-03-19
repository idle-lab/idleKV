#pragma once

#include "db/storage/kvstore.h"
#include "db/storage/data_entity.h"
#include <asio/any_io_executor.hpp>
#include <asio/awaitable.hpp>
#include <asio/steady_timer.hpp>
#include <asio/use_awaitable.hpp>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

namespace idlekv {

class ExecResult {
public:
    enum Type : uint8_t {
        kPong,
        kOk,
        kSimpleString,
        kBulkString,
        kNull,
        kInteger,
        kError,
    };

    ExecResult() = default;

    static auto Pong() -> ExecResult { return ExecResult(kPong); }
    static auto Ok() -> ExecResult { return ExecResult(kOk); }

    static auto SimpleString(std::string value) -> ExecResult {
        return ExecResult(kSimpleString, std::move(value));
    }

    static auto BulkString(std::string value) -> ExecResult {
        return ExecResult(kBulkString, std::move(value));
    }

    static auto BulkString(const std::shared_ptr<DataEntity>& data) -> ExecResult {
        return ExecResult(kBulkString, data);
    }


    static auto BulkString(std::string_view value) -> ExecResult {
        return ExecResult(kBulkString, std::string(value));
    }

    static auto Null() -> ExecResult { return ExecResult(kNull); }

    static auto Integer(int64_t value) -> ExecResult { return ExecResult(kInteger, value); }

    static auto Error(std::string value) -> ExecResult {
        return ExecResult(kError, std::move(value));
    }

    auto GetType() const -> Type { return type_; }

    auto IsOk() const -> bool { return type_ == kOk; }

    auto GetString() const -> std::string_view { return string_; }

    auto GetData() -> const std::shared_ptr<DataEntity>& { return data_; }
    auto GetData() const -> const std::shared_ptr<DataEntity>& { return data_; }

    auto GetInteger() const -> int64_t { return integer_; }

private:
    explicit ExecResult(Type type) : type_(type) {}

    ExecResult(Type type, std::string value) : type_(type), string_(std::move(value)) {}
    ExecResult(Type type, const std::shared_ptr<DataEntity>& data) : type_(type), data_(data) {}

    ExecResult(Type type, int64_t value) : type_(type), integer_(value) {}

    Type        type_ = kOk;
    std::string string_;
    std::shared_ptr<DataEntity> data_;
    int64_t     integer_ = 0;
};

} // namespace idlekv
