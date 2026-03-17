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

    static auto pong() -> ExecResult { return ExecResult(kPong); }
    static auto ok() -> ExecResult { return ExecResult(kOk); }

    static auto simple_string(std::string value) -> ExecResult {
        return ExecResult(kSimpleString, std::move(value));
    }

    static auto bulk_string(std::string value) -> ExecResult {
        return ExecResult(kBulkString, std::move(value));
    }

    static auto bulk_string(const std::shared_ptr<DataEntity>& data) -> ExecResult {
        return ExecResult(kBulkString, data);
    }


    static auto bulk_string(std::string_view value) -> ExecResult {
        return ExecResult(kBulkString, std::string(value));
    }

    static auto null() -> ExecResult { return ExecResult(kNull); }

    static auto integer(int64_t value) -> ExecResult { return ExecResult(kInteger, value); }

    static auto error(std::string value) -> ExecResult {
        return ExecResult(kError, std::move(value));
    }

    auto type() const -> Type { return type_; }

    auto is_ok() const -> bool { return type_ == kOk; }

    auto string() const -> std::string_view { return string_; }

    auto data() -> const std::shared_ptr<DataEntity>& { return data_; }
    auto data() const -> const std::shared_ptr<DataEntity>& { return data_; }

    auto integer() const -> int64_t { return integer_; }

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
