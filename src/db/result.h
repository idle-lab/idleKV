#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>

namespace idlekv {

class ExecResult {
public:
    enum Type : uint8_t {
        kOk,
        kSimpleString,
        kBulkString,
        kNull,
        kInteger,
        kError,
    };

    ExecResult() = default;

    static auto ok() -> ExecResult { return ExecResult(kOk); }

    static auto simple_string(std::string value) -> ExecResult {
        return ExecResult(kSimpleString, std::move(value));
    }

    static auto bulk_string(std::string value) -> ExecResult {
        return ExecResult(kBulkString, std::move(value));
    }

    static auto null() -> ExecResult { return ExecResult(kNull); }

    static auto integer(int64_t value) -> ExecResult { return ExecResult(kInteger, value); }

    static auto error(std::string value) -> ExecResult {
        return ExecResult(kError, std::move(value));
    }

    auto type() const -> Type { return type_; }

    auto is_ok() const -> bool { return type_ == kOk; }

    auto string() const -> std::string_view { return string_; }

    auto integer() const -> int64_t { return integer_; }

private:
    explicit ExecResult(Type type) : type_(type) {}

    ExecResult(Type type, std::string value) : type_(type), string_(std::move(value)) {}

    ExecResult(Type type, int64_t value) : type_(type), integer_(value) {}

    Type        type_    = kOk;
    std::string string_;
    int64_t     integer_ = 0;
};

} // namespace idlekv
