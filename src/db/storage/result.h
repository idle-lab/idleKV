#pragma once

#include "common/config.h"

#include <cstdint>
#include <string>
#include <utility>

namespace idlekv {

enum struct OpStatus : uint8_t { OK, DupKey, NoSuchKey, WrongType, Unknown };

inline auto OpStatusToString(OpStatus ops) -> std::string {
    switch (ops) {
    case OpStatus::OK:
        return "OK";
    case OpStatus::DupKey:
        return "Duplicate Key";
    case OpStatus::NoSuchKey:
        return "No Such Key";
    case OpStatus::Unknown:
        return "Unknown Error";
    default:
        UNREACHABLE();
    }
}

template <class PayLoad>
struct Result {
    Result() = default;
    Result(OpStatus s) : status(s) {}
    Result(OpStatus s, PayLoad res) : status(s), payload(std::move(res)) {}

    auto operator==(const OpStatus& s) const -> bool { return status == s; }

    auto Ok() const -> bool { return *this == OpStatus::OK; }
    auto Message() const -> std::string { return OpStatusToString(status); }

    auto Get() -> PayLoad& { return payload; }
    auto Get() const -> const PayLoad& { return payload; }

    OpStatus status;
    PayLoad  payload;
};

template <>
struct Result<void> {
    Result() = default;
    Result(OpStatus s) : status(s) {}
    auto     operator==(const OpStatus& s) const -> bool { return status == s; }
    auto     Ok() const -> bool { return *this == OpStatus::OK; }
    auto     Message() const -> std::string { return OpStatusToString(status); }
    OpStatus status;
};

} // namespace idlekv
