#pragma once

#include "common/config.h"

#include <cstdint>
#include <optional>
#include <string>
#include <utility>

namespace idlekv {

enum struct OpStatus : uint8_t { OK, DupKey, NoSuchKey, Unknown };

inline auto OpStatusToString(OpStatus ops) -> std::string {
    switch (ops) {
    case OpStatus::OK:
        return "OK";
    case OpStatus::DupKey:
        return "duplicate key";
    case OpStatus::NoSuchKey:
        return "no such key";
    case OpStatus::Unknown:
        return "unknown error";
    default:
        UNREACHABLE();
    }
}

template <class PayLoad>
struct Result {
    Result(OpStatus s, const std::optional<PayLoad>& res) : s_(s), res_(std::move(res)) {}

    auto operator==(const OpStatus& s) const -> bool { return s_ == s; }

    auto Ok() const -> bool { return *this == OpStatus::OK; }
    auto Message() const -> std::string { return OpStatusToString(s_); }

    auto Get() -> PayLoad& { return res_.value(); }
    auto Get() const -> const PayLoad& { return res_.value(); }

    OpStatus               s_;
    std::optional<PayLoad> res_;
};

} // namespace idlekv
