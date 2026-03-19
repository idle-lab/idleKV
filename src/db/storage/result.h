#pragma once

#include <cstdint>
#include <optional>
#include <utility>

namespace idlekv {

enum struct OpStatus : uint8_t { OK, DupKey, NoSuchKey, Unknown };

template <class PayLoad>
struct Result {
    Result(OpStatus s, const std::optional<PayLoad>& res) : s_(s), res_(std::move(res)) {}

    auto operator==(const OpStatus& s) const -> bool { return s_ == s; }

    auto Ok() const -> bool { return *this == OpStatus::OK; }

    auto Get() -> PayLoad& { return res_.value(); }
    auto Get() const -> const PayLoad& { return res_.value(); }

    OpStatus               s_;
    std::optional<PayLoad> res_;
};

} // namespace idlekv
