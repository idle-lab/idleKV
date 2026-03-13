#pragma once

#include <cstdint>
#include <optional>
#include <utility>

namespace idlekv {

enum struct OpStatus : uint8_t { OK, DupKey, Unknown };

template <class PayLoad>
struct Result {
    Result(OpStatus s, const std::optional<PayLoad>& res) : s_(s), res_(std::move(res)) {}

    auto operator==(const OpStatus& s) const -> bool { return s_ == s; }

    auto ok() const -> bool { return *this == OpStatus::OK; }

    auto get() -> PayLoad& { return res_.value(); }
    auto get() const -> const PayLoad& { return res_.value(); }

    OpStatus               s_;
    std::optional<PayLoad> res_;
};

} // namespace idlekv
