#pragma once

#include <cstdlib>
#include <memory>
#include <ostream>
#include <sstream>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>
#include <string>
#include <string_view>
#include <utility>

namespace idlekv {

std::shared_ptr<spdlog::logger> make_default_logger();

#define LOG(level, ...) spdlog::level(__VA_ARGS__)

namespace detail {

template <class T>
concept CheckStreamInsertable = requires(std::ostream& os, const T& value) { os << value; };

template <class T>
inline auto check_to_string(const T& value) -> std::string {
    if constexpr (CheckStreamInsertable<T>) {
        std::ostringstream oss;
        oss << std::boolalpha << value;
        return oss.str();
    } else {
        return "<non-streamable>";
    }
}

class CheckFailure {
public:
    CheckFailure(bool passed, const char* file, int line, std::string_view expr)
        : passed_(passed), file_(file), line_(line) {
        if (!passed_) {
            message_ << "CHECK failed: " << expr;
        }
    }

    CheckFailure(CheckFailure&&) noexcept = default;
    CheckFailure(const CheckFailure&)     = delete;
    auto operator=(CheckFailure&&) noexcept -> CheckFailure& = default;
    auto operator=(const CheckFailure&) -> CheckFailure& = delete;

    ~CheckFailure() {
        if (!passed_) {
            spdlog::critical("{}:{} {}", file_, line_, message_.str());
            std::abort();
        }
    }

    template <class T>
    auto operator<<(const T& value) -> CheckFailure& {
        if (!passed_) {
            message_ << value;
        }
        return *this;
    }

private:
    bool               passed_;
    const char*        file_;
    int                line_;
    std::ostringstream message_;
};

template <class L, class R, class Pred>
inline auto make_check_cmp(L&& lhs, R&& rhs, const char* file, int line, std::string_view lhs_expr,
                           std::string_view rhs_expr, std::string_view op, Pred pred)
    -> CheckFailure {
    bool ok = pred(lhs, rhs);
    CheckFailure check(ok, file, line, "");
    if (!ok) {
        check << lhs_expr << " " << op << " " << rhs_expr << " (lhs=" << check_to_string(lhs)
              << ", rhs=" << check_to_string(rhs) << ")";
    }
    return check;
}

} // namespace detail

} // namespace idlekv

#define CHECK(condition)                                                                      \
    ::idlekv::detail::CheckFailure(static_cast<bool>(condition), __FILE__, __LINE__, #condition)

#define IDLEKV_CHECK_OP(op, lhs, rhs)                                                         \
    ::idlekv::detail::make_check_cmp((lhs), (rhs), __FILE__, __LINE__, #lhs, #rhs, #op,     \
                                     [](const auto& idlekv_l, const auto& idlekv_r) {        \
                                         return idlekv_l op idlekv_r;                         \
                                     })

#define CHECK_GT(lhs, rhs) IDLEKV_CHECK_OP(>, lhs, rhs)
#define CHECK_LT(lhs, rhs) IDLEKV_CHECK_OP(<, lhs, rhs)
#define CHECK_EQ(lhs, rhs) IDLEKV_CHECK_OP(==, lhs, rhs)
#define CHECK_NE(lhs, rhs) IDLEKV_CHECK_OP(!=, lhs, rhs)
#define CHECK_GE(lhs, rhs) IDLEKV_CHECK_OP(>=, lhs, rhs)
#define CHECK_LE(lhs, rhs) IDLEKV_CHECK_OP(<=, lhs, rhs)
