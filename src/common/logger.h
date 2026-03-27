#pragma once

#include <cstdlib>
#include <memory>
#include <ostream>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>
#include <sstream>
#include <string>
#include <string_view>

namespace idlekv {

std::shared_ptr<spdlog::logger> MakeDefaultLogger();

#define LOG(level, ...) spdlog::level(__VA_ARGS__)

namespace detail {

class NullCheck {
public:
    template <class T>
    constexpr auto operator<<(const T&) noexcept -> NullCheck& {
        return *this;
    }
};

template <class T>
concept CheckStreamInsertable = requires(std::ostream& os, const T& value) { os << value; };

template <class T>
inline auto CheckToString(const T& value) -> std::string {
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

    CheckFailure(CheckFailure&&) noexcept                    = default;
    CheckFailure(const CheckFailure&)                        = delete;
    auto operator=(CheckFailure&&) noexcept -> CheckFailure& = default;
    auto operator=(const CheckFailure&) -> CheckFailure&     = delete;

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
inline auto MakeCheckCmp(L&& lhs, R&& rhs, const char* file, int line, std::string_view lhs_expr,
                         std::string_view rhs_expr, std::string_view op, Pred pred)
    -> CheckFailure {
    bool         ok = pred(lhs, rhs);
    CheckFailure check(ok, file, line, "");
    if (!ok) {
        check << lhs_expr << " " << op << " " << rhs_expr << " (lhs=" << CheckToString(lhs)
              << ", rhs=" << CheckToString(rhs) << ")";
    }
    return check;
}

} // namespace detail

} // namespace idlekv

#ifndef DISABLE_CHECK
#define CHECK(condition)                                                                           \
    ::idlekv::detail::CheckFailure(static_cast<bool>(condition), __FILE__, __LINE__, #condition)

#define IDLEKV_CHECK_OP(op, lhs, rhs)                                                              \
    ::idlekv::detail::MakeCheckCmp(                                                                \
        (lhs), (rhs), __FILE__, __LINE__, #lhs, #rhs, #op,                                         \
        [](const auto& idlekv_l, const auto& idlekv_r) { return idlekv_l op idlekv_r; })

#define CHECK_GT(lhs, rhs) IDLEKV_CHECK_OP(>, lhs, rhs)
#define CHECK_LT(lhs, rhs) IDLEKV_CHECK_OP(<, lhs, rhs)
#define CHECK_EQ(lhs, rhs) IDLEKV_CHECK_OP(==, lhs, rhs)
#define CHECK_NE(lhs, rhs) IDLEKV_CHECK_OP(!=, lhs, rhs)
#define CHECK_GE(lhs, rhs) IDLEKV_CHECK_OP(>=, lhs, rhs)
#define CHECK_LE(lhs, rhs) IDLEKV_CHECK_OP(<=, lhs, rhs)
#else
#define CHECK(condition) ((void)sizeof(static_cast<bool>(condition)), ::idlekv::detail::NullCheck())
#define IDLEKV_CHECK_OP(op, lhs, rhs)                                                              \
    ((void)sizeof((lhs)op(rhs)), (void)sizeof(lhs), (void)sizeof(rhs),                             \
     ::idlekv::detail::NullCheck())

#define CHECK_GT(lhs, rhs) IDLEKV_CHECK_OP(>, lhs, rhs)
#define CHECK_LT(lhs, rhs) IDLEKV_CHECK_OP(<, lhs, rhs)
#define CHECK_EQ(lhs, rhs) IDLEKV_CHECK_OP(==, lhs, rhs)
#define CHECK_NE(lhs, rhs) IDLEKV_CHECK_OP(!=, lhs, rhs)
#define CHECK_GE(lhs, rhs) IDLEKV_CHECK_OP(>=, lhs, rhs)
#define CHECK_LE(lhs, rhs) IDLEKV_CHECK_OP(<=, lhs, rhs)
#endif
