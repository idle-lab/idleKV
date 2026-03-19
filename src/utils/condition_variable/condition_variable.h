#pragma once

#include "common/asio_no_exceptions.h"
#include "common/logger.h"

#include <asio/any_io_executor.hpp>
#include <asio/as_tuple.hpp>
#include <asio/awaitable.hpp>
#include <asio/dispatch.hpp>
#include <asio/error.hpp>
#include <asio/io_context.hpp>
#include <asio/steady_timer.hpp>
#include <asio/use_awaitable.hpp>
#include <chrono>
namespace idlekv {

namespace utils {

constexpr auto kMaxTimePoint = std::chrono::steady_clock::time_point::max();

// a reusable coroutine-level condition variable.
// only can use in signal-thread, signal io_context environment.
class ConditionVariable {
public:
    ConditionVariable(const asio::any_io_executor& exector) : timer_(exector) {
        timer_.expires_at(kMaxTimePoint);
    }

    auto Notify() -> void { 
        timer_.cancel(); 
        timer_.expires_at(kMaxTimePoint);
    }

    auto AsyncWait() -> asio::awaitable<void> {
        auto [ec] = co_await timer_.async_wait(asio::as_tuple(asio::use_awaitable));
        CHECK_EQ(ec, asio::error::operation_aborted);
    }
    auto GetExecutor() -> const asio::any_io_executor& { return timer_.get_executor(); }

    ~ConditionVariable() {}
private:
    asio::steady_timer timer_;
};

// a non-reusable coroutine-level condition variable.
// only can use in signal-thread, signal io_context environment.
class DisposableConditionVariable {
public:
    DisposableConditionVariable(const asio::any_io_executor& exector) : timer_(exector) {
        timer_.expires_at(kMaxTimePoint);
    }

    auto Notify() -> void { 
        timer_.cancel(); 
    }

    auto AsyncWait() -> asio::awaitable<void> {
        auto [ec] = co_await timer_.async_wait(asio::as_tuple(asio::use_awaitable));
        CHECK_EQ(ec, asio::error::operation_aborted);
    }
    auto GetExecutor() -> const asio::any_io_executor& { return timer_.get_executor(); }

    ~DisposableConditionVariable() {}
private:
    asio::steady_timer timer_;
};

} // namespace utils

} // namespace idlekv
