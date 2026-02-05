#pragma once

#include <chrono>
#include <asiochan/asiochan.hpp>
#include <asio/asio.hpp>
#include <common/logger.h>
#include <memory>
#include <atomic>

namespace idlekv {

auto timer_context() -> asio::io_context& {
    static std::atomic<bool> initialized = false;
    static asio::io_context io_ctx;
    static asio::executor_work_guard wg =
        asio::make_work_guard(io_ctx);
    if (!initialized.exchange(true, std::memory_order_acq_rel)) {
        static std::jthread timer_thread([&]() {
            io_ctx.run();
        });
    }
    return io_ctx;
}

auto set_timeout(std::chrono::steady_clock::duration dur) -> asiochan::read_channel<void> {
    auto timer = std::make_shared<asio::steady_timer>(timer_context());
    timer->expires_after(dur);

    auto timeout = asiochan::channel<void>{};

    asio::co_spawn(
        timer_context(),
        [=]() mutable -> asio::awaitable<void> {
            co_await timer->async_wait(asio::use_awaitable);
            co_await timeout.write();
        },
        asio::detached);

    return timeout;
}

} // namespace idlekv
