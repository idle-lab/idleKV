#include <asio/as_tuple.hpp>
#include <common/logger.h>
#include <utils/timer/timer.h>

namespace idlekv {

auto TimerContext() -> asio::io_context& {
    static std::atomic<bool>         initialized = false;
    static asio::io_context          io_ctx;
    static asio::executor_work_guard wg = asio::make_work_guard(io_ctx);
    if (!initialized.exchange(true, std::memory_order_acq_rel)) {
        static std::jthread timer_thread([&]() { io_ctx.run(); });
    }
    return io_ctx;
}

auto SetTimeout(std::chrono::steady_clock::duration dur) -> asiochan::read_channel<void> {
    auto timer = std::make_shared<asio::steady_timer>(TimerContext());
    timer->expires_after(dur);

    auto timeout = asiochan::channel<void>{};

    asio::co_spawn(
        TimerContext(),
        [=]() mutable -> asio::awaitable<void> {
            auto [ec] = co_await timer->async_wait(asio::as_tuple(asio::use_awaitable));
            if (!ec) {
                co_await timeout.write();
            }
        },
        asio::detached);

    return timeout;
}

} // namespace idlekv
