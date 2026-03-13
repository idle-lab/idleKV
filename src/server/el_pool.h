#pragma once

#include "common/asio_no_exceptions.h"
#include "utils/cpu/basic.h"

#include <asio/awaitable.hpp>
#include <asio/co_spawn.hpp>
#include <asio/detached.hpp>
#include <asio/executor_work_guard.hpp>
#include <asio/io_context.hpp>
#include <asio/post.hpp>
#include <asio/use_future.hpp>
#include <atomic>
#include <concepts>
#include <cstddef>
#include <future>
#include <latch>
#include <memory>
#include <optional>
#include <thread>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>
namespace idlekv {

constexpr auto kMaxBusyCpuTime = std::chrono::milliseconds(100);

// manages a single io_context thread and runs submitted tasks on its bound cpu.
class EventLoop {
public:
    EventLoop(unsigned cpu) : io_(1), wg_(asio::make_work_guard(io_)), cpu_(cpu) {}

    auto run() -> void;

    // dispatch a function
    template <class Fn, class... Args>
        requires std::invocable<Fn, Args...>
    auto dispatch(Fn&& f, Args&&... args) {
        using R = std::invoke_result_t<Fn, Args...>;

        auto task = std::make_shared<std::packaged_task<R()>>(
            [fn = std::forward<Fn>(f), ... args = std::forward<Args>(args)]() mutable {
                return std::invoke(fn, args...);
            });
        auto fut = task->get_future();

        asio::post(io_, [task]() { (*task)(); });
        return std::future<R>(std::move(fut));
    }

    // dispatch a coroutine
    template <class RetType>
    auto dispatch(asio::awaitable<RetType>&& aw) -> void {
        return asio::co_spawn(io_, std::move(aw), asio::detached);
    }

    // wait for the function to finish executing and return the result.
    template <class Fn, class... Args>
        requires std::invocable<Fn, Args...>
    auto await_dispatch(Fn&& f, Args&&... args) {
        auto fut = dispatch(std::forward<Fn>(f), std::forward<Args>(args)...);
        return fut.get();
    }

    // wait for the coroutine to finish executing and return the result.
    template <class RetType>
    auto await_dispatch(asio::awaitable<RetType>&& aw) -> RetType {
        auto fut = asio::co_spawn(io_, std::move(aw), asio::use_future);
        return fut.get();
    }

    auto thread_id() -> std::thread::native_handle_type { return th_.native_handle(); }
    auto io_context() -> asio::io_context& { return io_; }
    auto cpu() -> unsigned { return cpu_; }

    // this function does not block, but instead simply signals the EventLoop to stop.
    auto stop() -> void;

private:
    asio::io_context                                           io_;
    asio::executor_work_guard<asio::io_context::executor_type> wg_;
    unsigned                                                   cpu_;

    std::jthread th_;
};

// owns all event loops and distributes work across worker threads.
class EventLoopPool {
public:
    template <class RetType>
    using await_optional_t =
        std::optional<std::conditional_t<std::is_void_v<RetType>, std::monostate, RetType>>;

    EventLoopPool(size_t pool_size = 0)
        : pool_size_(pool_size > 0 ? pool_size : utils::get_online_cpus_num()) {}

    auto run() -> void;

    template <class Fn>
        requires std::invocable<Fn, size_t, EventLoop*>
    auto await_foreach(Fn&& f) -> void {
        if (!is_running_.load(std::memory_order_acquire)) {
            return;
        }

        std::latch l(pool_size_);
        for (size_t i = 0; i < pool_size_; i++) {
            // f must be copied, it can not be moved, because we dsitribute it into
            // multiple EventLoop.
            els_[i]->dispatch([this, &l, i, f]() {
                f(i, els_[i].get());
                l.count_down();
            });
        }

        l.wait();
    }

    template <class Fn, class... Args>
    auto await_dispatch(Fn&& f, Args&&... args)
        -> await_optional_t<std::invoke_result_t<Fn, Args...>> {
        using RetType = std::invoke_result_t<Fn, Args...>;
        if (!is_running_.load(std::memory_order_acquire)) {
            return std::nullopt;
        }

        if constexpr (std::is_void_v<RetType>) {
            pick_up_el()->await_dispatch(std::forward<Fn>(f), std::forward<Args>(args)...);
            return std::monostate{};
        } else {
            return pick_up_el()->await_dispatch(std::forward<Fn>(f), std::forward<Args>(args)...);
        }
    }

    template <class RetType>
    auto await_dispatch(asio::awaitable<RetType>&& aw) -> await_optional_t<RetType> {
        if (!is_running_.load(std::memory_order_acquire)) {
            return std::nullopt;
        }

        if constexpr (std::is_void_v<RetType>) {
            pick_up_el()->await_dispatch(std::move(aw));
            return std::monostate{};
        } else {
            return pick_up_el()->await_dispatch(std::move(aw));
        }
    }

    // dispatch a coroutine
    template <class RetType>
    auto dispatch(asio::awaitable<RetType> aw) -> void {
        if (!is_running_.load(std::memory_order_acquire)) {
            return;
        }
        return pick_up_el()->dispatch(std::move(aw));
    }

    auto pick_up_el() -> EventLoop*;

    auto stop() -> void;
    auto pool_size() -> size_t { return pool_size_; }
    auto at(size_t i) -> EventLoop* { return els_[i].get(); }

    auto map_cpu_to_threads(size_t cpu) -> std::vector<unsigned>& { return cpu_threads_[cpu]; }

private:
    auto setup_els() -> void;

    std::atomic_bool is_running_;

    std::vector<std::unique_ptr<EventLoop>> els_;
    std::atomic_size_t                      next_el_{0};
    size_t                                  pool_size_;

    // abs cpu -> threads that pinned in that cpu.
    std::vector<std::vector<unsigned>> cpu_threads_;
};

} // namespace idlekv
