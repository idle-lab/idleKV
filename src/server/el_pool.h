#pragma once

#include "server/fiber_runtime.h"
#include "server/thread_state.h"
#include "utils/cpu/basic.h"

#include <atomic>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/fiber/condition_variable.hpp>
#include <boost/fiber/future/async.hpp>
#include <boost/fiber/policy.hpp>
#include <concepts>
#include <cstddef>
#include <exception>
#include <functional>
#include <future>
#include <latch>
#include <memory>
#include <optional>
#include <thread>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>
namespace idlekv {

using namespace boost;

inline const uint64_t kMaxParseCpuCycles =
    200 /* us */ * FiberCycleClock::Frequency() / 1'000'000ULL;
inline const uint64_t kMaxSquashCpuCycles =
    500 /* us */ * FiberCycleClock::Frequency() / 1'000'000ULL;

// manages a single io_context thread and runs submitted tasks on its bound cpu.
class EventLoop {
public:
    EventLoop(unsigned cpu, size_t pool_index)
        : io_(1), cpu_(cpu), pool_index_(pool_index) {}

    auto Run() -> void;

    template <class Fn, class... Args>
        requires std::invocable<Fn, Args...>
    auto Post(Fn&& f, Args&&... args) -> void {
        asio::post(io_, [fn = std::forward<Fn>(f), ... args = std::forward<Args>(args)]() mutable {
            std::invoke(fn, args...);
        });
    }

    template <class Fn, class... Args>
        requires std::invocable<Fn, Args...>
    auto Dispatch(Fn&& f, Args&&... args) -> void {
        Dispatch(FiberPriority::NORMAL, std::forward<Fn>(f), std::forward<Args>(args)...);
    }

    template <class Fn, class... Args>
        requires std::invocable<Fn, Args...>
    auto Dispatch(FiberPriority priority, Fn&& f, Args&&... args) -> void {
        // keep args... life cycle.
        auto task = std::make_shared<std::tuple<std::decay_t<Fn>, std::decay_t<Args>...>>(
            std::forward<Fn>(f), std::forward<Args>(args)...);

        // we need post to target thread and luanch the fiber.
        asio::post(io_, [task, priority]() mutable {
            LaunchFiberDetached(priority, [task = std::move(task)]() mutable {
                // unpack task and execute it.
                std::apply([](auto& fn,
                              auto&... inner_args) { std::invoke(fn, std::move(inner_args)...); },
                           *task);
            });
        });
    }

    template <class Fn, class... Args>
        requires std::invocable<Fn, Args...>
    auto Submit(Fn&& f, Args&&... args) {
        using R = std::invoke_result_t<Fn, Args...>;

        if (ThreadState::Tlocal() != nullptr && ThreadState::Tlocal()->GetEventLoop() == this) {
            std::promise<R> prom;
            auto            fut = prom.get_future();

            if constexpr (std::is_void_v<R>) {
                std::invoke(std::forward<Fn>(f), std::forward<Args>(args)...);
                prom.set_value();
            } else {
                prom.set_value(std::invoke(std::forward<Fn>(f), std::forward<Args>(args)...));
            }

            return fut;
        }

        return Submit(FiberPriority::NORMAL, std::forward<Fn>(f), std::forward<Args>(args)...);
    }

    template <class Fn, class... Args>
        requires std::invocable<Fn, Args...>
    auto Submit(FiberPriority priority, Fn&& f, Args&&... args) {
        using R = std::invoke_result_t<Fn, Args...>;

        if (priority == FiberPriority::NORMAL && ThreadState::Tlocal() != nullptr &&
            ThreadState::Tlocal()->GetEventLoop() == this) {
            std::promise<R> prom;
            auto            fut = prom.get_future();

            if constexpr (std::is_void_v<R>) {
                std::invoke(std::forward<Fn>(f), std::forward<Args>(args)...);
                prom.set_value();
            } else {
                prom.set_value(std::invoke(std::forward<Fn>(f), std::forward<Args>(args)...));
            }

            return fut;
        }

        auto task = std::make_shared<std::packaged_task<R()>>(
            [fn = std::forward<Fn>(f), ... args = std::forward<Args>(args)]() mutable {
                return std::invoke(fn, args...);
            });
        auto fut = task->get_future();

        asio::post(io_, [task, priority]() mutable {
            idlekv::LaunchFiberDetached(priority,
                                        [task = std::move(task)]() mutable { (*task)(); });
        });
        return std::future<R>(std::move(fut));
    }

    // wait for the function to finish executing and return the result.
    template <class Fn, class... Args>
        requires std::invocable<Fn, Args...>
    auto AwaitDispatch(Fn&& f, Args&&... args) {
        return AwaitDispatch(FiberPriority::NORMAL, std::forward<Fn>(f),
                             std::forward<Args>(args)...);
    }

    template <class Fn, class... Args>
        requires std::invocable<Fn, Args...>
    auto AwaitDispatch(FiberPriority priority, Fn&& f, Args&&... args) {
        using R = std::invoke_result_t<Fn, Args...>;

        if (ThreadState::Tlocal() != nullptr && ThreadState::Tlocal()->GetEventLoop() == this) {
            if (priority == FiberPriority::NORMAL) {
                if constexpr (std::is_void_v<R>) {
                    std::invoke(std::forward<Fn>(f), std::forward<Args>(args)...);
                    return;
                } else {
                    return std::invoke(std::forward<Fn>(f), std::forward<Args>(args)...);
                }
            }

            auto task = std::make_shared<std::tuple<std::decay_t<Fn>, std::decay_t<Args>...>>(
                std::forward<Fn>(f), std::forward<Args>(args)...);
            auto prom = std::make_shared<boost::fibers::promise<R>>();
            auto fut  = prom->get_future();

            LaunchFiber(priority, [task = std::move(task), prom = std::move(prom)]() mutable {
                try {
                    if constexpr (std::is_void_v<R>) {
                        std::apply(
                            [](auto& fn, auto&... inner_args) {
                                std::invoke(fn, std::move(inner_args)...);
                            },
                            *task);
                        prom->set_value();
                    } else {
                        prom->set_value(std::apply(
                            [](auto& fn, auto&... inner_args) {
                                return std::invoke(fn, std::move(inner_args)...);
                            },
                            *task));
                    }
                } catch (...) {
                    prom->set_exception(std::current_exception());
                }
            });

            if constexpr (std::is_void_v<R>) {
                fut.get();
                return;
            } else {
                return fut.get();
            }
        }

        auto fut = Submit(priority, std::forward<Fn>(f), std::forward<Args>(args)...);
        return fut.get();
    }

    auto ThreadId() -> std::thread::native_handle_type { return th_.native_handle(); }
    auto PoolIndex() -> size_t { return pool_index_; }
    auto IoContext() -> asio::io_context& { return io_; }
    auto Cpu() -> unsigned { return cpu_; }

    // this function does not block, but instead simply signals the EventLoop to stop.
    auto Stop() -> void;


private:
    asio::io_context   io_;
    unsigned           cpu_;
    size_t             pool_index_;

    std::jthread th_;
};

// owns all event loops and distributes work across worker threads.
class EventLoopPool {
public:
    template <class RetType>
    using await_optional_t =
        std::optional<std::conditional_t<std::is_void_v<RetType>, std::monostate, RetType>>;

    EventLoopPool(size_t PoolSize = 0)
        : pool_size_(PoolSize > 0 ? PoolSize : utils::GetOnlineCpusNum()) {}

    auto Run() -> void;

    template <class Fn>
        requires std::invocable<Fn, size_t, EventLoop*>
    auto AwaitForeach(Fn&& f) -> void {
        if (!is_running_.load(std::memory_order_acquire)) {
            return;
        }

        std::latch l(pool_size_);
        for (size_t i = 0; i < pool_size_; i++) {
            // f must be copied, it can not be moved, because we dsitribute it into
            // multiple EventLoop.
            els_[i]->Post([this, &l, i, f]() {
                f(i, els_[i].get());
                l.count_down();
            });
        }

        l.wait();
    }

    template <class Fn, class... Args>
    auto AwaitDispatch(Fn&& f, Args&&... args)
        -> await_optional_t<std::invoke_result_t<Fn, Args...>> {
        return AwaitDispatch(FiberPriority::NORMAL, std::forward<Fn>(f),
                             std::forward<Args>(args)...);
    }

    template <class Fn, class... Args>
    auto AwaitDispatch(FiberPriority priority, Fn&& f, Args&&... args)
        -> await_optional_t<std::invoke_result_t<Fn, Args...>> {
        using RetType = std::invoke_result_t<Fn, Args...>;
        if (!is_running_.load(std::memory_order_acquire)) {
            return std::nullopt;
        }

        auto* el = PickUpEl();
        if constexpr (std::is_void_v<RetType>) {
            el->AwaitDispatch(priority, std::forward<Fn>(f), std::forward<Args>(args)...);
            return std::monostate{};
        } else {
            return el->AwaitDispatch(priority, std::forward<Fn>(f), std::forward<Args>(args)...);
        }
    }

    template <class Fn, class... Args>
        requires std::invocable<Fn, Args...>
    auto Dispatch(Fn&& f, Args&&... args) -> void {
        Dispatch(FiberPriority::NORMAL, std::forward<Fn>(f), std::forward<Args>(args)...);
    }

    template <class Fn, class... Args>
        requires std::invocable<Fn, Args...>
    auto Dispatch(FiberPriority priority, Fn&& f, Args&&... args) -> void {
        if (!is_running_.load(std::memory_order_acquire)) {
            return;
        }

        PickUpEl()->Dispatch(priority, std::forward<Fn>(f), std::forward<Args>(args)...);
    }

    auto PickUpEl() -> EventLoop*;

    auto Stop() -> void;
    auto PoolSize() -> size_t { return pool_size_; }
    auto At(size_t i) -> EventLoop* { return els_[i].get(); }

    auto MapCpuToThreads(size_t cpu) -> std::vector<unsigned>& { return cpu_threads_[cpu]; }

private:
    auto SetupEls() -> void;

    std::atomic_bool is_running_;

    std::vector<std::unique_ptr<EventLoop>> els_;
    std::atomic_size_t                      next_el_{0};
    size_t                                  pool_size_;

    // abs cpu -> threads that pinned in that cpu.
    std::vector<std::vector<unsigned>> cpu_threads_;
};

} // namespace idlekv
