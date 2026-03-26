#pragma once

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <exception>
#include <functional>
#include <future>
#include <mutex>
#include <stdexcept>
#include <stop_token>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace idlekv {
namespace utils {

class ThreadPool {
public:
    explicit ThreadPool(size_t thread_count = 0) { Start(thread_count); }

    ~ThreadPool() { Stop(); }

    ThreadPool(const ThreadPool&)                    = delete;
    auto operator=(const ThreadPool&) -> ThreadPool& = delete;

    ThreadPool(ThreadPool&&)                    = delete;
    auto operator=(ThreadPool&&) -> ThreadPool& = delete;

    auto Start(size_t thread_count = 0) -> void {
        std::scoped_lock lk(mtx_);
        if (!workers_.empty()) {
            throw std::logic_error("ThreadPool::Start() called on a running pool");
        }

        accepting_ = true;

        const size_t worker_count = NormalizeThreadCount(thread_count);
        workers_.reserve(worker_count);
        for (size_t i = 0; i < worker_count; ++i) {
            workers_.emplace_back([this](std::stop_token st) { WorkerLoop(st); });
        }
    }

    auto Stop() -> void {
        std::vector<std::jthread> workers;
        {
            std::scoped_lock lk(mtx_);
            if (workers_.empty()) {
                accepting_ = false;
                return;
            }

            accepting_ = false;
            cv_.notify_all();
            workers.swap(workers_);
        }

        workers.clear();

        {
            std::scoped_lock lk(mtx_);
            idle_cv_.notify_all();
        }
    }

    auto WaitIdle() -> void {
        std::unique_lock lk(mtx_);
        idle_cv_.wait(lk, [this]() { return tasks_.empty() && active_workers_ == 0; });
    }

    auto Running() const -> bool {
        std::scoped_lock lk(mtx_);
        return accepting_;
    }

    auto ThreadCount() const -> size_t {
        std::scoped_lock lk(mtx_);
        return workers_.size();
    }

    auto PendingTasks() const -> size_t {
        std::scoped_lock lk(mtx_);
        return tasks_.size();
    }

    template <class Fn, class... Args>
        requires std::invocable<Fn, Args...>
    auto Post(Fn&& fn, Args&&... args) -> void {
        EnqueueTask(BindTask(std::forward<Fn>(fn), std::forward<Args>(args)...));
    }

    template <class Fn, class... Args>
        requires std::invocable<Fn, Args...>
    auto Submit(Fn&& fn, Args&&... args) -> std::future<std::invoke_result_t<Fn, Args...>> {
        using Result = std::invoke_result_t<Fn, Args...>;

        auto task = std::make_shared<std::packaged_task<Result()>>(
            [fn       = std::decay_t<Fn>(std::forward<Fn>(fn)),
             ... args = std::decay_t<Args>(std::forward<Args>(args))]() mutable {
                return std::invoke(std::move(fn), std::move(args)...);
            });

        auto fut = task->get_future();
        EnqueueTask([task = std::move(task)]() mutable { (*task)(); });
        return fut;
    }

private:
    template <class Fn, class... Args>
        requires std::invocable<Fn, Args...>
    static auto BindTask(Fn&& fn, Args&&... args) -> std::function<void()> {
        return [fn       = std::decay_t<Fn>(std::forward<Fn>(fn)),
                ... args = std::decay_t<Args>(std::forward<Args>(args))]() mutable {
            std::invoke(std::move(fn), std::move(args)...);
        };
    }

    auto EnqueueTask(std::function<void()> task) -> void {
        {
            std::scoped_lock lk(mtx_);
            if (!accepting_) {
                throw std::logic_error("ThreadPool is stopped");
            }
            tasks_.emplace_back(std::move(task));
        }
        cv_.notify_one();
    }

    auto WorkerLoop(std::stop_token st) -> void {
        for (;;) {
            std::function<void()> task;
            {
                std::unique_lock lk(mtx_);
                cv_.wait(lk, [this, st]() {
                    return st.stop_requested() || !tasks_.empty() || !accepting_;
                });

                if ((st.stop_requested() || !accepting_) && tasks_.empty()) {
                    break;
                }

                task = std::move(tasks_.front());
                tasks_.pop_front();
                ++active_workers_;
            }

            try {
                task();
            } catch (...) {
                // Keep worker threads alive for fire-and-forget jobs.
            }

            {
                std::scoped_lock lk(mtx_);
                --active_workers_;
                if (tasks_.empty() && active_workers_ == 0) {
                    idle_cv_.notify_all();
                }
            }
        }
    }

    static auto NormalizeThreadCount(size_t thread_count) -> size_t {
        if (thread_count != 0) {
            return thread_count;
        }

        const auto hw_threads = std::thread::hardware_concurrency();
        return hw_threads == 0 ? 1 : static_cast<size_t>(hw_threads);
    }

    mutable std::mutex                mtx_;
    std::condition_variable           cv_;
    std::condition_variable           idle_cv_;
    std::deque<std::function<void()>> tasks_;
    std::vector<std::jthread>         workers_;
    size_t                            active_workers_{0};
    bool                              accepting_{false};
};

} // namespace utils
} // namespace idlekv
