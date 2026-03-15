#pragma once

#include "common/logger.h"

#include <array>
#include <cstddef>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace idlekv {

// multi-producer signal-consumer task queue.
class TaskQueue {
    using Task = std::function<void()>;

public:
    explicit TaskQueue(std::string name) : name_(std::move(name)) {}

    TaskQueue(const TaskQueue&)                    = delete;
    auto operator=(const TaskQueue&) -> TaskQueue& = delete;

    TaskQueue(TaskQueue&&)                    = delete;
    auto operator=(TaskQueue&&) -> TaskQueue& = delete;

    ~TaskQueue() { close(); }

    class TaskBuffer {
    public:
        template <class F>
        auto add(F&& fn) -> void { tasks_.emplace_back(std::forward<F>(fn)); }

        auto tasks() -> std::vector<Task>& { return tasks_; }
        auto clear() -> void { tasks_.clear(); }

    private:
        std::vector<Task> tasks_;
    };

    template <class F>
    auto add(F&& fn) -> void {
        std::unique_lock<std::mutex> lk(mu_);
        if (!started_) {
            return;
        }

        producer_buf_->add(std::forward<F>(fn));
        ++pending_tasks_;
        lk.unlock();
        cv_.notify_one();
    }

    auto start() -> void {
        std::lock_guard<std::mutex> lg(mu_);
        if (started_ || closing_) {
            return;
        }

        bufs_[0].clear();
        bufs_[1].clear();
        producer_buf_  = &bufs_[0];
        consumer_buf_  = &bufs_[1];
        pending_tasks_ = 0;
        started_       = true;
        consumer_ = std::jthread(&TaskQueue::work, this);
    }

    auto close() -> void {
        std::jthread consumer;
        {
            std::lock_guard<std::mutex> lg(mu_);
            if (closing_ || (!started_ && !consumer_.joinable())) {
                return;
            }

            started_ = false;
            closing_ = true;
            consumer = std::move(consumer_);
        }

        cv_.notify_all();
        if (consumer.joinable()) {
            consumer.join();
        }

        std::lock_guard<std::mutex> lg(mu_);
        closing_ = false;
    }

private:
    auto work() -> void {
        while (true) {
            TaskBuffer* ready_buf = nullptr;
            std::unique_lock<std::mutex> ul(mu_);

            cv_.wait(ul, [&]() -> bool {
                return !started_ || pending_tasks_ > 0;
            });

            if (!started_ && pending_tasks_ == 0) {
                return;
            }

            std::swap(producer_buf_, consumer_buf_);
            ready_buf = consumer_buf_;
            ul.unlock();

            const auto consumed_tasks = ready_buf->tasks().size();
            for (auto& task : ready_buf->tasks()) {
                task();
            }

            ready_buf->clear();

            ul.lock();
            pending_tasks_ -= consumed_tasks;
        }
    }

    std::string name_;

    std::array<TaskBuffer, 2> bufs_;
    TaskBuffer*               producer_buf_ = &bufs_[0];
    TaskBuffer*               consumer_buf_ = &bufs_[1];
    size_t                    pending_tasks_ = 0;
    bool                      started_       = false;
    bool                      closing_       = false;

    std::jthread consumer_;
    std::mutex mu_;
    std::condition_variable cv_;
};

} // namespace idlekv
