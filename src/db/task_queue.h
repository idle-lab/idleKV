#pragma once

#include "metric/task_queue.h"

#include <bit>
#include <condition_variable>
#include <atomic>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>

namespace idlekv {

template <class T>
class MPSCQueue {
    template <class Tp>
    struct Slot {
        std::atomic<size_t> seq;
        Tp data;
    };

public:
    explicit MPSCQueue(size_t size)
        : size_(normalize_capacity(size)),
          mask_(size_ - 1),
          buffer_(new Slot<T>[size_]) {
        static_assert(std::is_default_constructible_v<T>,
                      "MPSCQueue requires default-constructible slot storage");

        for (size_t i = 0; i < size_; ++i) {
            buffer_[i].seq.store(i, std::memory_order_relaxed);
        }
    }

    template <class U>
        requires std::assignable_from<T&, U&&>
    auto push(U&& value) -> bool {
        size_t pos = tail_.load(std::memory_order_relaxed);

        for (;;) {
            Slot<T>& slot = buffer_[pos & mask_];
            size_t   seq  = slot.seq.load(std::memory_order_acquire);
            const auto diff =
                static_cast<std::ptrdiff_t>(seq) - static_cast<std::ptrdiff_t>(pos);

            if (diff == 0) {
                if (tail_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed,
                                                std::memory_order_relaxed)) {
                    slot.data = std::forward<U>(value);
                    slot.seq.store(pos + 1, std::memory_order_release);
                    return true;
                }
                continue;
            }

            if (diff < 0) {
                return false;
            }

            pos = tail_.load(std::memory_order_relaxed);
        }
    }

    auto pop(T& out) -> bool {
        const size_t pos  = head_;
        Slot<T>&     slot = buffer_[pos & mask_];
        size_t       seq  = slot.seq.load(std::memory_order_acquire);
        const auto diff =
            static_cast<std::ptrdiff_t>(seq) - static_cast<std::ptrdiff_t>(pos + 1);

        if (diff != 0) {
            return false;
        }

        out = std::move(slot.data);
        slot.seq.store(pos + size_, std::memory_order_release);
        head_ = pos + 1;
        return true;
    }

    auto size() const -> size_t { return size_; }

    ~MPSCQueue() { delete [] buffer_; }
private:
    static auto normalize_capacity(size_t size) -> size_t {
        if (size <= 1) {
            return 2;
        }
        return std::bit_ceil(size);
    }

    size_t size_;
    size_t mask_;

    alignas(64) Slot<T>* buffer_;

    alignas(64) std::atomic<size_t> tail_{0};
    alignas(64) size_t head_{0};
};

// multi-producer signal-consumer task queue.
class TaskQueue {
    using Task = std::function<void()>;
public:
    explicit TaskQueue(std::string name)
        : name_(std::move(name)),
          metrics_(TaskQueueMetricsRegistry::instance().register_queue(name_)) {}

    TaskQueue(const TaskQueue&)                    = delete;
    auto operator=(const TaskQueue&) -> TaskQueue& = delete;

    TaskQueue(TaskQueue&&)                    = delete;
    auto operator=(TaskQueue&&) -> TaskQueue& = delete;

    ~TaskQueue() { close(); }

    template <class F>
    auto add(F&& fn) -> void {
        if (!started_.load(std::memory_order_acquire)) {
            return;
        }

        active_producers_.fetch_add(1, std::memory_order_acq_rel);
        if (!started_.load(std::memory_order_acquire)) {
            release_producer();
            return;
        }

        Task   task(std::forward<F>(fn));
        size_t spins = 0;
        while (!queue_.push(std::move(task))) {
            if (!started_.load(std::memory_order_acquire)) {
                release_producer();
                return;
            }

            if ((++spins & 0x3F) == 0) {
                std::this_thread::yield();
            }
        }

        pending_tasks_.fetch_add(1, std::memory_order_release);
        metrics_->on_task_enqueued();
        release_producer();
        cv_.notify_one();
    }

    auto start() -> void {
        std::lock_guard<std::mutex> lg(lifecycle_mu_);
        if (started_.load(std::memory_order_acquire) || consumer_.joinable()) {
            return;
        }

        started_.store(true, std::memory_order_release);
        consumer_ = std::jthread(&TaskQueue::work, this);
    }

    auto close() -> void {
        std::jthread consumer;
        {
            std::lock_guard<std::mutex> lg(lifecycle_mu_);
            if (!started_.exchange(false, std::memory_order_acq_rel) && !consumer_.joinable()) {
                return;
            }
            consumer = std::move(consumer_);
        }

        cv_.notify_all();
        if (consumer.joinable()) {
            consumer.join();
        }
    }

private:
    auto release_producer() -> void {
        active_producers_.fetch_sub(1, std::memory_order_acq_rel);
        cv_.notify_all();
    }

    auto work() -> void {
        Task task;
        while (true) {
            while (queue_.pop(task)) {
                task();
                task = Task{};
                pending_tasks_.fetch_sub(1, std::memory_order_acq_rel);
                metrics_->on_task_completed();
            }

            std::unique_lock<std::mutex> lk(wait_mu_);
            cv_.wait(lk, [&]() {
                return pending_tasks_.load(std::memory_order_acquire) > 0 ||
                       (!started_.load(std::memory_order_acquire) &&
                        active_producers_.load(std::memory_order_acquire) == 0);
            });

            if (!started_.load(std::memory_order_acquire) &&
                pending_tasks_.load(std::memory_order_acquire) == 0 &&
                active_producers_.load(std::memory_order_acquire) == 0) {
                return;
            }
        }
    }

    std::string name_;
    std::shared_ptr<TaskQueueMetricsRegistry::QueueMetrics> metrics_;

    MPSCQueue<Task> queue_{1 << 10};
    std::atomic<size_t> pending_tasks_{0};
    std::atomic<size_t> active_producers_{0};

    std::jthread consumer_;
    std::mutex lifecycle_mu_;
    std::mutex wait_mu_;
    std::condition_variable cv_;
    std::atomic_bool started_{false};
};


} // namespace idlekv
