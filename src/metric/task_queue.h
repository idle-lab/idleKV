#pragma once

#ifdef DEBUG

#include "common/logger.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <iomanip>
#include <memory>
#include <mutex>
#include <sstream>
#include <stop_token>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace idlekv {

class TaskQueueMetricsRegistry {
public:
    using clock = std::chrono::steady_clock;

    class QueueMetrics {
    public:
        explicit QueueMetrics(std::string name) : name_(std::move(name)) {
            depth_samples_.reserve(2048);
            lock_wait_samples_.reserve(2048);
        }

        auto on_task_enqueued() -> size_t {
            enqueue_count_.fetch_add(1, std::memory_order_relaxed);
            const auto pending = pending_tasks_.fetch_add(1, std::memory_order_relaxed) + 1;
            update_max(window_max_pending_, pending);

            if ((depth_sequence_.fetch_add(1, std::memory_order_relaxed) & kSampleMask) == 0) {
                std::lock_guard<std::mutex> lk(depth_mu_);
                depth_samples_.push_back(pending);
            }

            return pending;
        }

        auto on_task_completed(size_t count = 1) -> size_t {
            if (count == 0) {
                return pending_tasks_.load(std::memory_order_relaxed);
            }

            completed_count_.fetch_add(count, std::memory_order_relaxed);
            total_completed_count_.fetch_add(count, std::memory_order_relaxed);
            return decrease_pending(count);
        }

        auto drop_pending(size_t count) -> size_t {
            if (count == 0) {
                return pending_tasks_.load(std::memory_order_relaxed);
            }
            return decrease_pending(count);
        }

        template <class Rep, class Period>
        auto observe_wake_to_first_task(std::chrono::duration<Rep, Period> dur) -> void {
            const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(dur).count();

            wake_to_first_task_count_.fetch_add(1, std::memory_order_relaxed);
            wake_to_first_task_count_total_.fetch_add(1, std::memory_order_relaxed);
            total_wake_to_first_task_ns_.fetch_add(static_cast<uint64_t>(ns),
                                                   std::memory_order_relaxed);
            window_wake_to_first_task_ns_.fetch_add(static_cast<uint64_t>(ns),
                                                    std::memory_order_relaxed);
            update_max(window_wake_to_first_task_max_ns_, static_cast<uint64_t>(ns));
        }

        template <class Rep, class Period>
        auto observe_lock_wait(std::chrono::duration<Rep, Period> dur, bool contended) -> void {
            lock_acquires_.fetch_add(1, std::memory_order_relaxed);
            if (contended) {
                contended_acquires_.fetch_add(1, std::memory_order_relaxed);
            }

            if ((lock_wait_sequence_.fetch_add(1, std::memory_order_relaxed) & kSampleMask) != 0) {
                return;
            }

            const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(dur).count();
            std::lock_guard<std::mutex> lk(lock_wait_mu_);
            lock_wait_samples_.push_back(static_cast<uint64_t>(ns));
        }

        auto report_once() -> void {
            const auto now = clock::now();
            const auto elapsed_seconds =
                std::max(std::chrono::duration<double>(now - last_report_at_).count(), 1e-9);
            last_report_at_ = now;

            const auto enqueue_count = enqueue_count_.exchange(0, std::memory_order_acq_rel);
            const auto completed_count = completed_count_.exchange(0, std::memory_order_acq_rel);
            const auto total_completed =
                total_completed_count_.load(std::memory_order_relaxed);
            const auto pending_max   = window_max_pending_.exchange(0, std::memory_order_acq_rel);
            const auto pending_now   = pending_tasks_.load(std::memory_order_relaxed);
            const auto wake_to_first_task_count =
                wake_to_first_task_count_.exchange(0, std::memory_order_acq_rel);
            const auto wake_to_first_task_ns =
                window_wake_to_first_task_ns_.exchange(0, std::memory_order_acq_rel);
            const auto wake_to_first_task_max =
                window_wake_to_first_task_max_ns_.exchange(0, std::memory_order_acq_rel);
            const auto lock_acquires = lock_acquires_.exchange(0, std::memory_order_acq_rel);
            const auto contended     = contended_acquires_.exchange(0, std::memory_order_acq_rel);
            const auto total_wake_to_first_task_count = wake_to_first_task_total_count();
            const auto total_wake_to_first_task_ns =
                total_wake_to_first_task_ns_.load(std::memory_order_relaxed);

            std::vector<uint64_t> depth_samples;
            {
                std::lock_guard<std::mutex> lk(depth_mu_);
                depth_samples.swap(depth_samples_);
            }

            std::vector<uint64_t> lock_wait_samples;
            {
                std::lock_guard<std::mutex> lk(lock_wait_mu_);
                lock_wait_samples.swap(lock_wait_samples_);
            }

            const bool has_window_activity =
                enqueue_count != 0 || completed_count != 0 || pending_max != 0 ||
                wake_to_first_task_count != 0 || lock_acquires != 0;
            if (!has_window_activity && pending_now == last_reported_pending_) {
                return;
            }
            last_reported_pending_ = pending_now;

            std::sort(depth_samples.begin(), depth_samples.end());
            std::sort(lock_wait_samples.begin(), lock_wait_samples.end());

            const auto depth_p50 = percentile(depth_samples, 0.50);
            const auto depth_p95 = percentile(depth_samples, 0.95);
            const auto depth_p99 = percentile(depth_samples, 0.99);
            const auto depth_max = depth_samples.empty() ? 0 : depth_samples.back();

            const auto lock_p50 = percentile(lock_wait_samples, 0.50);
            const auto lock_p95 = percentile(lock_wait_samples, 0.95);
            const auto lock_p99 = percentile(lock_wait_samples, 0.99);
            const auto lock_max = lock_wait_samples.empty() ? 0 : lock_wait_samples.back();
            const auto wake_to_first_task_avg =
                wake_to_first_task_count == 0 ? 0 : wake_to_first_task_ns / wake_to_first_task_count;
            const auto total_wake_to_first_task_avg =
                total_wake_to_first_task_count == 0
                    ? 0
                    : total_wake_to_first_task_ns / total_wake_to_first_task_count;
            const auto completed_rate = static_cast<double>(completed_count) / elapsed_seconds;

            const auto contention_ratio =
                lock_acquires == 0 ? 0.0
                                   : (100.0 * static_cast<double>(contended) /
                                      static_cast<double>(lock_acquires));

            spdlog::info(
                "[task-queue:{}] enqueue_count={} completed_count={} completed_rate={:.2f} task/s "
                "completed_total={} pending_now={} pending_max={} "
                "depth_sampled={} depth_p50={} depth_p95={} depth_p99={} depth_max={} "
                "wake_post_count={} wake_to_first_task_avg={} "
                "wake_to_first_task_max={} total_wake_to_first_task_avg={} "
                "lock_acquires={} contended={} contention={:.2f}% lock_sampled={} "
                "lock_wait_p50={} lock_wait_p95={} lock_wait_p99={} lock_wait_max={}",
                name_, enqueue_count, completed_count, completed_rate, total_completed,
                pending_now, pending_max, depth_samples.size(), depth_p50, depth_p95, depth_p99,
                depth_max, wake_to_first_task_count,
                format_duration_ns(wake_to_first_task_avg),
                format_duration_ns(wake_to_first_task_max),
                format_duration_ns(total_wake_to_first_task_avg), lock_acquires, contended,
                contention_ratio, lock_wait_samples.size(), format_duration_ns(lock_p50),
                format_duration_ns(lock_p95), format_duration_ns(lock_p99),
                format_duration_ns(lock_max));
        }

    private:
        static constexpr uint64_t kSampleMask = 0xFF;

        auto wake_to_first_task_total_count() const -> uint64_t {
            return wake_to_first_task_count_total_.load(std::memory_order_relaxed);
        }

        static auto percentile(const std::vector<uint64_t>& sorted, double q) -> uint64_t {
            if (sorted.empty()) {
                return 0;
            }
            if (sorted.size() == 1) {
                return sorted.front();
            }

            const double position = q * static_cast<double>(sorted.size() - 1);
            const auto   low      = static_cast<size_t>(position);
            const auto   high     = std::min(low + 1, sorted.size() - 1);
            const double weight   = position - static_cast<double>(low);

            const double blended = static_cast<double>(sorted[low]) * (1.0 - weight) +
                                   static_cast<double>(sorted[high]) * weight;
            return static_cast<uint64_t>(blended);
        }

        static auto format_duration_ns(uint64_t ns) -> std::string {
            constexpr double kNsPerUs = 1000.0;
            constexpr double kNsPerMs = 1000.0 * kNsPerUs;
            constexpr double kNsPerS  = 1000.0 * kNsPerMs;

            std::ostringstream oss;
            oss << std::fixed << std::setprecision(2);

            const auto value = static_cast<double>(ns);
            if (value >= kNsPerS) {
                oss << (value / kNsPerS) << " s";
            } else if (value >= kNsPerMs) {
                oss << (value / kNsPerMs) << " ms";
            } else if (value >= kNsPerUs) {
                oss << (value / kNsPerUs) << " us";
            } else {
                oss << value << " ns";
            }

            return oss.str();
        }

        static auto update_max(std::atomic<uint64_t>& target, uint64_t value) -> void {
            auto current = target.load(std::memory_order_relaxed);
            while (current < value &&
                   !target.compare_exchange_weak(current, value, std::memory_order_relaxed)) {
            }
        }

        auto decrease_pending(size_t count) -> size_t {
            auto current = pending_tasks_.load(std::memory_order_relaxed);
            while (true) {
                const auto next = count >= current ? 0U : static_cast<uint64_t>(current - count);
                if (pending_tasks_.compare_exchange_weak(current, next,
                                                         std::memory_order_relaxed)) {
                    return next;
                }
            }
        }

        std::string            name_;
        std::atomic<uint64_t>  enqueue_count_{0};
        std::atomic<uint64_t>  completed_count_{0};
        std::atomic<uint64_t>  total_completed_count_{0};
        std::atomic<uint64_t>  pending_tasks_{0};
        std::atomic<uint64_t>  window_max_pending_{0};
        std::atomic<uint64_t>  depth_sequence_{0};
        std::mutex             depth_mu_;
        std::vector<uint64_t>  depth_samples_;

        std::atomic<uint64_t>  wake_to_first_task_count_{0};
        std::atomic<uint64_t>  wake_to_first_task_count_total_{0};
        std::atomic<uint64_t>  window_wake_to_first_task_ns_{0};
        std::atomic<uint64_t>  total_wake_to_first_task_ns_{0};
        std::atomic<uint64_t>  window_wake_to_first_task_max_ns_{0};

        std::atomic<uint64_t>  lock_acquires_{0};
        std::atomic<uint64_t>  contended_acquires_{0};
        std::atomic<uint64_t>  lock_wait_sequence_{0};
        std::mutex             lock_wait_mu_;
        std::vector<uint64_t>  lock_wait_samples_;
        clock::time_point      last_report_at_{clock::now()};
        uint64_t               last_reported_pending_{0};
    };

    static auto instance() -> TaskQueueMetricsRegistry& {
        static TaskQueueMetricsRegistry registry;
        return registry;
    }

    auto register_queue(std::string name) -> std::shared_ptr<QueueMetrics> {
        auto metrics = std::make_shared<QueueMetrics>(std::move(name));
        {
            std::lock_guard<std::mutex> lk(mu_);
            queues_.push_back(metrics);
        }
        return metrics;
    }

    auto stop() -> void {
        bool expected = false;
        if (!stopped_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
            return;
        }

        cv_.notify_all();
        if (reporter_.joinable()) {
            reporter_.request_stop();
            reporter_.join();
        }
    }

    ~TaskQueueMetricsRegistry() { stop(); }

private:
    TaskQueueMetricsRegistry()
        : reporter_([this](std::stop_token stop_token) { report_loop(stop_token); }) {}

    TaskQueueMetricsRegistry(const TaskQueueMetricsRegistry&) = delete;
    auto operator=(const TaskQueueMetricsRegistry&) -> TaskQueueMetricsRegistry& = delete;

    auto report_loop(std::stop_token stop_token) -> void {
        std::unique_lock lk(cv_mu_);
        while (!stop_token.stop_requested() && !stopped_.load(std::memory_order_acquire)) {
            cv_.wait_for(lk, report_interval_, [this, &stop_token]() {
                return stop_token.stop_requested() || stopped_.load(std::memory_order_acquire);
            });

            if (stop_token.stop_requested() || stopped_.load(std::memory_order_acquire)) {
                break;
            }

            lk.unlock();
            report_once();
            lk.lock();
        }

        lk.unlock();
        report_once();
    }

    auto report_once() -> void {
        std::vector<std::shared_ptr<QueueMetrics>> queues;
        {
            std::lock_guard<std::mutex> lk(mu_);
            auto it = queues_.begin();
            while (it != queues_.end()) {
                if (auto queue = it->lock()) {
                    queues.push_back(std::move(queue));
                    ++it;
                } else {
                    it = queues_.erase(it);
                }
            }
        }

        for (const auto& queue : queues) {
            queue->report_once();
        }
    }

    std::chrono::seconds                     report_interval_{1};
    std::mutex                               mu_;
    std::vector<std::weak_ptr<QueueMetrics>> queues_;
    std::mutex                               cv_mu_;
    std::condition_variable                  cv_;
    std::atomic<bool>                        stopped_{false};
    std::jthread                             reporter_;
};

} // namespace idlekv

#else

#include <chrono>
#include <cstddef>
#include <memory>
#include <string>
#include <utility>

namespace idlekv {

class TaskQueueMetricsRegistry {
public:
    class QueueMetrics {
    public:
        explicit QueueMetrics(std::string) {}

        auto on_task_enqueued() -> size_t { return 0; }
        auto on_task_completed(size_t = 1) -> size_t { return 0; }
        auto drop_pending(size_t = 0) -> size_t { return 0; }

        template <class Rep, class Period>
        auto observe_wake_to_first_task(std::chrono::duration<Rep, Period>) -> void {}

        template <class Rep, class Period>
        auto observe_lock_wait(std::chrono::duration<Rep, Period>, bool) -> void {}
    };

    static auto instance() -> TaskQueueMetricsRegistry& {
        static TaskQueueMetricsRegistry registry;
        return registry;
    }

    auto register_queue(std::string name) -> std::shared_ptr<QueueMetrics> {
        return std::make_shared<QueueMetrics>(std::move(name));
    }

    auto stop() -> void {}

private:
    TaskQueueMetricsRegistry() = default;
};

} // namespace idlekv

#endif
