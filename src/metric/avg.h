#pragma once

#include "common/logger.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <iomanip>
#include <mutex>
#include <sstream>
#include <stop_token>
#include <string>
#include <thread>
#include <utility>

namespace idlekv {

class Avg {
public:
    using clock = std::chrono::high_resolution_clock;

    class Scope {
    public:
        explicit Scope(Avg& avg) : avg_(&avg), start_(clock::now()) {}

        Scope(const Scope&)                    = delete;
        auto operator=(const Scope&) -> Scope& = delete;

        Scope(Scope&& other) noexcept : avg_(other.avg_), start_(other.start_) {
            other.avg_ = nullptr;
        }

        auto operator=(Scope&& other) noexcept -> Scope& {
            if (this == &other) {
                return *this;
            }
            finish();
            avg_       = other.avg_;
            start_     = other.start_;
            other.avg_ = nullptr;
            return *this;
        }

        ~Scope() { finish(); }

        auto finish() -> void {
            if (avg_ == nullptr) {
                return;
            }
            avg_->observe_since(start_);
            avg_ = nullptr;
        }

    private:
        Avg*              avg_{nullptr};
        clock::time_point start_;
    };

    explicit Avg(std::string name, clock::duration report_interval = std::chrono::seconds(1),
                 spdlog::level::level_enum level = spdlog::level::info, bool report_empty = false)
        : name_(std::move(name)), report_interval_(report_interval), level_(level),
          report_empty_(report_empty),
          reporter_([this](std::stop_token stop_token) { report_loop(stop_token); }) {}

    Avg(const Avg&)                    = delete;
    auto operator=(const Avg&) -> Avg& = delete;
    Avg(Avg&&)                         = delete;
    auto operator=(Avg&&) -> Avg&      = delete;

    ~Avg() { stop(); }

    template <class Rep, class Period>
    auto observe(std::chrono::duration<Rep, Period> dur) -> void {
        auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(dur).count();
        total_ns_.fetch_add(static_cast<uint64_t>(ns), std::memory_order_relaxed);
        total_count_.fetch_add(1, std::memory_order_relaxed);
        window_ns_.fetch_add(static_cast<uint64_t>(ns), std::memory_order_relaxed);
        window_count_.fetch_add(1, std::memory_order_relaxed);
    }

    auto observe_bytes(uint64_t bytes) -> void {
        total_bytes_.fetch_add(bytes, std::memory_order_relaxed);
        window_bytes_.fetch_add(bytes, std::memory_order_relaxed);
    }

    auto observe_since(clock::time_point start) -> void { observe(clock::now() - start); }

    auto scope() -> Scope { return Scope(*this); }

    auto average() const -> std::chrono::nanoseconds {
        auto count = total_count_.load(std::memory_order_relaxed);
        if (count == 0) {
            return std::chrono::nanoseconds{0};
        }
        auto total_ns = total_ns_.load(std::memory_order_relaxed);
        return std::chrono::nanoseconds(total_ns / count);
    }

    auto count() const -> uint64_t { return total_count_.load(std::memory_order_relaxed); }
    auto total_bytes() const -> uint64_t { return total_bytes_.load(std::memory_order_relaxed); }
    auto average_bytes() const -> double {
        auto count = total_count_.load(std::memory_order_relaxed);
        if (count == 0) {
            return 0.0;
        }

        auto total_bytes = total_bytes_.load(std::memory_order_relaxed);
        return static_cast<double>(total_bytes) / count;
    }

    auto report_now() -> void { report_once(); }

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

private:
    auto report_loop(std::stop_token stop_token) -> void {
        std::unique_lock lk(mu_);
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
        auto window_count = window_count_.exchange(0, std::memory_order_acq_rel);
        auto window_ns    = window_ns_.exchange(0, std::memory_order_acq_rel);
        auto window_bytes = window_bytes_.exchange(0, std::memory_order_acq_rel);

        if (window_count == 0 && window_bytes == 0 && !report_empty_) {
            return;
        }

        auto total_count = total_count_.load(std::memory_order_relaxed);
        auto total_ns    = total_ns_.load(std::memory_order_relaxed);
        auto total_bytes = total_bytes_.load(std::memory_order_relaxed);

        bool has_latency = window_count > 0 || total_count > 0;
        bool has_bytes   = window_bytes > 0 || total_bytes > 0;

        if (has_latency) {
            double window_avg_ns =
                window_count == 0 ? 0.0 : static_cast<double>(window_ns) / window_count;
            double total_avg_ns =
                total_count == 0 ? 0.0 : static_cast<double>(total_ns) / total_count;

            if (has_bytes) {
                double window_avg_bytes =
                    window_count == 0 ? 0.0 : static_cast<double>(window_bytes) / window_count;
                double total_avg_bytes =
                    total_count == 0 ? 0.0 : static_cast<double>(total_bytes) / total_count;
                double seconds = std::chrono::duration<double>(report_interval_).count();
                double window_rate =
                    seconds <= 0.0 ? 0.0 : static_cast<double>(window_bytes) / seconds;

                spdlog::log(level_,
                            "[avg:{}] window_avg={} window_count={} total_avg={} total_count={} "
                            "window_avg_bytes={} total_avg_bytes={} window_rate={} "
                            "window_bytes={} total_bytes={}",
                            name_, format_duration(window_avg_ns), window_count,
                            format_duration(total_avg_ns), total_count,
                            format_bytes(window_avg_bytes), format_bytes(total_avg_bytes),
                            format_rate(window_rate),
                            format_bytes(static_cast<double>(window_bytes)),
                            format_bytes(static_cast<double>(total_bytes)));
                return;
            }

            spdlog::log(level_,
                        "[avg:{}] window_avg={} window_count={} total_avg={} total_count={}", name_,
                        format_duration(window_avg_ns), window_count, format_duration(total_avg_ns),
                        total_count);
            return;
        }

        double seconds     = std::chrono::duration<double>(report_interval_).count();
        double window_rate = seconds <= 0.0 ? 0.0 : static_cast<double>(window_bytes) / seconds;

        spdlog::log(level_, "[avg:{}] window_rate={} window_bytes={} total_bytes={}", name_,
                    format_rate(window_rate), format_bytes(static_cast<double>(window_bytes)),
                    format_bytes(static_cast<double>(total_bytes)));
    }

    static auto format_duration(double ns) -> std::string {
        constexpr double kNsPerUs = 1000.0;
        constexpr double kNsPerMs = 1000.0 * kNsPerUs;
        constexpr double kNsPerS  = 1000.0 * kNsPerMs;

        std::ostringstream oss;
        oss << std::fixed << std::setprecision(2);

        if (ns >= kNsPerS) {
            oss << (ns / kNsPerS) << " s";
        } else if (ns >= kNsPerMs) {
            oss << (ns / kNsPerMs) << " ms";
        } else if (ns >= kNsPerUs) {
            oss << (ns / kNsPerUs) << " us";
        } else {
            oss << ns << " ns";
        }

        return oss.str();
    }

    static auto format_bytes(double bytes) -> std::string {
        constexpr double kB  = 1024.0;
        constexpr double kMB = 1024.0 * kB;
        constexpr double kGB = 1024.0 * kMB;

        std::ostringstream oss;
        oss << std::fixed << std::setprecision(2);

        if (bytes >= kGB) {
            oss << (bytes / kGB) << " GiB";
        } else if (bytes >= kMB) {
            oss << (bytes / kMB) << " MiB";
        } else if (bytes >= kB) {
            oss << (bytes / kB) << " KiB";
        } else {
            oss << bytes << " B";
        }

        return oss.str();
    }

    static auto format_rate(double bytes_per_sec) -> std::string {
        return format_bytes(bytes_per_sec) + "/s";
    }

    std::string               name_;
    clock::duration           report_interval_;
    spdlog::level::level_enum level_;
    bool                      report_empty_{false};

    std::atomic<uint64_t> total_ns_{0};
    std::atomic<uint64_t> total_count_{0};
    std::atomic<uint64_t> window_ns_{0};
    std::atomic<uint64_t> window_count_{0};
    std::atomic<uint64_t> total_bytes_{0};
    std::atomic<uint64_t> window_bytes_{0};
    std::atomic<bool>     stopped_{false};

    std::mutex              mu_;
    std::condition_variable cv_;
    std::jthread            reporter_;
};

} // namespace idlekv
