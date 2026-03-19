#pragma once

#include "common/logger.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <iomanip>
#include <mutex>
#include <sstream>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace idlekv {

class RequestStageMetrics {
public:
    using clock = std::chrono::steady_clock;

    struct ParseBreakdown {
        uint64_t total_ns{0};
        uint64_t io_wait_ns{0};
        uint64_t decode_ns{0};
    };

    struct SlowRequestBreakdown {
        ParseBreakdown parse;
        uint64_t       total_ns{0};
        uint64_t       command_prepare_ns{0};
        uint64_t       exec_ns{0};
        uint64_t       reply_encode_ns{0};
        uint64_t       flush_ns{0};
        size_t         arg_count{0};
        bool           pipelined{false};
        bool           flushed{false};
        bool           parse_failed{false};
        std::string    cmd_name;
        std::string    peer;
        std::string    note;
    };

    static constexpr auto SlowRequestThreshold() -> std::chrono::nanoseconds {
        return std::chrono::microseconds(3000000);
    }

    static auto Instance() -> RequestStageMetrics& {
        static RequestStageMetrics metrics;
        return metrics;
    }

    static auto FormatDurationNs(uint64_t ns) -> std::string {
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

    static auto FormatSlowRequest(const SlowRequestBreakdown& breakdown) -> std::string {
        const auto send_total_ns = breakdown.reply_encode_ns + breakdown.flush_ns;
        const auto accounted_ns =
            breakdown.parse.total_ns + breakdown.command_prepare_ns + breakdown.exec_ns +
            breakdown.reply_encode_ns + breakdown.flush_ns;
        const auto other_ns =
            accounted_ns >= breakdown.total_ns ? 0 : (breakdown.total_ns - accounted_ns);

        std::ostringstream oss;
        oss << "[slow-request] cmd="
            << (breakdown.cmd_name.empty() ? "<parse-error>" : breakdown.cmd_name)
            << " argc=" << breakdown.arg_count
            << " total=" << FormatDurationNs(breakdown.total_ns)
            << " parse=" << FormatDurationNs(breakdown.parse.total_ns)
            << " parse_io_wait=" << FormatDurationNs(breakdown.parse.io_wait_ns)
            << " parse_decode=" << FormatDurationNs(breakdown.parse.decode_ns)
            << " cmd_prepare=" << FormatDurationNs(breakdown.command_prepare_ns)
            << " exec=" << FormatDurationNs(breakdown.exec_ns)
            << " send=" << FormatDurationNs(send_total_ns)
            << " reply_encode=" << FormatDurationNs(breakdown.reply_encode_ns)
            << " flush=" << FormatDurationNs(breakdown.flush_ns)
            << " other=" << FormatDurationNs(other_ns)
            << " pipelined=" << std::boolalpha << breakdown.pipelined
            << " flushed=" << breakdown.flushed
            << " parse_failed=" << breakdown.parse_failed
            << " peer=" << (breakdown.peer.empty() ? "-" : breakdown.peer);

        if (!breakdown.note.empty()) {
            oss << " note=\"" << breakdown.note << "\"";
        }

        return oss.str();
    }

    static auto ShouldReportSlowRequest(uint64_t total_ns) -> bool {
        return total_ns >= static_cast<uint64_t>(SlowRequestThreshold().count());
    }

    auto MaybeReportSlowRequest(const SlowRequestBreakdown& breakdown) -> void {
        if (!ShouldReportSlowRequest(breakdown.total_ns)) {
            return;
        }

        spdlog::warn("{}", FormatSlowRequest(breakdown));
    }

    template <class Rep, class Period>
    auto ObserveCmdParse(std::chrono::duration<Rep, Period> dur) -> void {
        ObserveStage(cmd_parse_, dur);
    }

    template <class Rep, class Period>
    auto ObserveQueueToShard(std::chrono::duration<Rep, Period> dur) -> void {
        ObserveStage(queue_to_shard_, dur);
    }

    template <class Rep, class Period>
    auto ObserveExecOnShard(std::chrono::duration<Rep, Period> dur) -> void {
        ObserveStage(exec_on_shard_, dur);
    }

    template <class Rep, class Period>
    auto ObserveQueueToSend(std::chrono::duration<Rep, Period> dur) -> void {
        ObserveStage(queue_to_send_, dur);
    }

    template <class Rep, class Period>
    auto ObserveFlushTime(std::chrono::duration<Rep, Period> dur) -> void {
        ObserveStage(flush_time_, dur);
    }

    auto Stop() -> void {
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

    ~RequestStageMetrics() { Stop(); }

private:
    struct StageWindow {
        explicit StageWindow(std::string_view stage_name) : name(stage_name) { samples.reserve(4096); }

        std::string_view        name;
        std::atomic<uint64_t>   seen{0};
        std::atomic<uint64_t>   sequence{0};
        std::mutex              mu;
        std::vector<uint64_t>   samples;
    };

    static constexpr uint64_t kSampleMask = 0xFF;

    RequestStageMetrics()
        : cmd_parse_("cmd_parse"),
          queue_to_shard_("queue_to_shard"),
          exec_on_shard_("exec_on_shard"),
          queue_to_send_("queue_to_send"),
          flush_time_("flush_time"),
          reporter_([this](std::stop_token stop_token) { ReportLoop(stop_token); }) {}

    RequestStageMetrics(const RequestStageMetrics&) = delete;
    auto operator=(const RequestStageMetrics&) -> RequestStageMetrics& = delete;

    template <class Rep, class Period>
    auto ObserveStage(StageWindow& stage, std::chrono::duration<Rep, Period> dur) -> void {
        stage.seen.fetch_add(1, std::memory_order_relaxed);
        if ((stage.sequence.fetch_add(1, std::memory_order_relaxed) & kSampleMask) != 0) {
            return;
        }

        const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(dur).count();
        std::lock_guard<std::mutex> lk(stage.mu);
        stage.samples.push_back(static_cast<uint64_t>(ns));
    }

    auto ReportLoop(std::stop_token stop_token) -> void {
        std::unique_lock lk(mu_);
        while (!stop_token.stop_requested() && !stopped_.load(std::memory_order_acquire)) {
            cv_.wait_for(lk, report_interval_, [this, &stop_token]() {
                return stop_token.stop_requested() || stopped_.load(std::memory_order_acquire);
            });

            if (stop_token.stop_requested() || stopped_.load(std::memory_order_acquire)) {
                break;
            }

            lk.unlock();
            ReportOnce();
            lk.lock();
        }

        lk.unlock();
        ReportOnce();
    }

    auto ReportOnce() -> void {
        ReportStage(cmd_parse_);
        // ReportStage(queue_to_shard_);
        ReportStage(exec_on_shard_);
        ReportStage(queue_to_send_);
        ReportStage(flush_time_);
    }

    auto ReportStage(StageWindow& stage) -> void {
        auto seen = stage.seen.exchange(0, std::memory_order_acq_rel);
        if (seen == 0) {
            return;
        }

        std::vector<uint64_t> samples;
        {
            std::lock_guard<std::mutex> lk(stage.mu);
            samples.swap(stage.samples);
        }

        if (samples.empty()) {
            spdlog::info("[stage:{}] window_count={} sampled=0", stage.name, seen);
            return;
        }

        std::sort(samples.begin(), samples.end());
        const auto p50 = Percentile(samples, 0.50);
        const auto p95 = Percentile(samples, 0.95);
        const auto p99 = Percentile(samples, 0.99);
        const auto max = samples.back();

        spdlog::info("[stage:{}] window_count={} sampled={} p50={} p95={} p99={} max={}",
                     stage.name, seen, samples.size(), FormatDurationNs(p50),
                     FormatDurationNs(p95), FormatDurationNs(p99),
                     FormatDurationNs(max));
    }

    static auto Percentile(const std::vector<uint64_t>& sorted, double q) -> uint64_t {
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

    StageWindow           cmd_parse_;
    StageWindow           queue_to_shard_;
    StageWindow           exec_on_shard_;
    StageWindow           queue_to_send_;
    StageWindow           flush_time_;
    std::chrono::seconds  report_interval_{1};
    std::mutex            mu_;
    std::condition_variable cv_;
    std::atomic<bool>     stopped_{false};
    std::jthread          reporter_;
};

} // namespace idlekv
