#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>

namespace idlekv {

class PrometheusMetrics {
public:
    static auto Instance() -> PrometheusMetrics&;

    auto OnConnectionAccepted() -> void;
    auto OnConnectionClosed() -> void;
    auto OnErrorResponse() -> void;

    template <class Rep, class Period>
    auto ObserveRequestDuration(std::chrono::duration<Rep, Period> dur) -> void {
        requests_total_.fetch_add(1, std::memory_order_relaxed);
        request_duration_.Observe(dur);
    }

    auto Render() const -> std::string;

private:
    struct HistogramBucket {
        uint64_t         upper_bound_ns;
        std::string_view label;
    };

    class Histogram {
    public:
        template <class Rep, class Period>
        auto Observe(std::chrono::duration<Rep, Period> dur) -> void {
            const auto ns = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(dur).count());

            sum_ns_.fetch_add(ns, std::memory_order_relaxed);
            count_.fetch_add(1, std::memory_order_relaxed);

            for (size_t i = 0; i < kBuckets.size(); ++i) {
                if (ns <= kBuckets[i].upper_bound_ns) {
                    bucket_counts_[i].fetch_add(1, std::memory_order_relaxed);
                    return;
                }
            }

            overflow_count_.fetch_add(1, std::memory_order_relaxed);
        }

        auto AppendPrometheus(std::string_view name, std::string_view help, std::string& out) const
            -> void;

    private:
        static constexpr std::array<HistogramBucket, 15> kBuckets{{
            {10'000ULL, "0.00001"},
            {25'000ULL, "0.000025"},
            {50'000ULL, "0.00005"},
            {100'000ULL, "0.0001"},
            {250'000ULL, "0.00025"},
            {500'000ULL, "0.0005"},
            {1'000'000ULL, "0.001"},
            {2'500'000ULL, "0.0025"},
            {5'000'000ULL, "0.005"},
            {10'000'000ULL, "0.01"},
            {25'000'000ULL, "0.025"},
            {50'000'000ULL, "0.05"},
            {100'000'000ULL, "0.1"},
            {250'000'000ULL, "0.25"},
            {1'000'000'000ULL, "1"},
        }};

        std::array<std::atomic<uint64_t>, kBuckets.size()> bucket_counts_{};
        std::atomic<uint64_t>                              overflow_count_{0};
        std::atomic<uint64_t>                              count_{0};
        std::atomic<uint64_t>                              sum_ns_{0};
    };

    PrometheusMetrics() = default;

    static auto AppendMetricHeader(std::string_view name, std::string_view type,
                                   std::string_view help, std::string& out) -> void;
    static auto AppendCounter(std::string_view name, std::string_view help, uint64_t value,
                              std::string& out) -> void;
    static auto AppendGauge(std::string_view name, std::string_view help, int64_t value,
                            std::string& out) -> void;

    std::atomic<uint64_t> requests_total_{0};
    std::atomic<uint64_t> error_responses_total_{0};
    std::atomic<uint64_t> connections_accepted_total_{0};
    std::atomic<int64_t>  active_connections_{0};
    Histogram             request_duration_{};
};

} // namespace idlekv
