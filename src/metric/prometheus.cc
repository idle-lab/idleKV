#include "metric/prometheus.h"

#include <string>

namespace idlekv {

namespace {

auto AppendUint(std::string& out, uint64_t value) -> void { out += std::to_string(value); }

auto AppendInt(std::string& out, int64_t value) -> void { out += std::to_string(value); }

auto AppendDouble(std::string& out, double value) -> void {
    auto text = std::to_string(value);
    while (!text.empty() && text.back() == '0') {
        text.pop_back();
    }
    if (!text.empty() && text.back() == '.') {
        text.push_back('0');
    }
    out += text;
}

} // namespace

auto PrometheusMetrics::Instance() -> PrometheusMetrics& {
    static PrometheusMetrics metrics;
    return metrics;
}

auto PrometheusMetrics::OnConnectionAccepted() -> void {
    connections_accepted_total_.fetch_add(1, std::memory_order_relaxed);
    active_connections_.fetch_add(1, std::memory_order_relaxed);
}

auto PrometheusMetrics::OnConnectionClosed() -> void {
    auto current = active_connections_.load(std::memory_order_relaxed);
    while (true) {
        const auto next = current > 0 ? current - 1 : 0;
        if (active_connections_.compare_exchange_weak(current, next, std::memory_order_relaxed,
                                                      std::memory_order_relaxed)) {
            return;
        }
    }
}

auto PrometheusMetrics::OnErrorResponse() -> void {
    error_responses_total_.fetch_add(1, std::memory_order_relaxed);
}

auto PrometheusMetrics::AppendMetricHeader(std::string_view name, std::string_view type,
                                           std::string_view help, std::string& out) -> void {
    out += "# HELP ";
    out += name;
    out += ' ';
    out += help;
    out += '\n';
    out += "# TYPE ";
    out += name;
    out += ' ';
    out += type;
    out += '\n';
}

auto PrometheusMetrics::AppendCounter(std::string_view name, std::string_view help, uint64_t value,
                                      std::string& out) -> void {
    AppendMetricHeader(name, "counter", help, out);
    out += name;
    out += ' ';
    AppendUint(out, value);
    out += '\n';
}

auto PrometheusMetrics::AppendGauge(std::string_view name, std::string_view help, int64_t value,
                                    std::string& out) -> void {
    AppendMetricHeader(name, "gauge", help, out);
    out += name;
    out += ' ';
    AppendInt(out, value);
    out += '\n';
}

auto PrometheusMetrics::Histogram::AppendPrometheus(std::string_view name, std::string_view help,
                                                    std::string& out) const -> void {
    PrometheusMetrics::AppendMetricHeader(name, "histogram", help, out);

    uint64_t cumulative = 0;
    for (size_t i = 0; i < kBuckets.size(); ++i) {
        cumulative += bucket_counts_[i].load(std::memory_order_relaxed);
        out += name;
        out += "_bucket{le=\"";
        out += kBuckets[i].label;
        out += "\"} ";
        AppendUint(out, cumulative);
        out += '\n';
    }

    const auto total = count_.load(std::memory_order_relaxed);
    out += name;
    out += "_bucket{le=\"+Inf\"} ";
    AppendUint(out, total);
    out += '\n';

    out += name;
    out += "_sum ";
    AppendDouble(out, static_cast<double>(sum_ns_.load(std::memory_order_relaxed)) / 1'000'000'000.0);
    out += '\n';

    out += name;
    out += "_count ";
    AppendUint(out, total);
    out += '\n';

    (void)overflow_count_.load(std::memory_order_relaxed);
}

auto PrometheusMetrics::Render() const -> std::string {
    std::string out;
    out.reserve(4096);

    AppendGauge("idlekv_up", "Whether the idleKV process is up.", 1, out);
    out += '\n';

    AppendGauge("idlekv_connections_active", "Current number of active Redis connections.",
                active_connections_.load(std::memory_order_relaxed), out);
    out += '\n';

    AppendCounter("idlekv_connections_accepted_total",
                  "Total number of accepted Redis connections.",
                  connections_accepted_total_.load(std::memory_order_relaxed), out);
    out += '\n';

    AppendCounter("idlekv_requests_total", "Total number of completed Redis requests.",
                  requests_total_.load(std::memory_order_relaxed), out);
    out += '\n';

    AppendCounter("idlekv_error_responses_total",
                  "Total number of Redis error responses sent to clients.",
                  error_responses_total_.load(std::memory_order_relaxed), out);
    out += '\n';

    request_duration_.AppendPrometheus("idlekv_request_duration_seconds",
                                       "Redis request handling duration in seconds.", out);

    return out;
}

} // namespace idlekv
