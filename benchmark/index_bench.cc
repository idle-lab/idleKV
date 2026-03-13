#include "db/storage/dash/dash.h"

#include "third_part/CLI11/CLI11.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <barrier>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <numeric>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <sys/resource.h>
#include <unistd.h>

namespace idlekv::benchmark {

namespace {

using Clock = std::chrono::steady_clock;

enum class Scenario {
    kLoad,
    kRead,
    kMixed,
    kAll,
};

enum class OutputFormat {
    kText,
    kCsv,
};

enum class OperationKind : size_t {
    kRead = 0,
    kUpsert,
    kErase,
};

constexpr std::array<OperationKind, 3> kOperationKinds{
    OperationKind::kRead,
    OperationKind::kUpsert,
    OperationKind::kErase,
};

auto to_string(OperationKind kind) -> std::string_view {
    switch (kind) {
    case OperationKind::kRead:
        return "read";
    case OperationKind::kUpsert:
        return "upsert";
    case OperationKind::kErase:
        return "erase";
    }
    return "unknown";
}

auto to_string(Scenario scenario) -> std::string_view {
    switch (scenario) {
    case Scenario::kLoad:
        return "load";
    case Scenario::kRead:
        return "read";
    case Scenario::kMixed:
        return "mixed";
    case Scenario::kAll:
        return "all";
    }
    return "unknown";
}

auto operation_index(OperationKind kind) -> size_t {
    return static_cast<size_t>(kind);
}

struct BenchmarkConfig {
    std::string index_name             = "dash";
    Scenario    scenario               = Scenario::kAll;
    OutputFormat format                = OutputFormat::kText;
    size_t      threads                = 4;
    uint64_t    initial_keys           = 1U << 20U;
    uint64_t    operations             = 1U << 20U;
    uint64_t    key_space              = 0;
    uint64_t    seed                   = 42;
    double      read_ratio             = 0.80;
    double      upsert_ratio           = 0.15;
    double      erase_ratio            = 0.05;
    double      read_miss_ratio        = 0.10;
    uint64_t    sample_rate            = 1024;
    uint64_t    memory_sample_interval = 2;
    size_t      dash_initial_depth     = 1;
    double      dash_merge_threshold   = 0.20;
};

class IIndex {
public:
    virtual ~IIndex() = default;

    virtual auto name() const -> std::string_view = 0;
    virtual auto upsert(uint64_t key, uint64_t value) -> bool = 0;
    virtual auto find(uint64_t key) -> std::optional<uint64_t> = 0;
    virtual auto erase(uint64_t key) -> bool = 0;
    virtual auto size() const -> size_t = 0;
    virtual auto metrics() const -> std::vector<std::pair<std::string, std::string>> = 0;
};

class DashIndex final : public IIndex {
public:
    explicit DashIndex(const BenchmarkConfig& config)
        : table_({.initial_global_depth = config.dash_initial_depth,
                  .merge_threshold      = config.dash_merge_threshold}) {}

    auto name() const -> std::string_view override { return "dash"; }

    auto upsert(uint64_t key, uint64_t value) -> bool override {
        while (true) {
            if (table_.insert(key, value)) {
                return true;
            }
            if (table_.erase(key)) {
                continue;
            }
        }
    }

    auto find(uint64_t key) -> std::optional<uint64_t> override { return table_.find(key); }

    auto erase(uint64_t key) -> bool override { return table_.erase(key); }

    auto size() const -> size_t override { return table_.size(); }

    auto metrics() const -> std::vector<std::pair<std::string, std::string>> override {
        const auto stats = table_.stats();
        return {
            {"directory_depth", std::to_string(table_.directory_depth())},
            {"directory_size", std::to_string(table_.directory_size())},
            {"unique_segments", std::to_string(table_.unique_segments())},
            {"split_count", std::to_string(stats.split_count)},
            {"merge_count", std::to_string(stats.merge_count)},
            {"directory_growth_count", std::to_string(stats.directory_growth_count)},
            {"directory_shrink_count", std::to_string(stats.directory_shrink_count)},
        };
    }

private:
    dash::DashEH<uint64_t, uint64_t> table_;
};

using IndexFactory = std::unique_ptr<IIndex> (*)(const BenchmarkConfig&);

struct IndexDescriptor {
    std::string_view name;
    std::string_view description;
    IndexFactory     factory = nullptr;
};

auto make_dash_index(const BenchmarkConfig& config) -> std::unique_ptr<IIndex> {
    return std::make_unique<DashIndex>(config);
}

auto build_registry() -> std::vector<IndexDescriptor> {
    return {
        {
            .name        = "dash",
            .description = "DASH extendible hash",
            .factory     = make_dash_index,
        },
    };
}

auto find_descriptor(std::string_view name, const std::vector<IndexDescriptor>& registry)
    -> const IndexDescriptor* {
    for (const auto& descriptor : registry) {
        if (descriptor.name == name) {
            return &descriptor;
        }
    }
    return nullptr;
}

auto splitmix64(uint64_t x) -> uint64_t {
    x += 0x9e3779b97f4a7c15ULL;
    x = (x ^ (x >> 30U)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27U)) * 0x94d049bb133111ebULL;
    return x ^ (x >> 31U);
}

class FastRandom {
public:
    explicit FastRandom(uint64_t seed) : state_(seed) {}

    auto next() -> uint64_t {
        state_ = splitmix64(state_);
        return state_;
    }

    auto uniform(uint64_t upper_bound) -> uint64_t {
        if (upper_bound <= 1) {
            return 0;
        }
        return next() % upper_bound;
    }

private:
    uint64_t state_;
};

auto read_current_rss_bytes() -> uint64_t {
    std::ifstream input("/proc/self/statm");
    uint64_t      total_pages    = 0;
    uint64_t      resident_pages = 0;
    input >> total_pages >> resident_pages;
    (void)total_pages;
    if (!input) {
        return 0;
    }
    const auto page_size = static_cast<uint64_t>(::sysconf(_SC_PAGESIZE));
    return resident_pages * page_size;
}

auto read_peak_rss_bytes() -> uint64_t {
    struct rusage usage {
    };
    if (::getrusage(RUSAGE_SELF, &usage) != 0) {
        return 0;
    }
    return static_cast<uint64_t>(usage.ru_maxrss) * 1024ULL;
}

class MemorySampler {
public:
    explicit MemorySampler(std::chrono::milliseconds interval) : interval_(interval) {}

    void start() {
        stop_.store(false, std::memory_order_release);
        peak_bytes_.store(read_current_rss_bytes(), std::memory_order_release);
        worker_ = std::thread([this] {
            while (!stop_.load(std::memory_order_acquire)) {
                update_peak(read_current_rss_bytes());
                std::this_thread::sleep_for(interval_);
            }
            update_peak(read_current_rss_bytes());
        });
    }

    auto stop() -> uint64_t {
        stop_.store(true, std::memory_order_release);
        if (worker_.joinable()) {
            worker_.join();
        }
        return peak_bytes_.load(std::memory_order_acquire);
    }

private:
    void update_peak(uint64_t rss_bytes) {
        auto current = peak_bytes_.load(std::memory_order_relaxed);
        while (rss_bytes > current &&
               !peak_bytes_.compare_exchange_weak(current, rss_bytes,
                                                  std::memory_order_release,
                                                  std::memory_order_relaxed)) {
        }
    }

    std::chrono::milliseconds interval_;
    std::atomic<bool>         stop_{false};
    std::atomic<uint64_t>     peak_bytes_{0};
    std::thread               worker_;
};

auto format_bytes(uint64_t bytes) -> std::string {
    constexpr std::array<std::string_view, 5> kUnits{"B", "KiB", "MiB", "GiB", "TiB"};

    double value = static_cast<double>(bytes);
    size_t unit  = 0;
    while (value >= 1024.0 && unit + 1 < kUnits.size()) {
        value /= 1024.0;
        ++unit;
    }

    std::ostringstream out;
    out << std::fixed << std::setprecision(unit == 0 ? 0 : 2) << value << ' ' << kUnits[unit];
    return out.str();
}

auto format_rate(double ops_per_sec) -> std::string {
    std::ostringstream out;
    out << std::fixed << std::setprecision(2) << ops_per_sec;
    return out.str();
}

auto format_latency(double ns) -> std::string {
    std::ostringstream out;
    out << std::fixed;
    if (ns >= 1'000'000.0) {
        out << std::setprecision(2) << (ns / 1'000'000.0) << " ms";
    } else if (ns >= 1'000.0) {
        out << std::setprecision(2) << (ns / 1'000.0) << " us";
    } else {
        out << std::setprecision(2) << ns << " ns";
    }
    return out.str();
}

struct LatencySummary {
    uint64_t sample_count = 0;
    double   mean_ns      = 0.0;
    uint64_t min_ns       = 0;
    uint64_t p50_ns       = 0;
    uint64_t p95_ns       = 0;
    uint64_t p99_ns       = 0;
    uint64_t max_ns       = 0;
};

struct OperationStats {
    uint64_t              count   = 0;
    uint64_t              success = 0;
    uint64_t              fail    = 0;
    std::vector<uint64_t> latency_samples_ns;
};

struct ThreadStats {
    std::array<OperationStats, kOperationKinds.size()> operations;
};

struct OperationSummary {
    OperationKind  kind = OperationKind::kRead;
    uint64_t       count = 0;
    uint64_t       success = 0;
    uint64_t       fail = 0;
    LatencySummary latency;
};

struct PhaseReport {
    std::string                                     phase_name;
    double                                          duration_seconds      = 0.0;
    double                                          throughput_ops_per_sec = 0.0;
    uint64_t                                        rss_before_bytes       = 0;
    uint64_t                                        rss_after_bytes        = 0;
    uint64_t                                        rss_peak_bytes         = 0;
    uint64_t                                        process_peak_bytes     = 0;
    size_t                                          final_size             = 0;
    std::array<OperationSummary, kOperationKinds.size()> operation_summaries{};
    std::vector<std::pair<std::string, std::string>> index_metrics;
};

auto percentile(const std::vector<uint64_t>& sorted, double q) -> uint64_t {
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

    const double blended =
        static_cast<double>(sorted[low]) * (1.0 - weight) + static_cast<double>(sorted[high]) * weight;
    return static_cast<uint64_t>(blended);
}

auto summarize_latency(std::vector<uint64_t> samples) -> LatencySummary {
    if (samples.empty()) {
        return {};
    }

    std::sort(samples.begin(), samples.end());

    const auto sum = std::accumulate(samples.begin(), samples.end(), 0.0);
    return {
        .sample_count = static_cast<uint64_t>(samples.size()),
        .mean_ns      = sum / static_cast<double>(samples.size()),
        .min_ns       = samples.front(),
        .p50_ns       = percentile(samples, 0.50),
        .p95_ns       = percentile(samples, 0.95),
        .p99_ns       = percentile(samples, 0.99),
        .max_ns       = samples.back(),
    };
}

template <class Worker>
auto run_parallel_phase(const BenchmarkConfig& config, std::string phase_name, Worker&& worker)
    -> PhaseReport {
    std::vector<std::thread> workers;
    workers.reserve(config.threads);

    std::vector<ThreadStats> thread_stats(config.threads);
    std::barrier             sync_point(static_cast<std::ptrdiff_t>(config.threads + 1));

    for (size_t thread_id = 0; thread_id < config.threads; ++thread_id) {
        workers.emplace_back([&, thread_id] {
            worker(thread_id, thread_stats[thread_id], sync_point);
        });
    }

    const uint64_t rss_before = read_current_rss_bytes();
    MemorySampler  sampler(std::chrono::milliseconds(config.memory_sample_interval));
    sampler.start();

    sync_point.arrive_and_wait();
    const auto start = Clock::now();

    for (auto& worker_thread : workers) {
        worker_thread.join();
    }

    const auto end = Clock::now();

    PhaseReport report;
    report.phase_name          = std::move(phase_name);
    report.duration_seconds    =
        std::chrono::duration_cast<std::chrono::duration<double>>(end - start).count();
    report.rss_before_bytes    = rss_before;
    report.rss_after_bytes     = read_current_rss_bytes();
    report.rss_peak_bytes      = sampler.stop();
    report.process_peak_bytes  = read_peak_rss_bytes();

    for (size_t op = 0; op < kOperationKinds.size(); ++op) {
        std::vector<uint64_t> samples;
        uint64_t             count   = 0;
        uint64_t             success = 0;
        uint64_t             fail    = 0;

        for (auto& stats : thread_stats) {
            auto& local = stats.operations[op];
            count += local.count;
            success += local.success;
            fail += local.fail;
            samples.insert(samples.end(), local.latency_samples_ns.begin(),
                           local.latency_samples_ns.end());
        }

        report.operation_summaries[op] = {
            .kind    = kOperationKinds[op],
            .count   = count,
            .success = success,
            .fail    = fail,
            .latency = summarize_latency(std::move(samples)),
        };
        report.throughput_ops_per_sec += static_cast<double>(count);
    }

    if (report.duration_seconds > 0.0) {
        report.throughput_ops_per_sec /= report.duration_seconds;
    }

    return report;
}

void maybe_record_latency(OperationStats& stats, uint64_t sample_rate, uint64_t latency_ns) {
    if (sample_rate == 1 || stats.count % sample_rate == 0) {
        stats.latency_samples_ns.push_back(latency_ns);
    }
}

auto compose_value(uint64_t key, uint64_t salt) -> uint64_t {
    return splitmix64(key ^ salt);
}

void execute_upsert(IIndex& index, OperationStats& stats, uint64_t key, uint64_t value,
                    uint64_t sample_rate) {
    const bool should_sample = sample_rate == 1 || stats.count % sample_rate == 0;
    bool       success       = false;

    if (should_sample) {
        const auto start = Clock::now();
        success          = index.upsert(key, value);
        const auto end   = Clock::now();
        maybe_record_latency(
            stats, sample_rate,
            static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count()));
    } else {
        success = index.upsert(key, value);
    }

    ++stats.count;
    if (success) {
        ++stats.success;
    } else {
        ++stats.fail;
    }
}

void execute_read(IIndex& index, OperationStats& stats, uint64_t key, uint64_t sample_rate) {
    const bool should_sample = sample_rate == 1 || stats.count % sample_rate == 0;
    std::optional<uint64_t> value;

    if (should_sample) {
        const auto start = Clock::now();
        value            = index.find(key);
        const auto end   = Clock::now();
        maybe_record_latency(
            stats, sample_rate,
            static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count()));
    } else {
        value = index.find(key);
    }

    ++stats.count;
    if (value.has_value()) {
        ++stats.success;
    } else {
        ++stats.fail;
    }
}

void execute_erase(IIndex& index, OperationStats& stats, uint64_t key, uint64_t sample_rate) {
    const bool should_sample = sample_rate == 1 || stats.count % sample_rate == 0;
    bool       erased        = false;

    if (should_sample) {
        const auto start = Clock::now();
        erased           = index.erase(key);
        const auto end   = Clock::now();
        maybe_record_latency(
            stats, sample_rate,
            static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count()));
    } else {
        erased = index.erase(key);
    }

    ++stats.count;
    if (erased) {
        ++stats.success;
    } else {
        ++stats.fail;
    }
}

auto shard_range(uint64_t total, size_t shards, size_t shard_id) -> std::pair<uint64_t, uint64_t> {
    const auto base      = total / shards;
    const auto remainder = total % shards;
    const auto begin     = base * shard_id + std::min<uint64_t>(shard_id, remainder);
    const auto count     = base + (static_cast<uint64_t>(shard_id) < remainder ? 1U : 0U);
    return {begin, count};
}

auto run_load_phase(IIndex& index, const BenchmarkConfig& config) -> PhaseReport {
    auto report = run_parallel_phase(
        config, "load",
        [&](size_t thread_id, ThreadStats& stats, std::barrier<>& sync_point) {
            auto& upsert_stats = stats.operations[operation_index(OperationKind::kUpsert)];
            const auto [begin, count] = shard_range(config.initial_keys, config.threads, thread_id);

            sync_point.arrive_and_wait();

            for (uint64_t offset = 0; offset < count; ++offset) {
                const auto key = begin + offset;
                execute_upsert(index, upsert_stats, key, compose_value(key, config.seed),
                               config.sample_rate);
            }
        });

    report.final_size     = index.size();
    report.index_metrics  = index.metrics();
    return report;
}

auto run_read_phase(IIndex& index, const BenchmarkConfig& config) -> PhaseReport {
    const auto miss_threshold =
        static_cast<uint64_t>(config.read_miss_ratio * static_cast<double>(std::numeric_limits<uint64_t>::max()));

    auto report = run_parallel_phase(
        config, "read",
        [&](size_t thread_id, ThreadStats& stats, std::barrier<>& sync_point) {
            auto&      read_stats       = stats.operations[operation_index(OperationKind::kRead)];
            const auto [ignored_begin, count] =
                shard_range(config.operations, config.threads, thread_id);
            (void)ignored_begin;
            FastRandom rng(splitmix64(config.seed ^ (0x100000001b3ULL + thread_id)));

            sync_point.arrive_and_wait();

            for (uint64_t i = 0; i < count; ++i) {
                const bool miss = rng.next() < miss_threshold;
                const auto key  = miss ? (config.key_space + rng.uniform(config.initial_keys))
                                       : rng.uniform(config.initial_keys);
                execute_read(index, read_stats, key, config.sample_rate);
            }
        });

    report.final_size     = index.size();
    report.index_metrics  = index.metrics();
    return report;
}

auto pick_operation(FastRandom& rng, const BenchmarkConfig& config) -> OperationKind {
    const double total_ratio = config.read_ratio + config.upsert_ratio + config.erase_ratio;
    const double read_cutoff = config.read_ratio / total_ratio;
    const double write_cutoff = (config.read_ratio + config.upsert_ratio) / total_ratio;
    const double unit = static_cast<double>(rng.next()) /
                        static_cast<double>(std::numeric_limits<uint64_t>::max());

    if (unit < read_cutoff) {
        return OperationKind::kRead;
    }
    if (unit < write_cutoff) {
        return OperationKind::kUpsert;
    }
    return OperationKind::kErase;
}

auto run_mixed_phase(IIndex& index, const BenchmarkConfig& config) -> PhaseReport {
    const auto miss_threshold =
        static_cast<uint64_t>(config.read_miss_ratio * static_cast<double>(std::numeric_limits<uint64_t>::max()));
    const uint64_t mutable_begin = config.initial_keys;
    const uint64_t mutable_span  = std::max<uint64_t>(1, config.key_space - config.initial_keys);

    auto report = run_parallel_phase(
        config, "mixed",
        [&](size_t thread_id, ThreadStats& stats, std::barrier<>& sync_point) {
            auto&      read_stats   = stats.operations[operation_index(OperationKind::kRead)];
            auto&      upsert_stats = stats.operations[operation_index(OperationKind::kUpsert)];
            auto&      erase_stats  = stats.operations[operation_index(OperationKind::kErase)];
            const auto [ignored_begin, count] =
                shard_range(config.operations, config.threads, thread_id);
            (void)ignored_begin;
            FastRandom rng(splitmix64(config.seed ^ (0x9e3779b97f4a7c15ULL + thread_id)));

            sync_point.arrive_and_wait();

            for (uint64_t i = 0; i < count; ++i) {
                const auto operation = pick_operation(rng, config);
                switch (operation) {
                case OperationKind::kRead: {
                    const bool miss = rng.next() < miss_threshold;
                    const auto key  = miss ? (config.key_space + rng.uniform(config.initial_keys))
                                           : rng.uniform(config.initial_keys);
                    execute_read(index, read_stats, key, config.sample_rate);
                    break;
                }
                case OperationKind::kUpsert: {
                    const auto key = mutable_begin + rng.uniform(mutable_span);
                    execute_upsert(index, upsert_stats, key,
                                   compose_value(key, config.seed ^ (thread_id + i + 1)),
                                   config.sample_rate);
                    break;
                }
                case OperationKind::kErase: {
                    const auto key = mutable_begin + rng.uniform(mutable_span);
                    execute_erase(index, erase_stats, key, config.sample_rate);
                    break;
                }
                }
            }
        });

    report.final_size     = index.size();
    report.index_metrics  = index.metrics();
    return report;
}

void print_text_header(const BenchmarkConfig& config, const IIndex& index) {
    std::cout << "index=" << index.name() << " scenario=" << to_string(config.scenario)
              << " threads=" << config.threads << " initial_keys=" << config.initial_keys
              << " operations=" << config.operations << " key_space=" << config.key_space
              << " sample_rate=" << config.sample_rate << " seed=" << config.seed << '\n';
}

void print_phase_text(const PhaseReport& report) {
    std::cout << '\n' << '[' << report.phase_name << ']' << '\n';
    std::cout << "duration: " << std::fixed << std::setprecision(6) << report.duration_seconds
              << " s\n";
    std::cout << "throughput: " << format_rate(report.throughput_ops_per_sec) << " ops/s\n";
    std::cout << "rss_before: " << format_bytes(report.rss_before_bytes) << '\n';
    std::cout << "rss_after: " << format_bytes(report.rss_after_bytes) << '\n';
    std::cout << "rss_peak_phase: " << format_bytes(report.rss_peak_bytes) << '\n';
    std::cout << "rss_peak_process: " << format_bytes(report.process_peak_bytes) << '\n';
    std::cout << "final_size: " << report.final_size << '\n';
    if (report.final_size != 0) {
        const auto bytes_per_entry =
            static_cast<double>(report.rss_after_bytes) / static_cast<double>(report.final_size);
        std::cout << "bytes_per_entry: " << std::fixed << std::setprecision(2) << bytes_per_entry
                  << '\n';
    }

    for (const auto& operation : report.operation_summaries) {
        if (operation.count == 0) {
            continue;
        }

        std::cout << "op=" << to_string(operation.kind) << " count=" << operation.count
                  << " success=" << operation.success << " fail=" << operation.fail
                  << " sampled=" << operation.latency.sample_count;

        if (operation.latency.sample_count != 0) {
            std::cout << " mean=" << format_latency(operation.latency.mean_ns)
                      << " p50=" << format_latency(static_cast<double>(operation.latency.p50_ns))
                      << " p95=" << format_latency(static_cast<double>(operation.latency.p95_ns))
                      << " p99=" << format_latency(static_cast<double>(operation.latency.p99_ns))
                      << " max=" << format_latency(static_cast<double>(operation.latency.max_ns));
        }
        std::cout << '\n';
    }

    if (!report.index_metrics.empty()) {
        std::cout << "metrics:";
        bool first = true;
        for (const auto& [key, value] : report.index_metrics) {
            std::cout << (first ? ' ' : ',') << key << '=' << value;
            first = false;
        }
        std::cout << '\n';
    }
}

void print_phase_csv_header() {
    std::cout << "index,phase,threads,duration_s,throughput_ops_s,rss_before_bytes,"
                 "rss_after_bytes,rss_peak_phase_bytes,rss_peak_process_bytes,final_size,"
                 "op_kind,count,success,fail,sampled,mean_ns,p50_ns,p95_ns,p99_ns,max_ns\n";
}

void print_phase_csv(const BenchmarkConfig& config, const IIndex& index, const PhaseReport& report) {
    for (const auto& operation : report.operation_summaries) {
        if (operation.count == 0) {
            continue;
        }

        std::cout << index.name() << ',' << report.phase_name << ',' << config.threads << ','
                  << std::fixed << std::setprecision(6) << report.duration_seconds << ','
                  << std::fixed << std::setprecision(2) << report.throughput_ops_per_sec << ','
                  << report.rss_before_bytes << ',' << report.rss_after_bytes << ','
                  << report.rss_peak_bytes << ',' << report.process_peak_bytes << ','
                  << report.final_size << ',' << to_string(operation.kind) << ','
                  << operation.count << ',' << operation.success << ',' << operation.fail << ','
                  << operation.latency.sample_count << ',' << std::fixed << std::setprecision(2)
                  << operation.latency.mean_ns << ',' << operation.latency.p50_ns << ','
                  << operation.latency.p95_ns << ',' << operation.latency.p99_ns << ','
                  << operation.latency.max_ns << '\n';
    }
}

void normalize_config(BenchmarkConfig& config) {
    config.threads     = std::max<size_t>(1, config.threads);
    config.initial_keys = std::max<uint64_t>(1, config.initial_keys);
    config.operations   = std::max<uint64_t>(1, config.operations);
    config.sample_rate  = std::max<uint64_t>(1, config.sample_rate);
    config.memory_sample_interval =
        std::max<uint64_t>(1, config.memory_sample_interval);
    config.key_space = std::max<uint64_t>(
        config.key_space == 0 ? config.initial_keys * 2 : config.key_space,
        config.initial_keys + 1);

    const double total_ratio = config.read_ratio + config.upsert_ratio + config.erase_ratio;
    if (total_ratio <= 0.0) {
        throw CLI::ValidationError("--read-ratio/--upsert-ratio/--erase-ratio",
                                   "mixed workload ratios must sum to a positive value");
    }

    config.read_ratio /= total_ratio;
    config.upsert_ratio /= total_ratio;
    config.erase_ratio /= total_ratio;

    if (config.read_miss_ratio < 0.0 || config.read_miss_ratio > 1.0) {
        throw CLI::ValidationError("--read-miss-ratio", "must be within [0, 1]");
    }
    if (config.dash_merge_threshold <= 0.0 || config.dash_merge_threshold >= 1.0) {
        throw CLI::ValidationError("--dash-merge-threshold", "must be within (0, 1)");
    }
}

auto parse_scenario(std::string_view value) -> Scenario {
    if (value == "load") {
        return Scenario::kLoad;
    }
    if (value == "read") {
        return Scenario::kRead;
    }
    if (value == "mixed") {
        return Scenario::kMixed;
    }
    if (value == "all") {
        return Scenario::kAll;
    }
    throw CLI::ConversionError("scenario", std::string(value));
}

auto parse_format(std::string_view value) -> OutputFormat {
    if (value == "text") {
        return OutputFormat::kText;
    }
    if (value == "csv") {
        return OutputFormat::kCsv;
    }
    throw CLI::ConversionError("format", std::string(value));
}

void print_registry(const std::vector<IndexDescriptor>& registry) {
    for (const auto& descriptor : registry) {
        std::cout << descriptor.name << " - " << descriptor.description << '\n';
    }
}

} // namespace

auto run(int argc, char** argv) -> int {
    BenchmarkConfig config;
    const auto      registry = build_registry();

    bool        list_indexes = false;
    std::string scenario     = "all";
    std::string format       = "text";

    size_t default_threads = std::thread::hardware_concurrency();
    if (default_threads == 0) {
        default_threads = 4;
    }
    config.threads = std::min<size_t>(default_threads, 8);

    CLI::App app{"idleKV index benchmark"};
    app.add_flag("--list", list_indexes, "list registered indexes and exit");
    app.add_option("--index", config.index_name, "index adapter to benchmark")->default_val(config.index_name);
    app.add_option("--scenario", scenario, "benchmark scenario: load, read, mixed, all")
        ->default_val(scenario);
    app.add_option("--format", format, "report format: text, csv")->default_val(format);
    app.add_option("--threads", config.threads, "worker threads")->default_val(config.threads);
    app.add_option("--initial-keys", config.initial_keys, "number of keys loaded before the measured phase")
        ->default_val(config.initial_keys);
    app.add_option("--operations", config.operations, "measured operations for read/mixed phases")
        ->default_val(config.operations);
    app.add_option("--key-space", config.key_space, "key universe for mutable workload, 0 means auto")
        ->default_val(config.key_space);
    app.add_option("--seed", config.seed, "benchmark seed")->default_val(config.seed);
    app.add_option("--read-ratio", config.read_ratio, "mixed workload read ratio")
        ->default_val(config.read_ratio);
    app.add_option("--upsert-ratio", config.upsert_ratio, "mixed workload upsert ratio")
        ->default_val(config.upsert_ratio);
    app.add_option("--erase-ratio", config.erase_ratio, "mixed workload erase ratio")
        ->default_val(config.erase_ratio);
    app.add_option("--read-miss-ratio", config.read_miss_ratio, "miss ratio for read and mixed read operations")
        ->default_val(config.read_miss_ratio);
    app.add_option("--sample-rate", config.sample_rate, "record 1 latency sample every N operations")
        ->default_val(config.sample_rate);
    app.add_option("--memory-sample-interval-ms", config.memory_sample_interval,
                   "RSS sampling interval in milliseconds")->default_val(config.memory_sample_interval);
    app.add_option("--dash-initial-depth", config.dash_initial_depth, "Dash initial global depth")
        ->default_val(config.dash_initial_depth);
    app.add_option("--dash-merge-threshold", config.dash_merge_threshold, "Dash merge threshold")
        ->default_val(config.dash_merge_threshold);

    try {
        app.parse(argc, argv);
    } catch (const CLI::ParseError& error) {
        return app.exit(error);
    }

    if (list_indexes) {
        print_registry(registry);
        return 0;
    }

    config.scenario = parse_scenario(scenario);
    config.format   = parse_format(format);
    normalize_config(config);

    const auto* descriptor = find_descriptor(config.index_name, registry);
    if (descriptor == nullptr) {
        std::cerr << "unknown index: " << config.index_name << '\n';
        print_registry(registry);
        return 1;
    }

    auto index = descriptor->factory(config);

    if (config.format == OutputFormat::kText) {
        print_text_header(config, *index);
    } else {
        print_phase_csv_header();
    }

    const auto emit_report = [&](const PhaseReport& report) {
        if (config.format == OutputFormat::kText) {
            print_phase_text(report);
        } else {
            print_phase_csv(config, *index, report);
        }
    };

    switch (config.scenario) {
    case Scenario::kLoad:
        emit_report(run_load_phase(*index, config));
        break;
    case Scenario::kRead:
        run_load_phase(*index, config);
        emit_report(run_read_phase(*index, config));
        break;
    case Scenario::kMixed:
        run_load_phase(*index, config);
        emit_report(run_mixed_phase(*index, config));
        break;
    case Scenario::kAll:
        emit_report(run_load_phase(*index, config));
        emit_report(run_read_phase(*index, config));
        emit_report(run_mixed_phase(*index, config));
        break;
    }

    return 0;
}

} // namespace idlekv::benchmark

auto main(int argc, char** argv) -> int {
    try {
        return idlekv::benchmark::run(argc, argv);
    } catch (const std::exception& error) {
        std::cerr << "benchmark failed: " << error.what() << '\n';
        return 1;
    }
}
