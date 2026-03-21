#include "db/storage/art/art.h"

#include <absl/container/flat_hash_map.h>
#include <CLI11/CLI11.hpp>

#include <algorithm>
#include <cmath>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory_resource>
#include <numeric>
#include <optional>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <time.h>
#include <unistd.h>
#include <unordered_map>
#include <utility>
#include <vector>

namespace {

using idlekv::Art;
using idlekv::ArtKey;
using idlekv::InsertMode;
using idlekv::InsertResutl;

constexpr double kMiB = 1024.0 * 1024.0;
constexpr std::string_view kAlphabet =
    "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
constexpr size_t kNpos = std::numeric_limits<size_t>::max();

struct Options {
    size_t      key_count = 2000000;
    size_t      op_count = 2000000;
    uint64_t    seed = 0xC0FFEEULL;
    std::string datasets = "shared_prefix,wide_fanout,mixed";
    std::string indexes = "art,std_unordered_map,absl_flat_hash_map";
    std::string csv_out;
};

class TrackingMemoryResource : public std::pmr::memory_resource {
public:
    explicit TrackingMemoryResource(std::pmr::memory_resource* upstream) : upstream_(upstream) {}

    auto CurrentBytes() const -> size_t { return current_bytes_; }
    auto PeakBytes() const -> size_t { return peak_bytes_; }
    auto TotalAllocatedBytes() const -> size_t { return total_allocated_bytes_; }
    auto AllocCalls() const -> size_t { return alloc_calls_; }
    auto DeallocCalls() const -> size_t { return dealloc_calls_; }

protected:
    auto do_allocate(size_t bytes, size_t alignment) -> void* override {
        void* ptr = upstream_->allocate(bytes, alignment);
        current_bytes_ += bytes;
        peak_bytes_ = std::max(peak_bytes_, current_bytes_);
        total_allocated_bytes_ += bytes;
        ++alloc_calls_;
        return ptr;
    }

    auto do_deallocate(void* p, size_t bytes, size_t alignment) -> void override {
        if (bytes > current_bytes_) {
            throw std::runtime_error("tracking allocator underflow");
        }
        current_bytes_ -= bytes;
        ++dealloc_calls_;
        upstream_->deallocate(p, bytes, alignment);
    }

    auto do_is_equal(const std::pmr::memory_resource& other) const noexcept -> bool override {
        return this == &other;
    }

private:
    std::pmr::memory_resource* upstream_;
    size_t current_bytes_{0};
    size_t peak_bytes_{0};
    size_t total_allocated_bytes_{0};
    size_t alloc_calls_{0};
    size_t dealloc_calls_{0};
};

using PmrString = std::pmr::string;

auto ToStringView(std::string_view value) -> std::string_view { return value; } 

auto ToStringView(const std::string& value) -> std::string_view {
    return std::string_view(value.data(), value.size());
}

auto ToStringView(const PmrString& value) -> std::string_view {
    return std::string_view(value.data(), value.size());
}

struct TransparentStringHash {
    using is_transparent = void;

    auto operator()(std::string_view value) const -> size_t {
        return std::hash<std::string_view>{}(value);
    }

    auto operator()(const std::string& value) const -> size_t { return (*this)(ToStringView(value)); }
    auto operator()(const PmrString& value) const -> size_t { return (*this)(ToStringView(value)); }
};

struct TransparentStringEqual {
    using is_transparent = void;

    template<class L, class R>
    auto operator()(const L& lhs, const R& rhs) const -> bool {
        return ToStringView(lhs) == ToStringView(rhs);
    }
};

class ArtIndex {
public:
    static constexpr std::string_view kName = "art";

    auto Name() const -> std::string_view { return kName; }
    auto Tracker() const -> const TrackingMemoryResource& { return tracker_; }

    auto PrepareForBulkLoad(size_t) -> void {}

    auto Insert(std::string_view key, uint64_t value) -> InsertResutl {
        auto art_key = ArtKey::BuildFromString(key);
        return art_.Insert(art_key, value);
    }

    auto Upsert(std::string_view key, uint64_t value) -> InsertResutl {
        auto art_key = ArtKey::BuildFromString(key);
        return art_.Insert(art_key, value, InsertMode::Upsert);
    }

    auto Lookup(std::string_view key) -> std::optional<uint64_t> {
        auto art_key = ArtKey::BuildFromString(key);
        return art_.Lookup(art_key);
    }

    auto Erase(std::string_view key) -> size_t {
        auto art_key = ArtKey::BuildFromString(key);
        return art_.Erase(art_key);
    }

private:
    std::pmr::unsynchronized_pool_resource arena_{std::pmr::new_delete_resource()};
    TrackingMemoryResource                 tracker_{&arena_};
    Art<uint64_t>                          art_{&tracker_};
};

class StdUnorderedMapIndex {
public:
    static constexpr std::string_view kName = "std::unordered_map";
    using Allocator = std::pmr::polymorphic_allocator<std::pair<const PmrString, uint64_t>>;
    using Map = std::unordered_map<PmrString,
                                   uint64_t,
                                   TransparentStringHash,
                                   TransparentStringEqual,
                                   Allocator>;

    StdUnorderedMapIndex()
        : map_(0, TransparentStringHash{}, TransparentStringEqual{}, Allocator{&tracker_}) {}

    auto Name() const -> std::string_view { return kName; }
    auto Tracker() const -> const TrackingMemoryResource& { return tracker_; }

    auto PrepareForBulkLoad(size_t count) -> void { map_.reserve(count); }

    auto Insert(std::string_view key, uint64_t value) -> InsertResutl {
        auto [it, inserted] = map_.emplace(MakeOwnedKey(key), value);
        return inserted ? InsertResutl::OK : InsertResutl::DuplicateKey;
    }

    auto Upsert(std::string_view key, uint64_t value) -> InsertResutl {
        auto it = map_.find(key);
        if (it != map_.end()) {
            it->second = value;
            return InsertResutl::UpsertValue;
        }
        auto [inserted_it, inserted] = map_.emplace(MakeOwnedKey(key), value);
        (void)inserted_it;
        return inserted ? InsertResutl::OK : InsertResutl::DuplicateKey;
    }

    auto Lookup(std::string_view key) -> std::optional<uint64_t> {
        auto it = map_.find(key);
        if (it == map_.end()) {
            return std::nullopt;
        }
        return it->second;
    }

    auto Erase(std::string_view key) -> size_t {
        auto it = map_.find(key);
        if (it == map_.end()) {
            return 0;
        }
        map_.erase(it);
        return 1;
    }

private:
    auto MakeOwnedKey(std::string_view key) -> PmrString {
        return PmrString(key.begin(), key.end(), std::pmr::polymorphic_allocator<char>{&tracker_});
    }

    std::pmr::unsynchronized_pool_resource arena_{std::pmr::new_delete_resource()};
    TrackingMemoryResource                 tracker_{&arena_};
    Map                                    map_;
};

class AbslFlatHashMapIndex {
public:
    static constexpr std::string_view kName = "absl::flat_hash_map";
    using Allocator = std::pmr::polymorphic_allocator<std::pair<const PmrString, uint64_t>>;
    using Map = absl::flat_hash_map<PmrString,
                                    uint64_t,
                                    TransparentStringHash,
                                    TransparentStringEqual,
                                    Allocator>;

    AbslFlatHashMapIndex()
        : map_(0, TransparentStringHash{}, TransparentStringEqual{}, Allocator{&tracker_}) {}

    auto Name() const -> std::string_view { return kName; }
    auto Tracker() const -> const TrackingMemoryResource& { return tracker_; }

    auto PrepareForBulkLoad(size_t count) -> void { map_.reserve(count); }

    auto Insert(std::string_view key, uint64_t value) -> InsertResutl {
        auto [it, inserted] = map_.emplace(MakeOwnedKey(key), value);
        return inserted ? InsertResutl::OK : InsertResutl::DuplicateKey;
    }

    auto Upsert(std::string_view key, uint64_t value) -> InsertResutl {
        auto it = map_.find(key);
        if (it != map_.end()) {
            it->second = value;
            return InsertResutl::UpsertValue;
        }
        auto [inserted_it, inserted] = map_.emplace(MakeOwnedKey(key), value);
        (void)inserted_it;
        return inserted ? InsertResutl::OK : InsertResutl::DuplicateKey;
    }

    auto Lookup(std::string_view key) -> std::optional<uint64_t> {
        auto it = map_.find(key);
        if (it == map_.end()) {
            return std::nullopt;
        }
        return it->second;
    }

    auto Erase(std::string_view key) -> size_t {
        auto it = map_.find(key);
        if (it == map_.end()) {
            return 0;
        }
        map_.erase(it);
        return 1;
    }

private:
    auto MakeOwnedKey(std::string_view key) -> PmrString {
        return PmrString(key.begin(), key.end(), std::pmr::polymorphic_allocator<char>{&tracker_});
    }

    std::pmr::unsynchronized_pool_resource arena_{std::pmr::new_delete_resource()};
    TrackingMemoryResource                 tracker_{&arena_};
    Map                                    map_;
};

struct Dataset {
    std::string              name;
    std::vector<std::string> keys;
    std::vector<std::string> missing_keys;
    size_t                   raw_key_bytes{0};
    size_t                   raw_payload_bytes{0};
    double                   avg_key_len{0.0};
    size_t                   max_key_len{0};
};

struct LatencyStats {
    uint64_t min_ns{0};
    uint64_t max_ns{0};
    uint64_t p50_ns{0};
    uint64_t p95_ns{0};
    uint64_t p99_ns{0};
    uint64_t p999_ns{0};
    double   avg_ns{0.0};
};

struct BenchmarkReport {
    std::string               index_name;
    std::string               dataset_name;
    std::string               operation_name;
    size_t                    op_count{0};
    double                    seconds{0.0};
    double                    qps{0.0};
    double                    throughput_mib_per_sec{0.0};
    double                    hit_rate{0.0};
    double                    miss_rate{0.0};
    LatencyStats              latency;
    size_t                    start_live_bytes{0};
    size_t                    end_live_bytes{0};
    size_t                    peak_live_bytes{0};
    size_t                    phase_allocated_bytes{0};
    size_t                    phase_alloc_calls{0};
    size_t                    phase_dealloc_calls{0};
    size_t                    rss_before_setup_bytes{0};
    size_t                    rss_after_setup_bytes{0};
    size_t                    rss_after_ops_bytes{0};
    size_t                    reference_payload_bytes{0};
    size_t                    resident_key_count{0};
    std::optional<std::string> note;
};

struct SampleMeta {
    size_t key_bytes{0};
    bool   hit{false};
    bool   miss{false};
};

struct DenseIndexSet {
    explicit DenseIndexSet(size_t capacity) : positions(capacity, kNpos) {}

    auto Empty() const -> bool { return values.empty(); }
    auto Size() const -> size_t { return values.size(); }

    auto Insert(size_t index) -> void {
        if (positions[index] != kNpos) {
            return;
        }
        positions[index] = values.size();
        values.push_back(index);
    }

    auto Erase(size_t index) -> void {
        const size_t pos = positions[index];
        if (pos == kNpos) {
            return;
        }
        const size_t last = values.back();
        values[pos] = last;
        positions[last] = pos;
        values.pop_back();
        positions[index] = kNpos;
    }

    auto Random(std::mt19937_64& rng) const -> size_t {
        std::uniform_int_distribution<size_t> dist(0, values.size() - 1);
        return values[dist(rng)];
    }

    std::vector<size_t> values;
    std::vector<size_t> positions;
};

auto NowNs() -> uint64_t {
    timespec ts{};
    ::clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL + static_cast<uint64_t>(ts.tv_nsec);
}

auto GetCurrentRssBytes() -> size_t {
    std::ifstream statm("/proc/self/statm");
    size_t        total_pages = 0;
    size_t        resident_pages = 0;
    statm >> total_pages >> resident_pages;
    if (!statm) {
        return 0;
    }
    const long page_size = ::sysconf(_SC_PAGESIZE);
    return resident_pages * static_cast<size_t>(page_size > 0 ? page_size : 4096);
}

auto FormatBytes(size_t bytes) -> std::string {
    std::ostringstream oss;
    if (bytes >= static_cast<size_t>(kMiB)) {
        oss << std::fixed << std::setprecision(2) << (static_cast<double>(bytes) / kMiB) << " MiB";
    } else if (bytes >= 1024) {
        oss << std::fixed << std::setprecision(2) << (static_cast<double>(bytes) / 1024.0) << " KiB";
    } else {
        oss << bytes << " B";
    }
    return oss.str();
}

auto FormatPercent(double value) -> std::string {
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2) << (value * 100.0) << "%";
    return oss.str();
}

auto FormatRate(double value) -> std::string {
    std::ostringstream oss;
    if (value >= 1000000.0) {
        oss << std::fixed << std::setprecision(2) << (value / 1000000.0) << " Mops/s";
    } else if (value >= 1000.0) {
        oss << std::fixed << std::setprecision(2) << (value / 1000.0) << " Kops/s";
    } else {
        oss << std::fixed << std::setprecision(2) << value << " ops/s";
    }
    return oss.str();
}

auto FormatDouble(double value, int precision = 2) -> std::string {
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(precision) << value;
    return oss.str();
}

auto EncodeBase62(uint64_t value, size_t min_width = 1) -> std::string {
    std::string encoded;
    do {
        encoded.push_back(kAlphabet[value % kAlphabet.size()]);
        value /= kAlphabet.size();
    } while (value != 0);

    while (encoded.size() < min_width) {
        encoded.push_back(kAlphabet[0]);
    }

    std::reverse(encoded.begin(), encoded.end());
    return encoded;
}

auto MakeSharedPrefixKeys(size_t count, size_t start, std::string_view prefix)
    -> std::vector<std::string> {
    std::vector<std::string> keys;
    keys.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        keys.push_back(std::string(prefix) + EncodeBase62(start + i, 8));
    }
    return keys;
}

auto MakeWideFanoutKeys(size_t count, size_t start) -> std::vector<std::string> {
    std::vector<std::string> keys;
    keys.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        const uint64_t sequence = start + i;
        std::string    key;
        key.push_back(kAlphabet[sequence % kAlphabet.size()]);
        key.push_back('/');
        key.append(EncodeBase62(sequence / kAlphabet.size(), 10));
        keys.push_back(std::move(key));
    }
    return keys;
}

auto MakeMixedKeys(size_t count, size_t start) -> std::vector<std::string> {
    std::vector<std::string> keys;
    keys.reserve(count);

    const size_t first = count / 3;
    const size_t second = count / 3;
    const size_t third = count - first - second;

    auto shared = MakeSharedPrefixKeys(first, start, "tenant:region:svc:");
    auto wide = MakeWideFanoutKeys(second, start);

    keys.insert(keys.end(),
                std::make_move_iterator(shared.begin()),
                std::make_move_iterator(shared.end()));
    keys.insert(keys.end(), std::make_move_iterator(wide.begin()), std::make_move_iterator(wide.end()));

    for (size_t i = 0; i < third; ++i) {
        const uint64_t sequence = start + first + second + i;
        std::string    key = "tree/";
        key.push_back(kAlphabet[sequence % kAlphabet.size()]);
        key.push_back('/');
        key.append(EncodeBase62(sequence / kAlphabet.size(), 6));
        key.push_back('/');
        key.append(EncodeBase62(sequence * 17 + 13, 4));
        keys.push_back(std::move(key));
    }

    return keys;
}

auto NormalizeName(std::string name) -> std::string {
    for (char& ch : name) {
        if (ch == '-') {
            ch = '_';
            continue;
        }
        ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    }
    return name;
}

auto SplitCsv(std::string_view csv) -> std::vector<std::string> {
    std::vector<std::string> items;
    size_t                   begin = 0;
    while (begin < csv.size()) {
        const size_t end = csv.find(',', begin);
        const size_t len = (end == std::string_view::npos) ? csv.size() - begin : end - begin;
        std::string  item(csv.substr(begin, len));
        const size_t first = item.find_first_not_of(" \t");
        if (first == std::string::npos) {
            if (end == std::string_view::npos) {
                break;
            }
            begin = end + 1;
            continue;
        }
        item.erase(0, first);
        item.erase(item.find_last_not_of(" \t") + 1);
        if (!item.empty()) {
            items.push_back(NormalizeName(item));
        }
        if (end == std::string_view::npos) {
            break;
        }
        begin = end + 1;
    }
    return items;
}

struct ResolvedIndexNames {
    std::vector<std::string> names;
    bool                     remapped_bench_art{false};
    bool                     dropped_duplicates{false};
};

auto JoinCsv(const std::vector<std::string>& items) -> std::string {
    std::ostringstream oss;
    for (size_t i = 0; i < items.size(); ++i) {
        if (i != 0) {
            oss << ',';
        }
        oss << items[i];
    }
    return oss.str();
}

auto ResolveIndexNames(const std::vector<std::string>& requested) -> ResolvedIndexNames {
    ResolvedIndexNames resolved;
    resolved.names.reserve(requested.size());

    for (const std::string& name : requested) {
        std::string effective = name;
        if (name == "bench_art" || name == "external_bench_art") {
            effective = "art";
            resolved.remapped_bench_art = true;
        }

        if (std::find(resolved.names.begin(), resolved.names.end(), effective) != resolved.names.end()) {
            resolved.dropped_duplicates = true;
            continue;
        }
        resolved.names.push_back(std::move(effective));
    }

    return resolved;
}

auto BuildDataset(std::string_view name, size_t count) -> Dataset {
    Dataset data;
    data.name = std::string(name);

    const std::string normalized = NormalizeName(std::string(name));
    if (normalized == "shared_prefix") {
        data.keys = MakeSharedPrefixKeys(count, 0, "tenant:region:svc:");
        data.missing_keys = MakeSharedPrefixKeys(count, count * 4, "tenant:region:svc:");
    } else if (normalized == "wide_fanout") {
        data.keys = MakeWideFanoutKeys(count, 0);
        data.missing_keys = MakeWideFanoutKeys(count, count * 4);
    } else if (normalized == "mixed") {
        data.keys = MakeMixedKeys(count, 0);
        data.missing_keys = MakeMixedKeys(count, count * 4);
    } else {
        throw std::runtime_error("unknown dataset: " + std::string(name));
    }

    for (const std::string& key : data.keys) {
        data.raw_key_bytes += key.size();
        data.max_key_len = std::max(data.max_key_len, key.size());
    }

    data.raw_payload_bytes = data.raw_key_bytes + data.keys.size() * sizeof(uint64_t);
    data.avg_key_len = data.keys.empty()
                           ? 0.0
                           : static_cast<double>(data.raw_key_bytes) / static_cast<double>(data.keys.size());
    return data;
}

template<class T>
auto ShuffleIndices(size_t count, T seed) -> std::vector<size_t> {
    std::vector<size_t> indices(count);
    std::iota(indices.begin(), indices.end(), 0);
    std::mt19937_64 rng(seed);
    std::shuffle(indices.begin(), indices.end(), rng);
    return indices;
}

auto Require(bool condition, std::string_view message) -> void {
    if (!condition) {
        throw std::runtime_error(std::string(message));
    }
}

auto ComputeLatencyStats(std::vector<uint64_t> latencies) -> LatencyStats {
    if (latencies.empty()) {
        return {};
    }

    const double sum =
        std::accumulate(latencies.begin(), latencies.end(), 0.0, [](double acc, uint64_t ns) {
            return acc + static_cast<double>(ns);
        });

    std::sort(latencies.begin(), latencies.end());

    auto percentile = [&](double ratio) -> uint64_t {
        const size_t index = static_cast<size_t>(
            std::ceil(ratio * static_cast<double>(latencies.size())) - 1.0);
        return latencies[std::min(index, latencies.size() - 1)];
    };

    LatencyStats stats;
    stats.min_ns = latencies.front();
    stats.max_ns = latencies.back();
    stats.p50_ns = percentile(0.50);
    stats.p95_ns = percentile(0.95);
    stats.p99_ns = percentile(0.99);
    stats.p999_ns = percentile(0.999);
    stats.avg_ns = sum / static_cast<double>(latencies.size());
    return stats;
}

template<class Fn>
auto TraceOperationSamples(size_t op_count, Fn&& fn)
    -> std::tuple<std::vector<uint64_t>, uint64_t, uint64_t, size_t, size_t> {
    std::vector<uint64_t> latencies;
    latencies.reserve(op_count);

    uint64_t key_bytes = 0;
    size_t   hit_count = 0;
    size_t   miss_count = 0;
    const uint64_t start_ns = NowNs();
    for (size_t i = 0; i < op_count; ++i) {
        const uint64_t before_ns = NowNs();
        const SampleMeta sample = fn(i);
        const uint64_t after_ns = NowNs();
        latencies.push_back(after_ns - before_ns);
        key_bytes += sample.key_bytes;
        hit_count += sample.hit ? 1U : 0U;
        miss_count += sample.miss ? 1U : 0U;
    }
    const uint64_t elapsed_ns = NowNs() - start_ns;
    return {std::move(latencies), elapsed_ns, key_bytes, hit_count, miss_count};
}

template<class Index>
auto BuildIndex(Index& index,
                const std::vector<std::string>& keys,
                uint64_t seed,
                uint64_t value_offset = 0) -> void {
    index.PrepareForBulkLoad(keys.size());
    const auto order = ShuffleIndices(keys.size(), seed);
    for (size_t idx : order) {
        const auto res = index.Insert(keys[idx], value_offset + idx);
        Require(res == InsertResutl::OK, "index preload insert failed");
    }
}

auto BytesPerKey(const BenchmarkReport& report) -> double {
    return report.resident_key_count > 0
               ? static_cast<double>(report.end_live_bytes) / static_cast<double>(report.resident_key_count)
               : 0.0;
}

auto Amplification(const BenchmarkReport& report) -> double {
    return report.reference_payload_bytes > 0
               ? static_cast<double>(report.end_live_bytes) /
                     static_cast<double>(report.reference_payload_bytes)
               : 0.0;
}

auto AllocCallsPerOp(const BenchmarkReport& report) -> double {
    return report.op_count > 0
               ? static_cast<double>(report.phase_alloc_calls) / static_cast<double>(report.op_count)
               : 0.0;
}

auto FinalizeReport(std::string_view index_name,
                    const Dataset& data,
                    std::string operation_name,
                    size_t resident_key_count,
                    size_t op_count,
                    uint64_t elapsed_ns,
                    uint64_t key_bytes_processed,
                    size_t hit_count,
                    size_t miss_count,
                    const std::vector<uint64_t>& latencies,
                    const TrackingMemoryResource& tracker,
                    size_t phase_allocated_bytes,
                    size_t phase_alloc_calls,
                    size_t phase_dealloc_calls,
                    size_t rss_before_setup,
                    size_t rss_after_setup,
                    size_t rss_after_ops,
                    size_t start_live_bytes,
                    size_t end_live_bytes,
                    std::optional<std::string> note = std::nullopt) -> BenchmarkReport {
    BenchmarkReport report;
    report.index_name = std::string(index_name);
    report.dataset_name = data.name;
    report.operation_name = std::move(operation_name);
    report.op_count = op_count;
    report.seconds = static_cast<double>(elapsed_ns) / 1000000000.0;
    report.qps = report.seconds > 0.0 ? static_cast<double>(op_count) / report.seconds : 0.0;
    report.throughput_mib_per_sec =
        report.seconds > 0.0 ? (static_cast<double>(key_bytes_processed) / report.seconds) / kMiB : 0.0;
    report.hit_rate = op_count > 0 ? static_cast<double>(hit_count) / static_cast<double>(op_count) : 0.0;
    report.miss_rate = op_count > 0 ? static_cast<double>(miss_count) / static_cast<double>(op_count) : 0.0;
    report.latency = ComputeLatencyStats(latencies);
    report.start_live_bytes = start_live_bytes;
    report.end_live_bytes = end_live_bytes;
    report.peak_live_bytes = tracker.PeakBytes();
    report.phase_allocated_bytes = phase_allocated_bytes;
    report.phase_alloc_calls = phase_alloc_calls;
    report.phase_dealloc_calls = phase_dealloc_calls;
    report.rss_before_setup_bytes = rss_before_setup;
    report.rss_after_setup_bytes = rss_after_setup;
    report.rss_after_ops_bytes = rss_after_ops;
    report.reference_payload_bytes =
        resident_key_count == data.keys.size()
            ? data.raw_payload_bytes
            : static_cast<size_t>(std::llround(data.avg_key_len * resident_key_count)) +
                  resident_key_count * sizeof(uint64_t);
    report.resident_key_count = resident_key_count;
    report.note = std::move(note);
    return report;
}

template<class Index>
auto RunInsertUniqueBenchmark(const Dataset& data, uint64_t seed) -> BenchmarkReport {
    Index       index;
    const auto& tracker = index.Tracker();
    const auto  order = ShuffleIndices(data.keys.size(), seed ^ 0x11ULL);

    const size_t rss_before_setup = GetCurrentRssBytes();
    index.PrepareForBulkLoad(data.keys.size());
    const size_t rss_after_setup = GetCurrentRssBytes();
    const size_t start_live_bytes = tracker.CurrentBytes();
    const size_t alloc_bytes_before_ops = tracker.TotalAllocatedBytes();
    const size_t alloc_calls_before_ops = tracker.AllocCalls();
    const size_t dealloc_calls_before_ops = tracker.DeallocCalls();

    auto [latencies, elapsed_ns, key_bytes_processed, hit_count, miss_count] =
        TraceOperationSamples(order.size(), [&](size_t i) -> SampleMeta {
            const size_t index_id = order[i];
            const auto   res = index.Insert(data.keys[index_id], index_id);
            Require(res == InsertResutl::OK, "insert benchmark encountered duplicate");
            return {.key_bytes = data.keys[index_id].size(), .hit = true, .miss = false};
        });

    const size_t end_live_bytes = tracker.CurrentBytes();
    const size_t rss_after_ops = GetCurrentRssBytes();
    return FinalizeReport(index.Name(),
                          data,
                          "InsertUnique",
                          data.keys.size(),
                          order.size(),
                          elapsed_ns,
                          key_bytes_processed,
                          hit_count,
                          miss_count,
                          latencies,
                          tracker,
                          tracker.TotalAllocatedBytes() - alloc_bytes_before_ops,
                          tracker.AllocCalls() - alloc_calls_before_ops,
                          tracker.DeallocCalls() - dealloc_calls_before_ops,
                          rss_before_setup,
                          rss_after_setup,
                          rss_after_ops,
                          start_live_bytes,
                          end_live_bytes);
}

template<class Index>
auto RunLookupHitBenchmark(const Dataset& data, size_t op_count, uint64_t seed) -> BenchmarkReport {
    Index       index;
    const auto& tracker = index.Tracker();
    const size_t rss_before_setup = GetCurrentRssBytes();
    BuildIndex(index, data.keys, seed ^ 0x21ULL);
    const size_t rss_after_setup = GetCurrentRssBytes();
    const size_t start_live_bytes = tracker.CurrentBytes();
    const size_t alloc_bytes_before_ops = tracker.TotalAllocatedBytes();
    const size_t alloc_calls_before_ops = tracker.AllocCalls();
    const size_t dealloc_calls_before_ops = tracker.DeallocCalls();

    std::mt19937_64 rng(seed ^ 0x22ULL);
    std::uniform_int_distribution<size_t> dist(0, data.keys.size() - 1);

    auto [latencies, elapsed_ns, key_bytes_processed, hit_count, miss_count] =
        TraceOperationSamples(op_count, [&](size_t) -> SampleMeta {
            const size_t index_id = dist(rng);
            const auto   value = index.Lookup(data.keys[index_id]);
            Require(value.has_value() && *value == index_id,
                    "lookup-hit benchmark returned wrong value");
            return {.key_bytes = data.keys[index_id].size(), .hit = true, .miss = false};
        });

    const size_t end_live_bytes = tracker.CurrentBytes();
    const size_t rss_after_ops = GetCurrentRssBytes();
    return FinalizeReport(index.Name(),
                          data,
                          "LookupHit",
                          data.keys.size(),
                          op_count,
                          elapsed_ns,
                          key_bytes_processed,
                          hit_count,
                          miss_count,
                          latencies,
                          tracker,
                          tracker.TotalAllocatedBytes() - alloc_bytes_before_ops,
                          tracker.AllocCalls() - alloc_calls_before_ops,
                          tracker.DeallocCalls() - dealloc_calls_before_ops,
                          rss_before_setup,
                          rss_after_setup,
                          rss_after_ops,
                          start_live_bytes,
                          end_live_bytes);
}

template<class Index>
auto RunLookupMissBenchmark(const Dataset& data, size_t op_count, uint64_t seed) -> BenchmarkReport {
    Index       index;
    const auto& tracker = index.Tracker();
    const size_t rss_before_setup = GetCurrentRssBytes();
    BuildIndex(index, data.keys, seed ^ 0x31ULL);
    const size_t rss_after_setup = GetCurrentRssBytes();
    const size_t start_live_bytes = tracker.CurrentBytes();
    const size_t alloc_bytes_before_ops = tracker.TotalAllocatedBytes();
    const size_t alloc_calls_before_ops = tracker.AllocCalls();
    const size_t dealloc_calls_before_ops = tracker.DeallocCalls();

    std::mt19937_64 rng(seed ^ 0x32ULL);
    std::uniform_int_distribution<size_t> dist(0, data.missing_keys.size() - 1);

    auto [latencies, elapsed_ns, key_bytes_processed, hit_count, miss_count] =
        TraceOperationSamples(op_count, [&](size_t) -> SampleMeta {
            const size_t index_id = dist(rng);
            const auto   value = index.Lookup(data.missing_keys[index_id]);
            Require(!value.has_value(), "lookup-miss benchmark unexpectedly hit");
            return {.key_bytes = data.missing_keys[index_id].size(), .hit = false, .miss = true};
        });

    const size_t end_live_bytes = tracker.CurrentBytes();
    const size_t rss_after_ops = GetCurrentRssBytes();
    return FinalizeReport(index.Name(),
                          data,
                          "LookupMiss",
                          data.keys.size(),
                          op_count,
                          elapsed_ns,
                          key_bytes_processed,
                          hit_count,
                          miss_count,
                          latencies,
                          tracker,
                          tracker.TotalAllocatedBytes() - alloc_bytes_before_ops,
                          tracker.AllocCalls() - alloc_calls_before_ops,
                          tracker.DeallocCalls() - dealloc_calls_before_ops,
                          rss_before_setup,
                          rss_after_setup,
                          rss_after_ops,
                          start_live_bytes,
                          end_live_bytes);
}

template<class Index>
auto RunUpsertHitBenchmark(const Dataset& data, size_t op_count, uint64_t seed) -> BenchmarkReport {
    Index       index;
    const auto& tracker = index.Tracker();
    const size_t rss_before_setup = GetCurrentRssBytes();
    BuildIndex(index, data.keys, seed ^ 0x41ULL);
    const size_t rss_after_setup = GetCurrentRssBytes();
    const size_t start_live_bytes = tracker.CurrentBytes();
    const size_t alloc_bytes_before_ops = tracker.TotalAllocatedBytes();
    const size_t alloc_calls_before_ops = tracker.AllocCalls();
    const size_t dealloc_calls_before_ops = tracker.DeallocCalls();

    std::mt19937_64 rng(seed ^ 0x42ULL);
    std::uniform_int_distribution<size_t> dist(0, data.keys.size() - 1);

    auto [latencies, elapsed_ns, key_bytes_processed, hit_count, miss_count] =
        TraceOperationSamples(op_count, [&](size_t i) -> SampleMeta {
            const size_t index_id = dist(rng);
            const auto   res = index.Upsert(data.keys[index_id], 0xABCD0000ULL + i);
            Require(res == InsertResutl::UpsertValue, "upsert benchmark failed");
            return {.key_bytes = data.keys[index_id].size(), .hit = true, .miss = false};
        });

    const size_t end_live_bytes = tracker.CurrentBytes();
    const size_t rss_after_ops = GetCurrentRssBytes();
    return FinalizeReport(index.Name(),
                          data,
                          "UpsertHit",
                          data.keys.size(),
                          op_count,
                          elapsed_ns,
                          key_bytes_processed,
                          hit_count,
                          miss_count,
                          latencies,
                          tracker,
                          tracker.TotalAllocatedBytes() - alloc_bytes_before_ops,
                          tracker.AllocCalls() - alloc_calls_before_ops,
                          tracker.DeallocCalls() - dealloc_calls_before_ops,
                          rss_before_setup,
                          rss_after_setup,
                          rss_after_ops,
                          start_live_bytes,
                          end_live_bytes);
}

template<class Index>
auto RunEraseHitBenchmark(const Dataset& data, uint64_t seed) -> BenchmarkReport {
    Index       index;
    const auto& tracker = index.Tracker();
    const size_t rss_before_setup = GetCurrentRssBytes();
    BuildIndex(index, data.keys, seed ^ 0x51ULL);
    const size_t rss_after_setup = GetCurrentRssBytes();
    const size_t start_live_bytes = tracker.CurrentBytes();
    const size_t alloc_bytes_before_ops = tracker.TotalAllocatedBytes();
    const size_t alloc_calls_before_ops = tracker.AllocCalls();
    const size_t dealloc_calls_before_ops = tracker.DeallocCalls();

    const auto order = ShuffleIndices(data.keys.size(), seed ^ 0x52ULL);
    auto [latencies, elapsed_ns, key_bytes_processed, hit_count, miss_count] =
        TraceOperationSamples(order.size(), [&](size_t i) -> SampleMeta {
            const size_t index_id = order[i];
            const size_t erased = index.Erase(data.keys[index_id]);
            Require(erased == 1, "erase benchmark failed to delete an existing key");
            return {.key_bytes = data.keys[index_id].size(), .hit = true, .miss = false};
        });

    const size_t end_live_bytes = tracker.CurrentBytes();
    const size_t rss_after_ops = GetCurrentRssBytes();
    return FinalizeReport(index.Name(),
                          data,
                          "EraseHit",
                          0,
                          order.size(),
                          elapsed_ns,
                          key_bytes_processed,
                          hit_count,
                          miss_count,
                          latencies,
                          tracker,
                          tracker.TotalAllocatedBytes() - alloc_bytes_before_ops,
                          tracker.AllocCalls() - alloc_calls_before_ops,
                          tracker.DeallocCalls() - dealloc_calls_before_ops,
                          rss_before_setup,
                          rss_after_setup,
                          rss_after_ops,
                          start_live_bytes,
                          end_live_bytes);
}

template<class Index>
auto RunMixedReadWriteBenchmark(const Dataset& data, size_t op_count, uint64_t seed) -> BenchmarkReport {
    Index        index;
    DenseIndexSet live(data.keys.size());
    DenseIndexSet absent(data.keys.size());
    const auto&  tracker = index.Tracker();

    const size_t initial_live = std::max<size_t>(1, (data.keys.size() * 3) / 4);
    const auto   load_order = ShuffleIndices(data.keys.size(), seed ^ 0x61ULL);

    const size_t rss_before_setup = GetCurrentRssBytes();
    index.PrepareForBulkLoad(data.keys.size());
    for (size_t i = 0; i < initial_live; ++i) {
        const size_t index_id = load_order[i];
        const auto   res = index.Insert(data.keys[index_id], index_id);
        Require(res == InsertResutl::OK, "mixed benchmark preload insert failed");
        live.Insert(index_id);
    }
    for (size_t i = initial_live; i < load_order.size(); ++i) {
        absent.Insert(load_order[i]);
    }
    const size_t rss_after_setup = GetCurrentRssBytes();
    const size_t start_live_bytes = tracker.CurrentBytes();
    const size_t alloc_bytes_before_ops = tracker.TotalAllocatedBytes();
    const size_t alloc_calls_before_ops = tracker.AllocCalls();
    const size_t dealloc_calls_before_ops = tracker.DeallocCalls();

    std::mt19937_64 rng(seed ^ 0x62ULL);
    std::uniform_int_distribution<int> kind_dist(0, 99);
    std::uniform_int_distribution<size_t> miss_dist(0, data.missing_keys.size() - 1);

    size_t lookup_hit_ops = 0;
    size_t lookup_miss_ops = 0;
    size_t upsert_ops = 0;
    size_t insert_ops = 0;
    size_t erase_ops = 0;

    auto [latencies, elapsed_ns, key_bytes_processed, hit_count, miss_count] =
        TraceOperationSamples(op_count, [&](size_t i) -> SampleMeta {
            const int op_kind = kind_dist(rng);

            if (op_kind < 45 && !live.Empty()) {
                const size_t index_id = live.Random(rng);
                const auto   value = index.Lookup(data.keys[index_id]);
                Require(value.has_value(), "mixed lookup-hit returned miss");
                ++lookup_hit_ops;
                return {.key_bytes = data.keys[index_id].size(), .hit = true, .miss = false};
            }

            if (op_kind < 65) {
                const size_t index_id = miss_dist(rng);
                const auto   value = index.Lookup(data.missing_keys[index_id]);
                Require(!value.has_value(), "mixed lookup-miss unexpectedly hit");
                ++lookup_miss_ops;
                return {.key_bytes = data.missing_keys[index_id].size(), .hit = false, .miss = true};
            }

            if (op_kind < 82 && !live.Empty()) {
                const size_t index_id = live.Random(rng);
                const auto   res = index.Upsert(data.keys[index_id], 0xDD000000ULL + i);
                Require(res == InsertResutl::UpsertValue, "mixed upsert failed");
                ++upsert_ops;
                return {.key_bytes = data.keys[index_id].size(), .hit = true, .miss = false};
            }

            if (op_kind < 91 && !absent.Empty()) {
                const size_t index_id = absent.Random(rng);
                const auto   res = index.Insert(data.keys[index_id], 0xEE000000ULL + i);
                Require(res == InsertResutl::OK, "mixed insert failed");
                absent.Erase(index_id);
                live.Insert(index_id);
                ++insert_ops;
                return {.key_bytes = data.keys[index_id].size(), .hit = true, .miss = false};
            }

            if (!live.Empty()) {
                const size_t index_id = live.Random(rng);
                const size_t erased = index.Erase(data.keys[index_id]);
                Require(erased == 1, "mixed erase failed");
                live.Erase(index_id);
                absent.Insert(index_id);
                ++erase_ops;
                return {.key_bytes = data.keys[index_id].size(), .hit = true, .miss = false};
            }

            const size_t index_id = miss_dist(rng);
            const auto   value = index.Lookup(data.missing_keys[index_id]);
            Require(!value.has_value(), "mixed fallback miss unexpectedly hit");
            ++lookup_miss_ops;
            return {.key_bytes = data.missing_keys[index_id].size(), .hit = false, .miss = true};
        });

    const size_t end_live_bytes = tracker.CurrentBytes();
    const size_t rss_after_ops = GetCurrentRssBytes();

    std::ostringstream note;
    note << "mix breakdown: lookup_hit=" << lookup_hit_ops << ", lookup_miss=" << lookup_miss_ops
         << ", upsert=" << upsert_ops << ", insert=" << insert_ops << ", erase=" << erase_ops
         << ", resident_keys_end=" << live.Size();

    return FinalizeReport(index.Name(),
                          data,
                          "MixedReadWrite",
                          live.Size(),
                          op_count,
                          elapsed_ns,
                          key_bytes_processed,
                          hit_count,
                          miss_count,
                          latencies,
                          tracker,
                          tracker.TotalAllocatedBytes() - alloc_bytes_before_ops,
                          tracker.AllocCalls() - alloc_calls_before_ops,
                          tracker.DeallocCalls() - dealloc_calls_before_ops,
                          rss_before_setup,
                          rss_after_setup,
                          rss_after_ops,
                          start_live_bytes,
                          end_live_bytes,
                          note.str());
}

template<class Index>
auto RunAllBenchmarksForIndex(const Dataset& data, const Options& options)
    -> std::vector<BenchmarkReport> {
    std::vector<BenchmarkReport> reports;
    reports.reserve(6);
    reports.push_back(RunInsertUniqueBenchmark<Index>(data, options.seed));
    reports.push_back(RunLookupHitBenchmark<Index>(data, options.op_count, options.seed));
    reports.push_back(RunLookupMissBenchmark<Index>(data, options.op_count, options.seed));
    reports.push_back(RunUpsertHitBenchmark<Index>(data, options.op_count, options.seed));
    reports.push_back(RunEraseHitBenchmark<Index>(data, options.seed));
    reports.push_back(RunMixedReadWriteBenchmark<Index>(data, options.op_count, options.seed));
    return reports;
}

auto RunAllBenchmarksForIndexName(std::string_view index_name,
                                  const Dataset& data,
                                  const Options& options) -> std::vector<BenchmarkReport> {
    std::string normalized = NormalizeName(std::string(index_name));
    if (normalized == "bench_art" || normalized == "external_bench_art") {
        normalized = "art";
    }
    if (normalized == "art") {
        return RunAllBenchmarksForIndex<ArtIndex>(data, options);
    }
    if (normalized == "std_unordered_map" || normalized == "unordered_map") {
        return RunAllBenchmarksForIndex<StdUnorderedMapIndex>(data, options);
    }
    if (normalized == "absl_flat_hash_map" || normalized == "flat_hash_map") {
        return RunAllBenchmarksForIndex<AbslFlatHashMapIndex>(data, options);
    }
    throw std::runtime_error("unknown index: " + std::string(index_name));
}

auto PrintDatasetSummary(const Dataset& data) -> void {
    std::cout << "Dataset: " << data.name << '\n';
    std::cout << "  keys=" << data.keys.size() << ", avg_key_len=" << FormatDouble(data.avg_key_len, 2)
              << ", max_key_len=" << data.max_key_len
              << ", raw_payload=" << FormatBytes(data.raw_payload_bytes) << '\n';
}

auto PrintReport(const BenchmarkReport& report) -> void {
    std::cout << "  Index: " << report.index_name << " [" << report.operation_name << "]\n";
    std::cout << "    qps=" << FormatRate(report.qps) << ", throughput="
              << FormatDouble(report.throughput_mib_per_sec, 2) << " MiB/s"
              << ", hits=" << FormatPercent(report.hit_rate)
              << ", misses=" << FormatPercent(report.miss_rate) << '\n';
    std::cout << "    latency avg=" << FormatDouble(report.latency.avg_ns, 1) << " ns"
              << ", p50=" << report.latency.p50_ns << " ns"
              << ", p95=" << report.latency.p95_ns << " ns"
              << ", p99=" << report.latency.p99_ns << " ns"
              << ", p999=" << report.latency.p999_ns << " ns"
              << ", max=" << report.latency.max_ns << " ns" << '\n';
    std::cout << "    memory live(start/end/peak)=" << FormatBytes(report.start_live_bytes) << " / "
              << FormatBytes(report.end_live_bytes) << " / " << FormatBytes(report.peak_live_bytes)
              << ", rss(before/setup/after)=" << FormatBytes(report.rss_before_setup_bytes) << " / "
              << FormatBytes(report.rss_after_setup_bytes) << " / "
              << FormatBytes(report.rss_after_ops_bytes) << '\n';
    std::cout << "    index bytes/key=" << FormatDouble(BytesPerKey(report), 2)
              << ", amplification=" << FormatDouble(Amplification(report), 2)
              << ", alloc_calls/op=" << FormatDouble(AllocCallsPerOp(report), 3)
              << ", phase_allocated=" << FormatBytes(report.phase_allocated_bytes)
              << ", allocs=" << report.phase_alloc_calls << ", deallocs=" << report.phase_dealloc_calls
              << '\n';
    if (report.note.has_value()) {
        std::cout << "    note: " << *report.note << '\n';
    }
}

auto CsvEscape(std::string_view field) -> std::string {
    bool need_quotes = false;
    for (char ch : field) {
        if (ch == ',' || ch == '"' || ch == '\n') {
            need_quotes = true;
            break;
        }
    }
    if (!need_quotes) {
        return std::string(field);
    }

    std::string escaped;
    escaped.reserve(field.size() + 2);
    escaped.push_back('"');
    for (char ch : field) {
        if (ch == '"') {
            escaped.push_back('"');
        }
        escaped.push_back(ch);
    }
    escaped.push_back('"');
    return escaped;
}

auto MaybeWriteCsv(const std::vector<BenchmarkReport>& reports, const std::string& path) -> void {
    if (path.empty()) {
        return;
    }

    const std::filesystem::path out_path(path);
    if (out_path.has_parent_path()) {
        std::filesystem::create_directories(out_path.parent_path());
    }

    std::ofstream out(out_path);
    if (!out) {
        throw std::runtime_error("failed to open csv output: " + path);
    }

    out << "index,dataset,operation,op_count,seconds,qps,throughput_mib_per_sec,hit_rate,miss_rate,"
           "latency_avg_ns,latency_p50_ns,latency_p95_ns,latency_p99_ns,latency_p999_ns,latency_max_ns,"
           "start_live_bytes,end_live_bytes,peak_live_bytes,phase_allocated_bytes,phase_alloc_calls,"
           "phase_dealloc_calls,rss_before_setup_bytes,rss_after_setup_bytes,rss_after_ops_bytes,"
           "reference_payload_bytes,resident_key_count,bytes_per_key,amplification,alloc_calls_per_op,note\n";

    for (const auto& report : reports) {
        out << CsvEscape(report.index_name) << ','
            << CsvEscape(report.dataset_name) << ','
            << CsvEscape(report.operation_name) << ','
            << report.op_count << ','
            << std::fixed << std::setprecision(9) << report.seconds << ','
            << std::fixed << std::setprecision(6) << report.qps << ','
            << std::fixed << std::setprecision(6) << report.throughput_mib_per_sec << ','
            << std::fixed << std::setprecision(6) << report.hit_rate << ','
            << std::fixed << std::setprecision(6) << report.miss_rate << ','
            << std::fixed << std::setprecision(3) << report.latency.avg_ns << ','
            << report.latency.p50_ns << ','
            << report.latency.p95_ns << ','
            << report.latency.p99_ns << ','
            << report.latency.p999_ns << ','
            << report.latency.max_ns << ','
            << report.start_live_bytes << ','
            << report.end_live_bytes << ','
            << report.peak_live_bytes << ','
            << report.phase_allocated_bytes << ','
            << report.phase_alloc_calls << ','
            << report.phase_dealloc_calls << ','
            << report.rss_before_setup_bytes << ','
            << report.rss_after_setup_bytes << ','
            << report.rss_after_ops_bytes << ','
            << report.reference_payload_bytes << ','
            << report.resident_key_count << ','
            << std::fixed << std::setprecision(6) << BytesPerKey(report) << ','
            << std::fixed << std::setprecision(6) << Amplification(report) << ','
            << std::fixed << std::setprecision(6) << AllocCallsPerOp(report) << ','
            << CsvEscape(report.note.value_or("")) << '\n';
    }
}

auto main_impl(int argc, char** argv) -> int {
    Options options;

    CLI::App app("Single-thread index benchmark for idlekv");
    app.add_option("--keys", options.key_count, "Number of unique keys per dataset");
    app.add_option("--ops", options.op_count, "Operation count for point workloads");
    app.add_option("--seed", options.seed, "Random seed");
    app.add_option("--datasets",
                   options.datasets,
                   "Comma-separated dataset names: shared_prefix,wide_fanout,mixed");
    app.add_option("--indexes",
                   options.indexes,
                   "Comma-separated index names: art,std_unordered_map,absl_flat_hash_map");
    app.add_option("--csv-out", options.csv_out, "Optional CSV output path");
    CLI11_PARSE(app, argc, argv);

    const auto dataset_names = SplitCsv(options.datasets);
    const auto requested_index_names = SplitCsv(options.indexes);
    const auto resolved_index_names = ResolveIndexNames(requested_index_names);
    const auto& index_names = resolved_index_names.names;
    Require(!dataset_names.empty(), "at least one dataset is required");
    Require(!index_names.empty(), "at least one index is required");
    Require(options.key_count > 0, "--keys must be greater than 0");
    Require(options.op_count > 0, "--ops must be greater than 0");

    std::vector<BenchmarkReport> all_reports;

    std::cout << "idlekv index benchmark\n";
    std::cout << "  key_count=" << options.key_count << ", op_count=" << options.op_count
              << ", seed=" << options.seed << "\n";
    std::cout << "  indexes=" << JoinCsv(index_names) << "\n";
    if (resolved_index_names.remapped_bench_art) {
        std::cout << "  note: requested 'bench_art' is mapped to 'art' for compatibility\n";
    }
    if (resolved_index_names.dropped_duplicates) {
        std::cout << "  note: duplicate indexes were removed after normalization\n";
    }
    std::cout << '\n';

    for (const auto& dataset_name : dataset_names) {
        const Dataset data = BuildDataset(dataset_name, options.key_count);
        PrintDatasetSummary(data);

        for (const auto& index_name : index_names) {
            const auto reports = RunAllBenchmarksForIndexName(index_name, data, options);
            for (const auto& report : reports) {
                PrintReport(report);
                all_reports.push_back(report);
            }
        }
        std::cout << '\n';
    }

    MaybeWriteCsv(all_reports, options.csv_out);
    if (!options.csv_out.empty()) {
        std::cout << "CSV written to: " << options.csv_out << '\n';
    }
    return EXIT_SUCCESS;
}

} // namespace

auto main(int argc, char** argv) -> int {
    try {
        return main_impl(argc, argv);
    } catch (const std::exception& ex) {
        std::cerr << "benchmark failed: " << ex.what() << '\n';
        return EXIT_FAILURE;
    }
}
