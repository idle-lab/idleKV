#include "db/info.h"

#include "db/engine.h"

#include <array>
#include <cctype>
#include <fstream>
#include <iterator>
#include <spdlog/fmt/fmt.h>
#include <unistd.h>

namespace idlekv {

namespace {

auto EqualsIgnoreCase(std::string_view lhs, std::string_view rhs) -> bool {
    if (lhs.size() != rhs.size()) {
        return false;
    }

    for (size_t i = 0; i < lhs.size(); ++i) {
        if (std::tolower(static_cast<unsigned char>(lhs[i])) !=
            std::tolower(static_cast<unsigned char>(rhs[i]))) {
            return false;
        }
    }
    return true;
}

auto ReadCurrentRssBytes() -> size_t {
    std::ifstream statm("/proc/self/statm");
    size_t        total_pages    = 0;
    size_t        resident_pages = 0;
    statm >> total_pages >> resident_pages;
    if (!statm) {
        return 0;
    }

    const long page_size = ::sysconf(_SC_PAGESIZE);
    return resident_pages * static_cast<size_t>(page_size > 0 ? page_size : 4096);
}

auto FormatHumanBytes(size_t bytes) -> std::string {
    constexpr std::array<std::string_view, 6> kUnits = {"B", "K", "M", "G", "T", "P"};

    double value      = static_cast<double>(bytes);
    size_t unit_index = 0;
    while (value >= 1024.0 && unit_index + 1 < kUnits.size()) {
        value /= 1024.0;
        ++unit_index;
    }

    if (unit_index == 0) {
        return fmt::format("{}{}", bytes, kUnits[unit_index]);
    }
    return fmt::format("{:.2f}{}", value, kUnits[unit_index]);
}

} // namespace

auto ResolveInfoSection(std::string_view section) -> InfoSection {
    if (EqualsIgnoreCase(section, "memory") || EqualsIgnoreCase(section, "default") ||
        EqualsIgnoreCase(section, "all") || EqualsIgnoreCase(section, "everything")) {
        return InfoSection::Memory;
    }

    return InfoSection::Unsupported;
}

auto CollectInfoMemoryStats(IdleEngine* eng) -> InfoMemoryStats {
    InfoMemoryStats stats;
    stats.used_memory_rss = ReadCurrentRssBytes();
    if (eng == nullptr) {
        return stats;
    }

    stats.shard_num = eng->ShardNum();
    stats.shard_used_memory.reserve(stats.shard_num);
    stats.shard_used_memory_peak.reserve(stats.shard_num);

    for (size_t i = 0; i < stats.shard_num; ++i) {
        const Shard* shard      = eng->ShardAt(i);
        const size_t shard_used = shard != nullptr ? shard->MemoryUsage() : 0;
        const size_t shard_peak = shard != nullptr ? shard->PeakMemoryUsage() : 0;

        stats.shard_used_memory.push_back(shard_used);
        stats.shard_used_memory_peak.push_back(shard_peak);
        stats.used_memory += shard_used;
        stats.used_memory_peak += shard_peak;
    }

    return stats;
}

auto FormatInfoMemorySection(const InfoMemoryStats& stats) -> std::string {
    std::string out;
    out.reserve(256 + stats.shard_num * 96);

    fmt::format_to(std::back_inserter(out),
                   "# Memory\r\n"
                   "used_memory:{}\r\n"
                   "used_memory_human:{}\r\n"
                   "used_memory_peak:{}\r\n"
                   "used_memory_peak_human:{}\r\n"
                   "used_memory_rss:{}\r\n"
                   "used_memory_rss_human:{}\r\n"
                   "mem_allocator:mimalloc\r\n"
                   "idlekv_memory_accounting:shard-logical-bytes\r\n"
                   "idlekv_shard_num:{}\r\n",
                   stats.used_memory, FormatHumanBytes(stats.used_memory), stats.used_memory_peak,
                   FormatHumanBytes(stats.used_memory_peak), stats.used_memory_rss,
                   FormatHumanBytes(stats.used_memory_rss), stats.shard_num);

    for (size_t i = 0; i < stats.shard_used_memory.size(); ++i) {
        fmt::format_to(std::back_inserter(out), "idlekv_shard_{}_used_memory:{}\r\n", i,
                       stats.shard_used_memory[i]);
    }
    for (size_t i = 0; i < stats.shard_used_memory_peak.size(); ++i) {
        fmt::format_to(std::back_inserter(out), "idlekv_shard_{}_used_memory_peak:{}\r\n", i,
                       stats.shard_used_memory_peak[i]);
    }

    return out;
}

} // namespace idlekv
