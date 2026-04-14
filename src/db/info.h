#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace idlekv {

class IdleEngine;

enum class InfoSection : uint8_t {
    Memory,
    Unsupported,
};

struct InfoMemoryStats {
    size_t              used_memory{0};
    size_t              used_memory_peak{0};
    size_t              used_memory_rss{0};
    size_t              shard_num{0};
    std::vector<size_t> shard_used_memory;
    std::vector<size_t> shard_used_memory_peak;
};

auto ResolveInfoSection(std::string_view section) -> InfoSection;
auto CollectInfoMemoryStats(IdleEngine* eng) -> InfoMemoryStats;
auto FormatInfoMemorySection(const InfoMemoryStats& stats) -> std::string;

} // namespace idlekv
