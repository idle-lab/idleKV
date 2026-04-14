#include "db/info.h"

#include <gtest/gtest.h>
#include <string>

namespace {

using idlekv::FormatInfoMemorySection;
using idlekv::InfoMemoryStats;
using idlekv::InfoSection;
using idlekv::ResolveInfoSection;

TEST(InfoTest, ResolveInfoSectionAcceptsMemoryAliasesCaseInsensitively) {
    EXPECT_EQ(ResolveInfoSection("memory"), InfoSection::Memory);
    EXPECT_EQ(ResolveInfoSection("MEMORY"), InfoSection::Memory);
    EXPECT_EQ(ResolveInfoSection("default"), InfoSection::Memory);
    EXPECT_EQ(ResolveInfoSection("all"), InfoSection::Memory);
    EXPECT_EQ(ResolveInfoSection("everything"), InfoSection::Memory);
}

TEST(InfoTest, ResolveInfoSectionRejectsUnsupportedSections) {
    EXPECT_EQ(ResolveInfoSection("server"), InfoSection::Unsupported);
    EXPECT_EQ(ResolveInfoSection("clients"), InfoSection::Unsupported);
}

TEST(InfoTest, FormatInfoMemorySectionIncludesCoreAndShardFields) {
    InfoMemoryStats stats;
    stats.used_memory            = 1536;
    stats.used_memory_peak       = 2048;
    stats.used_memory_rss        = 4096;
    stats.shard_num              = 2;
    stats.shard_used_memory      = {512, 1024};
    stats.shard_used_memory_peak = {768, 1536};

    const std::string info = FormatInfoMemorySection(stats);

    EXPECT_NE(info.find("# Memory\r\n"), std::string::npos);
    EXPECT_NE(info.find("used_memory:1536\r\n"), std::string::npos);
    EXPECT_NE(info.find("used_memory_human:1.50K\r\n"), std::string::npos);
    EXPECT_NE(info.find("used_memory_peak:2048\r\n"), std::string::npos);
    EXPECT_NE(info.find("used_memory_peak_human:2.00K\r\n"), std::string::npos);
    EXPECT_NE(info.find("used_memory_rss:4096\r\n"), std::string::npos);
    EXPECT_NE(info.find("used_memory_rss_human:4.00K\r\n"), std::string::npos);
    EXPECT_NE(info.find("mem_allocator:mimalloc\r\n"), std::string::npos);
    EXPECT_NE(info.find("idlekv_memory_accounting:shard-logical-bytes\r\n"), std::string::npos);
    EXPECT_NE(info.find("idlekv_shard_num:2\r\n"), std::string::npos);
    EXPECT_NE(info.find("idlekv_shard_0_used_memory:512\r\n"), std::string::npos);
    EXPECT_NE(info.find("idlekv_shard_1_used_memory:1024\r\n"), std::string::npos);
    EXPECT_NE(info.find("idlekv_shard_0_used_memory_peak:768\r\n"), std::string::npos);
    EXPECT_NE(info.find("idlekv_shard_1_used_memory_peak:1536\r\n"), std::string::npos);
}

} // namespace
