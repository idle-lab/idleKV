#include "db/command.h"

#include <cstring>
#include <gtest/gtest.h>
#include <initializer_list>
#include <string>
#include <string_view>
#include <vector>

namespace idlekv {
namespace {

auto AppendArg(CmdArgs& args, std::string_view value) -> void {
    const size_t index = args.size();
    args.PreAlloc(value.size());
    if (!value.empty()) {
        std::memcpy(const_cast<char*>(args[index].data()), value.data(), value.size());
    }
}

auto MakeArgs(std::initializer_list<std::string_view> values) -> CmdArgs {
    CmdArgs args;
    for (std::string_view value : values) {
        AppendArg(args, value);
    }
    return args;
}

auto CollectArgs(const CmdArgs& args) -> std::vector<std::string> {
    std::vector<std::string> values;
    values.reserve(args.size());
    for (std::string_view value : args) {
        values.emplace_back(value);
    }
    return values;
}

TEST(CmdArgsTest, StoresAndIteratesArgumentsInOrder) {
    CmdArgs args;

    EXPECT_TRUE(args.empty());
    EXPECT_EQ(args.size(), 0U);
    EXPECT_EQ(args.begin(), args.end());

    AppendArg(args, "set");
    AppendArg(args, "mykey");
    AppendArg(args, "");
    AppendArg(args, "42");

    EXPECT_FALSE(args.empty());
    EXPECT_EQ(args.size(), 4U);
    EXPECT_EQ(args.Front(), "set");
    EXPECT_EQ(args.at(0), "set");
    EXPECT_EQ(args.at(1), "mykey");
    EXPECT_EQ(args.at(2), "");
    EXPECT_EQ(args[3], "42");
    EXPECT_EQ(args.elem_len(0), 3U);
    EXPECT_EQ(args.elem_len(1), 5U);
    EXPECT_EQ(args.elem_len(2), 0U);
    EXPECT_EQ(args.elem_len(3), 2U);
    EXPECT_EQ(CollectArgs(args), (std::vector<std::string>{"set", "mykey", "", "42"}));

    auto it = args.begin();
    EXPECT_EQ(*it, "set");
    ++it;
    EXPECT_EQ(*it, "mykey");
    it += 2;
    EXPECT_EQ(*it, "42");
    EXPECT_EQ(it - args.begin(), 3);
    --it;
    EXPECT_EQ(*it, "");
    EXPECT_EQ(args.end() - args.begin(), 4);
}

TEST(CmdArgsTest, ClearResetsContentsButPreservesAllocatedStorage) {
    CmdArgs args;

    for (int i = 0; i < 6; ++i) {
        AppendArg(args, std::string(256, static_cast<char>('a' + i)));
    }

    const size_t heap_before_clear = args.HeapMemory();
    ASSERT_GT(heap_before_clear, 1024U);

    args.clear();

    EXPECT_TRUE(args.empty());
    EXPECT_EQ(args.size(), 0U);
    EXPECT_EQ(args.begin(), args.end());
    EXPECT_EQ(args.HeapMemory(), heap_before_clear);

    AppendArg(args, "ping");
    AppendArg(args, "");
    AppendArg(args, "payload");

    EXPECT_EQ(CollectArgs(args), (std::vector<std::string>{"ping", "", "payload"}));
    EXPECT_EQ(args.Front(), "ping");
}

TEST(CmdArgsTest, SwapArgsExchangesContents) {
    CmdArgs lhs = MakeArgs({"set", "key", "1"});
    CmdArgs rhs = MakeArgs({"ping"});

    lhs.SwapArgs(rhs);

    EXPECT_EQ(CollectArgs(lhs), (std::vector<std::string>{"ping"}));
    EXPECT_EQ(CollectArgs(rhs), (std::vector<std::string>{"set", "key", "1"}));
    EXPECT_EQ(lhs.Front(), "ping");
    EXPECT_EQ(rhs.Front(), "set");
}

TEST(CmdArgsTest, ClearForReuseKeepsSmallInlineStorageReusable) {
    CmdArgs args = MakeArgs({"get", "k"});

    EXPECT_EQ(args.HeapMemory(), 0U);

    args.ClearForReuse();

    EXPECT_TRUE(args.empty());
    EXPECT_EQ(args.size(), 0U);
    EXPECT_EQ(args.HeapMemory(), 0U);

    AppendArg(args, "mset");
    AppendArg(args, "a");
    AppendArg(args, "1");
    AppendArg(args, "b");
    AppendArg(args, "2");

    EXPECT_EQ(CollectArgs(args), (std::vector<std::string>{"mset", "a", "1", "b", "2"}));
    EXPECT_EQ(args.Front(), "mset");
}

TEST(CmdArgsTest, ClearForReuseDropsLargeHeapAllocations) {
    CmdArgs args;

    for (int i = 0; i < 6; ++i) {
        AppendArg(args, std::string(256, static_cast<char>('a' + i)));
    }

    const size_t heap_before_reuse = args.HeapMemory();
    ASSERT_GT(heap_before_reuse, 1024U);

    args.ClearForReuse();

    EXPECT_TRUE(args.empty());
    EXPECT_EQ(args.size(), 0U);
    EXPECT_LT(args.HeapMemory(), heap_before_reuse);
    EXPECT_LE(args.HeapMemory(), 1024U);

    AppendArg(args, "x");
    AppendArg(args, "y");

    EXPECT_EQ(CollectArgs(args), (std::vector<std::string>{"x", "y"}));
}

} // namespace
} // namespace idlekv
