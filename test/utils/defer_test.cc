#include "utils/defer/defer.h"

#include <gtest/gtest.h>

namespace {

using idlekv::utils::Defer;

TEST(DeferTest, RunsCallbackOnScopeExit) {
    int value = 0;

    {
        Defer defer([&] { value = 42; });
        EXPECT_TRUE(defer.active());
    }

    EXPECT_EQ(value, 42);
}

TEST(DeferTest, DismissSkipsCallback) {
    int value = 0;

    {
        Defer defer([&] { value = 1; });
        defer.dismiss();
        EXPECT_FALSE(defer.active());
    }

    EXPECT_EQ(value, 0);
}

TEST(DeferTest, MoveTransfersOwnershipOfCallback) {
    int value = 0;

    {
        Defer first([&] { value += 1; });
        Defer second(std::move(first));

        EXPECT_FALSE(first.active());
        EXPECT_TRUE(second.active());
    }

    EXPECT_EQ(value, 1);
}

} // namespace
