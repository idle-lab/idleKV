#include "metric/avg.h"

#include <chrono>
#include <gtest/gtest.h>

namespace {

using idlekv::Avg;

TEST(AvgTest, AverageBytesTracksBytesPerObservation) {
    Avg avg("avg_test", std::chrono::hours(1));

    avg.observe(std::chrono::nanoseconds(10));
    avg.observe_bytes(100);
    avg.observe(std::chrono::nanoseconds(20));
    avg.observe_bytes(300);

    EXPECT_EQ(avg.count(), 2U);
    EXPECT_EQ(avg.total_bytes(), 400U);
    EXPECT_DOUBLE_EQ(avg.average_bytes(), 200.0);

    avg.stop();
}

} // namespace
