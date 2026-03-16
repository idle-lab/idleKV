#include "metric/request_stage.h"

#include <gtest/gtest.h>

namespace {

using idlekv::RequestStageMetrics;

TEST(RequestStageMetricsTest, FormatSlowRequestIncludesStageBreakdown) {
    RequestStageMetrics::SlowRequestBreakdown breakdown{
        .parse =
            RequestStageMetrics::ParseBreakdown{
                .total_ns   = 900000,
                .io_wait_ns = 600000,
                .decode_ns  = 300000,
            },
        .total_ns = 2200000,
        .command_prepare_ns = 10000,
        .exec_ns = 800000,
        .reply_encode_ns = 120000,
        .flush_ns = 350000,
        .arg_count = 2,
        .pipelined = false,
        .flushed = true,
        .cmd_name = "get",
        .peer = "127.0.0.1:6379",
        .note = "result=bulk_string",
    };

    const auto line = RequestStageMetrics::format_slow_request(breakdown);

    EXPECT_NE(line.find("[slow-request] cmd=get"), std::string::npos);
    EXPECT_NE(line.find("total=2.20 ms"), std::string::npos);
    EXPECT_NE(line.find("parse=900.00 us"), std::string::npos);
    EXPECT_NE(line.find("parse_io_wait=600.00 us"), std::string::npos);
    EXPECT_NE(line.find("parse_decode=300.00 us"), std::string::npos);
    EXPECT_NE(line.find("exec=800.00 us"), std::string::npos);
    EXPECT_NE(line.find("send=470.00 us"), std::string::npos);
    EXPECT_NE(line.find("peer=127.0.0.1:6379"), std::string::npos);
    EXPECT_NE(line.find("note=\"result=bulk_string\""), std::string::npos);
}

} // namespace
