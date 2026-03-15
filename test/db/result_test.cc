#include "db/result.h"

#include <asio/asio.hpp>
#include <asio/co_spawn.hpp>
#include <asio/detached.hpp>
#include <gtest/gtest.h>

namespace {

using idlekv::ExecResult;
using idlekv::PromiseResult;

TEST(PromiseResultTest, AsyncWaitReturnsWhenNotifyHappenedBeforeWait) {
    asio::io_context ctx;
    PromiseResult    promise(ctx.get_executor());

    promise.set_reslute(ExecResult::ok());
    promise.notify();

    bool resumed = false;
    asio::co_spawn(
        ctx,
        [&]() -> asio::awaitable<void> {
            co_await promise.async_wait();
            resumed = true;
        },
        asio::detached);

    ctx.run();

    EXPECT_TRUE(resumed);
    EXPECT_TRUE(promise.result().is_ok());
}

TEST(ExecResultTest, BulkStringFromStringViewOwnsItsPayload) {
    std::string source = "value";
    auto        res    = ExecResult::bulk_string(std::string_view(source));

    source.assign("xxxxx");

    EXPECT_EQ(res.type(), ExecResult::kBulkString);
    EXPECT_EQ(res.string(), "value");
}

} // namespace
