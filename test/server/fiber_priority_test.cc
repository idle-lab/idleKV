#include "server/el_pool.h"

#include <atomic>
#include <chrono>
#include <gtest/gtest.h>
#include <vector>

namespace idlekv {
namespace {

TEST(FiberPriorityTest, HigherPriorityFibersRunBeforeLowerPriorityFibers) {
    EventLoop el(0, 0);
    el.Run();

    auto run_order = el.AwaitDispatch([]() {
        std::vector<int>                  order;
        boost::fibers::mutex              mu;
        boost::fibers::condition_variable cv;
        int                               remaining = 3;

        auto finish = [&]() {
            std::unique_lock<boost::fibers::mutex> lk(mu);
            --remaining;
            cv.notify_one();
        };

        LaunchFiber(FiberPriority::BACKGROUND, [&]() {
            order.push_back(1);
            finish();
        });
        LaunchFiber([&]() {
            order.push_back(2);
            finish();
        });
        LaunchFiber(FiberPriority::HIGH, [&]() {
            order.push_back(3);
            finish();
        });

        std::unique_lock<boost::fibers::mutex> lk(mu);
        cv.wait(lk, [&]() { return remaining == 0; });
        return order;
    });

    el.IoContext().stop();

    EXPECT_EQ((std::vector<int>{3, 2, 1}), run_order);
}

TEST(FiberPriorityTest, SamePriorityFibersKeepFifoOrderInsideEachQueue) {
    EventLoop el(0, 0);
    el.Run();

    auto run_order = el.AwaitDispatch([]() {
        std::vector<int>                  order;
        boost::fibers::mutex              mu;
        boost::fibers::condition_variable cv;
        int                               remaining = 4;

        auto finish = [&]() {
            std::unique_lock<boost::fibers::mutex> lk(mu);
            --remaining;
            cv.notify_one();
        };

        LaunchFiber(FiberPriority::BACKGROUND, [&]() {
            order.push_back(1);
            finish();
        });
        LaunchFiber(FiberPriority::BACKGROUND, [&]() {
            order.push_back(2);
            finish();
        });
        LaunchFiber(FiberPriority::HIGH, [&]() {
            order.push_back(3);
            finish();
        });
        LaunchFiber(FiberPriority::HIGH, [&]() {
            order.push_back(4);
            finish();
        });

        std::unique_lock<boost::fibers::mutex> lk(mu);
        cv.wait(lk, [&]() { return remaining == 0; });
        return order;
    });

    el.IoContext().stop();

    EXPECT_EQ((std::vector<int>{3, 4, 1, 2}), run_order);
}

TEST(FiberPriorityTest, DispatcherProgressesRemoteWakeEvenWhenNormalFiberIsAlwaysReady) {
    EventLoop el0(0, 0);
    EventLoop el1(0, 1);
    el0.Run();
    el1.Run();

    auto result = el0.Submit([&el1]() {
        std::atomic_bool done{false};

        auto busy = LaunchFiber([&done]() {
            while (!done.load(std::memory_order_acquire)) {
                boost::this_fiber::yield();
            }
        });

        auto prom = std::make_shared<boost::fibers::promise<int>>();
        auto fut  = prom->get_future();

        el1.Dispatch([prom]() { prom->set_value(7); });

        const int value = fut.get();
        done.store(true, std::memory_order_release);
        busy.join();
        return value;
    });

    EXPECT_EQ(result.wait_for(std::chrono::seconds(1)), std::future_status::ready);
    EXPECT_EQ(result.get(), 7);

    el0.IoContext().stop();
    el1.IoContext().stop();
}

} // namespace
} // namespace idlekv
