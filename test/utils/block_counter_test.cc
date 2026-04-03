#include "server/el_pool.h"
#include "utils/fiber/block_counter.h"

#include <atomic>
#include <gtest/gtest.h>

namespace idlekv {
namespace {

auto RunBlockedWithoutDoneScenario() -> bool {
    EventLoop el(0, 0);
    el.Run();

    auto returned_without_done = el.AwaitDispatch([]() {
        utils::SingleWaiterBlockCounter counter;
        std::atomic_bool                waiter_returned{false};

        counter.Start(1);

        auto waiter = LaunchFiber(FiberPriority::HIGH, [&]() {
            counter.Wait();
            waiter_returned.store(true, std::memory_order_release);
        });

        for (size_t i = 0; i < 32; ++i) {
            boost::this_fiber::yield();
        }

        const bool returned = waiter_returned.load(std::memory_order_acquire);
        counter.Done();

        waiter.join();
        return returned;
    });

    el.IoContext().stop();
    return returned_without_done;
}

auto RunCrossEventLoopDoneScenario() -> bool {
    EventLoop waiter_el(0, 0);
    EventLoop worker_el(0, 1);
    waiter_el.Run();
    worker_el.Run();

    auto wait_completed = waiter_el.AwaitDispatch([&]() {
        utils::SingleWaiterBlockCounter counter;
        std::atomic_bool                wait_returned{false};

        counter.Start(1);

        worker_el.Dispatch([&counter]() { counter.Done(); });
        counter.Wait();
        wait_returned.store(true, std::memory_order_release);
        return wait_returned.load(std::memory_order_acquire);
    });

    waiter_el.IoContext().stop();
    worker_el.IoContext().stop();
    return wait_completed;
}

auto RunReuseBeforeWaiterResumesScenario() -> bool {
    EventLoop el(0, 0);
    el.Run();

    auto waiter_finished_first_generation = el.AwaitDispatch([]() {
        utils::SingleWaiterBlockCounter counter;
        std::atomic_bool                waiter_returned{false};
        std::atomic_bool                waiter_started_wait{false};
        std::atomic_bool                second_generation_started{false};

        counter.Start(1);

        auto waiter = LaunchFiber(FiberPriority::NORMAL, [&]() {
            waiter_started_wait.store(true, std::memory_order_release);
            counter.Wait();
            waiter_returned.store(true, std::memory_order_release);
        });

        auto finisher = LaunchFiber(FiberPriority::HIGH, [&]() {
            while (!waiter_started_wait.load(std::memory_order_acquire)) {
                boost::this_fiber::yield();
            }

            counter.Done();
            counter.Start(1);
            second_generation_started.store(true, std::memory_order_release);

            for (size_t i = 0; i < 32 && !waiter_returned.load(std::memory_order_acquire); ++i) {
                boost::this_fiber::yield();
            }

            counter.Done();
        });

        waiter.join();
        finisher.join();
        return waiter_returned.load(std::memory_order_acquire) &&
               second_generation_started.load(std::memory_order_acquire);
    });

    el.IoContext().stop();
    return waiter_finished_first_generation;
}

TEST(SingleWaiterBlockCounterTest, WaitStaysBlockedUntilDone) {
    EXPECT_FALSE(RunBlockedWithoutDoneScenario());
}

TEST(SingleWaiterBlockCounterTest, WaitHandlesDoneFromAnotherEventLoop) {
    EXPECT_TRUE(RunCrossEventLoopDoneScenario());
}

TEST(SingleWaiterBlockCounterTest, WaitCompletesItsGenerationAfterCounterReuse) {
    EXPECT_TRUE(RunReuseBeforeWaiterResumesScenario());
}

} // namespace
} // namespace idlekv
