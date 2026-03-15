#include "db/task_queue.h"

#include <atomic>
#include <barrier>
#include <chrono>
#include <future>
#include <gtest/gtest.h>
#include <mutex>
#include <thread>
#include <vector>

namespace {

using idlekv::TaskQueue;
using namespace std::chrono_literals;

template <class Predicate>
auto wait_until(Predicate&& pred, std::chrono::milliseconds timeout = 2s) -> bool {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) {
            return true;
        }
        std::this_thread::sleep_for(1ms);
    }

    return pred();
}

TEST(TaskQueueTest, IgnoresTasksBeforeStartAndRunsTasksAfterStart) {
    TaskQueue        queue("task-queue-start");
    std::atomic<int> executed{0};

    queue.add([&] { executed.fetch_add(1, std::memory_order_relaxed); });
    EXPECT_EQ(executed.load(std::memory_order_relaxed), 0);

    queue.start();
    queue.add([&] { executed.fetch_add(1, std::memory_order_relaxed); });

    ASSERT_TRUE(wait_until([&] { return executed.load(std::memory_order_acquire) == 1; }));

    queue.close();
    EXPECT_EQ(executed.load(std::memory_order_relaxed), 1);
}

TEST(TaskQueueTest, PreservesSubmissionOrderForSingleProducer) {
    TaskQueue        queue("task-queue-order");
    std::mutex       mu;
    std::vector<int> executed;

    queue.start();

    for (int i = 0; i < 6; ++i) {
        queue.add([&, i] {
            std::lock_guard<std::mutex> lk(mu);
            executed.push_back(i);
        });
    }

    ASSERT_TRUE(wait_until([&] {
        std::lock_guard<std::mutex> lk(mu);
        return executed.size() == 6;
    }));

    queue.close();

    const std::vector<int> expected = {0, 1, 2, 3, 4, 5};
    EXPECT_EQ(executed, expected);
}

TEST(TaskQueueTest, ExecutesConcurrentProducerTasksExactlyOnce) {
    constexpr int producer_count     = 4;
    constexpr int tasks_per_producer = 200;
    constexpr int total_tasks        = producer_count * tasks_per_producer;

    TaskQueue        queue("task-queue-concurrent");
    std::barrier     start_line(producer_count);
    std::atomic<int> completed{0};
    std::mutex       seen_mu;
    std::vector<int> seen(total_tasks, 0);
    std::vector<std::thread> producers;

    queue.start();
    producers.reserve(producer_count);

    for (int producer = 0; producer < producer_count; ++producer) {
        producers.emplace_back([&, producer] {
            start_line.arrive_and_wait();

            for (int item = 0; item < tasks_per_producer; ++item) {
                const int value = producer * tasks_per_producer + item;
                queue.add([&, value] {
                    {
                        std::lock_guard<std::mutex> lk(seen_mu);
                        ++seen[value];
                    }
                    completed.fetch_add(1, std::memory_order_release);
                });
            }
        });
    }

    for (auto& producer : producers) {
        producer.join();
    }

    ASSERT_TRUE(wait_until(
        [&] { return completed.load(std::memory_order_acquire) == total_tasks; }, 5s));

    queue.close();

    for (int count : seen) {
        EXPECT_EQ(count, 1);
    }
}

TEST(TaskQueueTest, CloseDrainsQueuedTasksAndRejectsNewOnes) {
    TaskQueue        queue("task-queue-close");
    std::atomic<int> executed{0};
    std::promise<void> first_started;
    auto                first_started_future = first_started.get_future();
    std::promise<void>  release_first;
    auto                release_first_future = release_first.get_future().share();

    queue.start();
    queue.add([&] {
        executed.fetch_add(1, std::memory_order_relaxed);
        first_started.set_value();
        release_first_future.wait();
    });

    ASSERT_EQ(first_started_future.wait_for(1s), std::future_status::ready);

    queue.add([&] { executed.fetch_add(1, std::memory_order_relaxed); });

    std::thread closer([&] { queue.close(); });

    ASSERT_TRUE(wait_until([&] { return executed.load(std::memory_order_acquire) == 1; }));

    release_first.set_value();
    closer.join();

    EXPECT_EQ(executed.load(std::memory_order_relaxed), 2);

    queue.add([&] { executed.fetch_add(100, std::memory_order_relaxed); });
    std::this_thread::sleep_for(50ms);
    EXPECT_EQ(executed.load(std::memory_order_relaxed), 2);
}

TEST(TaskQueueTest, RetriesWhenRingBufferIsTemporarilyFull) {
    constexpr int queued_while_blocked = (1 << 10) + 128;

    TaskQueue          queue("task-queue-full");
    std::atomic<int>   executed{0};
    std::promise<void> first_started;
    auto               first_started_future = first_started.get_future();
    std::promise<void> release_first;
    auto               release_first_future = release_first.get_future().share();
    std::promise<void> producer_done;
    auto               producer_done_future = producer_done.get_future();

    queue.start();
    queue.add([&] {
        executed.fetch_add(1, std::memory_order_relaxed);
        first_started.set_value();
        release_first_future.wait();
    });

    ASSERT_EQ(first_started_future.wait_for(1s), std::future_status::ready);

    std::thread producer([&] {
        for (int i = 0; i < queued_while_blocked; ++i) {
            queue.add([&] { executed.fetch_add(1, std::memory_order_relaxed); });
        }
        producer_done.set_value();
    });

    EXPECT_EQ(producer_done_future.wait_for(50ms), std::future_status::timeout);

    release_first.set_value();
    producer.join();

    ASSERT_TRUE(wait_until([&] {
        return executed.load(std::memory_order_acquire) == queued_while_blocked + 1;
    }, 5s));

    queue.close();
    EXPECT_EQ(executed.load(std::memory_order_relaxed), queued_while_blocked + 1);
}

} // namespace
