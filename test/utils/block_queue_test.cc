#include "utils/block_queue/block_queue.h"

#include <asio/bind_executor.hpp>
#include <asio/io_context.hpp>
#include <atomic>
#include <barrier>
#include <chrono>
#include <condition_variable>
#include <gtest/gtest.h>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

namespace {

using idlekv::utils::BlockingQueue;

struct IoContextThread {
    asio::io_context                                           ctx;
    asio::executor_work_guard<asio::io_context::executor_type> guard{asio::make_work_guard(ctx)};
    std::thread                                                thread;

    ~IoContextThread() { stop(); }

    auto start() -> void {
        thread = std::thread([this] { ctx.run(); });
    }

    auto stop() -> void {
        guard.reset();
        if (thread.joinable()) {
            thread.join();
        }
    }
};

TEST(BlockingQueueTest, TryPushTryEmplaceAndTryPopFollowFifo) {
    BlockingQueue<std::string> queue;

    EXPECT_EQ(queue.capacity(), 0U);
    EXPECT_TRUE(queue.empty());
    EXPECT_EQ(queue.size(), 0U);

    EXPECT_TRUE(queue.try_push("first"));
    EXPECT_TRUE(queue.try_emplace(3, 'x'));

    EXPECT_EQ(queue.size(), 2U);
    EXPECT_EQ(queue.try_pop(), std::optional<std::string>("first"));
    EXPECT_EQ(queue.try_pop(), std::optional<std::string>("xxx"));
    EXPECT_TRUE(queue.empty());
    EXPECT_EQ(queue.try_pop(), std::nullopt);
}

TEST(BlockingQueueTest, BoundedQueueRejectsTryPushWhenFull) {
    BlockingQueue<int> queue(2);

    EXPECT_EQ(queue.capacity(), 2U);
    EXPECT_TRUE(queue.try_push(1));
    EXPECT_TRUE(queue.try_push(2));
    EXPECT_FALSE(queue.try_push(3));

    EXPECT_EQ(queue.size(), 2U);
    EXPECT_EQ(queue.try_pop(), std::optional<int>(1));
    EXPECT_EQ(queue.try_pop(), std::optional<int>(2));
    EXPECT_EQ(queue.try_pop(), std::nullopt);
}

TEST(BlockingQueueTest, CloseRejectsNewPushesButPreservesBufferedItems) {
    BlockingQueue<int> queue;

    ASSERT_TRUE(queue.try_push(10));
    ASSERT_TRUE(queue.try_push(20));

    queue.close();

    EXPECT_TRUE(queue.closed());
    EXPECT_FALSE(queue.try_push(30));
    EXPECT_FALSE(queue.try_emplace(40));

    EXPECT_EQ(queue.try_pop(), std::optional<int>(10));
    EXPECT_EQ(queue.try_pop(), std::optional<int>(20));
    EXPECT_EQ(queue.try_pop(), std::nullopt);
}

TEST(BlockingQueueTest, AsyncPopWaitsUntilProducerPushes) {
    BlockingQueue<int> queue;
    asio::io_context   ctx;

    std::optional<std::optional<int>> popped;
    queue.async_pop(asio::bind_executor(
        ctx.get_executor(), [&](std::optional<int> value) { popped = std::move(value); }));

    EXPECT_FALSE(popped.has_value());

    EXPECT_TRUE(queue.try_push(42));

    ctx.run();

    ASSERT_TRUE(popped.has_value());
    EXPECT_EQ(*popped, std::optional<int>(42));
    EXPECT_TRUE(queue.empty());
}

TEST(BlockingQueueTest, AsyncPushWaitsForCapacityAndBecomesVisibleAfterPop) {
    BlockingQueue<int> queue(1);
    asio::io_context   ctx;

    ASSERT_TRUE(queue.try_push(1));

    std::optional<bool> accepted;
    queue.async_push(2, asio::bind_executor(ctx.get_executor(), [&](bool ok) { accepted = ok; }));

    EXPECT_FALSE(accepted.has_value());
    EXPECT_EQ(queue.try_pop(), std::optional<int>(1));
    EXPECT_EQ(queue.size(), 1U);
    EXPECT_EQ(queue.try_pop(), std::optional<int>(2));
    EXPECT_EQ(queue.try_pop(), std::nullopt);

    ctx.run();

    ASSERT_TRUE(accepted.has_value());
    EXPECT_TRUE(*accepted);
}

TEST(BlockingQueueTest, AsyncPushHandsOffDirectlyToWaitingPop) {
    BlockingQueue<int> queue;
    asio::io_context   pop_ctx;
    asio::io_context   push_ctx;

    std::optional<std::optional<int>> popped;
    std::optional<bool>               accepted;

    queue.async_pop(asio::bind_executor(
        pop_ctx.get_executor(), [&](std::optional<int> value) { popped = std::move(value); }));
    queue.async_push(7,
                     asio::bind_executor(push_ctx.get_executor(), [&](bool ok) { accepted = ok; }));

    EXPECT_TRUE(queue.empty());

    pop_ctx.run();
    push_ctx.run();

    ASSERT_TRUE(popped.has_value());
    EXPECT_EQ(*popped, std::optional<int>(7));
    ASSERT_TRUE(accepted.has_value());
    EXPECT_TRUE(*accepted);
}

TEST(BlockingQueueTest, CloseCompletesWaitingAsyncPopWithNullopt) {
    BlockingQueue<int> queue;
    asio::io_context   ctx;

    std::optional<std::optional<int>> popped;
    queue.async_pop(asio::bind_executor(
        ctx.get_executor(), [&](std::optional<int> value) { popped = std::move(value); }));

    queue.close();
    ctx.run();

    ASSERT_TRUE(popped.has_value());
    EXPECT_EQ(*popped, std::nullopt);
}

TEST(BlockingQueueTest, CloseRejectsWaitingAsyncPush) {
    BlockingQueue<int> queue(1);
    asio::io_context   ctx;

    ASSERT_TRUE(queue.try_push(1));

    std::optional<bool> accepted;
    queue.async_push(2, asio::bind_executor(ctx.get_executor(), [&](bool ok) { accepted = ok; }));

    queue.close();
    ctx.run();

    ASSERT_TRUE(accepted.has_value());
    EXPECT_FALSE(*accepted);
    EXPECT_EQ(queue.try_pop(), std::optional<int>(1));
    EXPECT_EQ(queue.try_pop(), std::nullopt);
}

TEST(BlockingQueueTest, ConcurrentTryPushAndTryPopTransferEachValueExactlyOnce) {
    constexpr int producer_count     = 4;
    constexpr int consumer_count     = 4;
    constexpr int items_per_producer = 200;
    constexpr int total_items        = producer_count * items_per_producer;

    BlockingQueue<int> queue;

    std::barrier             start_line(producer_count + consumer_count);
    std::atomic<bool>        push_failed{false};
    std::atomic<bool>        saw_bad_value{false};
    std::atomic<int>         consumed{0};
    std::mutex               seen_mu;
    std::vector<int>         seen(total_items, 0);
    std::vector<std::thread> producers;
    std::vector<std::thread> consumers;

    consumers.reserve(consumer_count);
    for (int consumer = 0; consumer < consumer_count; ++consumer) {
        consumers.emplace_back([&] {
            start_line.arrive_and_wait();

            for (;;) {
                auto value = queue.try_pop();
                if (!value.has_value()) {
                    if (queue.closed()) {
                        break;
                    }
                    std::this_thread::yield();
                    continue;
                }

                if (*value < 0 || *value >= total_items) {
                    saw_bad_value.store(true, std::memory_order_release);
                    continue;
                }

                {
                    std::lock_guard<std::mutex> lk(seen_mu);
                    ++seen[*value];
                }
                consumed.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    producers.reserve(producer_count);
    for (int producer = 0; producer < producer_count; ++producer) {
        producers.emplace_back([&, producer] {
            start_line.arrive_and_wait();

            for (int i = 0; i < items_per_producer; ++i) {
                const int value = producer * items_per_producer + i;
                if (!queue.try_push(value)) {
                    push_failed.store(true, std::memory_order_release);
                    return;
                }
            }
        });
    }

    for (auto& producer : producers) {
        producer.join();
    }

    queue.close();

    for (auto& consumer : consumers) {
        consumer.join();
    }

    EXPECT_FALSE(push_failed.load(std::memory_order_acquire));
    EXPECT_FALSE(saw_bad_value.load(std::memory_order_acquire));
    EXPECT_EQ(consumed.load(std::memory_order_acquire), total_items);
    EXPECT_TRUE(queue.empty());

    for (int count : seen) {
        EXPECT_EQ(count, 1);
    }
}

TEST(BlockingQueueTest, ConcurrentAsyncPopsAcrossExecutorsReceiveAllProducedValues) {
    constexpr int executor_count     = 4;
    constexpr int producer_count     = 4;
    constexpr int items_per_producer = 25;
    constexpr int total_values       = producer_count * items_per_producer;
    constexpr int total_waiters      = total_values + executor_count;

    std::vector<std::unique_ptr<IoContextThread>> executors;
    executors.reserve(executor_count);
    for (int i = 0; i < executor_count; ++i) {
        auto executor = std::make_unique<IoContextThread>();
        executor->start();
        executors.push_back(std::move(executor));
    }

    BlockingQueue<int> queue;

    std::atomic<bool> push_failed{false};
    std::atomic<bool> saw_bad_value{false};
    std::atomic<int>  received_values{0};
    std::atomic<int>  received_nulls{0};
    std::atomic<int>  completed{0};
    std::mutex        seen_mu;
    std::vector<int>  seen(total_values, 0);

    std::mutex              done_mu;
    std::condition_variable done_cv;

    for (int i = 0; i < total_waiters; ++i) {
        auto& executor = executors[i % executor_count];
        queue.async_pop(
            asio::bind_executor(executor->ctx.get_executor(), [&](std::optional<int> value) {
                if (value.has_value()) {
                    if (*value < 0 || *value >= total_values) {
                        saw_bad_value.store(true, std::memory_order_release);
                    } else {
                        std::lock_guard<std::mutex> lk(seen_mu);
                        ++seen[*value];
                    }
                    received_values.fetch_add(1, std::memory_order_relaxed);
                } else {
                    received_nulls.fetch_add(1, std::memory_order_relaxed);
                }

                if (completed.fetch_add(1, std::memory_order_acq_rel) + 1 == total_waiters) {
                    std::lock_guard<std::mutex> lk(done_mu);
                    done_cv.notify_one();
                }
            }));
    }

    std::barrier             start_line(producer_count);
    std::vector<std::thread> producers;
    producers.reserve(producer_count);

    for (int producer = 0; producer < producer_count; ++producer) {
        producers.emplace_back([&, producer] {
            start_line.arrive_and_wait();

            for (int i = 0; i < items_per_producer; ++i) {
                const int value = producer * items_per_producer + i;
                if (!queue.try_push(value)) {
                    push_failed.store(true, std::memory_order_release);
                    return;
                }
            }
        });
    }

    for (auto& producer : producers) {
        producer.join();
    }

    queue.close();

    {
        std::unique_lock<std::mutex> lk(done_mu);
        ASSERT_TRUE(done_cv.wait_for(lk, std::chrono::seconds(5), [&] {
            return completed.load(std::memory_order_acquire) == total_waiters;
        }));
    }

    for (auto& executor : executors) {
        executor->stop();
    }

    EXPECT_FALSE(push_failed.load(std::memory_order_acquire));
    EXPECT_FALSE(saw_bad_value.load(std::memory_order_acquire));
    EXPECT_EQ(received_values.load(std::memory_order_acquire), total_values);
    EXPECT_EQ(received_nulls.load(std::memory_order_acquire), executor_count);

    for (int count : seen) {
        EXPECT_EQ(count, 1);
    }
}

TEST(BlockingQueueTest, ConcurrentAsyncPushesAreAdmittedAsConsumersMakeProgress) {
    constexpr int executor_count = 4;
    constexpr int waiter_count   = 32;
    constexpr int consumer_count = 2;

    std::vector<std::unique_ptr<IoContextThread>> executors;
    executors.reserve(executor_count);
    for (int i = 0; i < executor_count; ++i) {
        auto executor = std::make_unique<IoContextThread>();
        executor->start();
        executors.push_back(std::move(executor));
    }

    BlockingQueue<int> queue(1);
    ASSERT_TRUE(queue.try_push(-1));

    std::atomic<int>  accepted{0};
    std::atomic<int>  rejected{0};
    std::atomic<int>  popped{0};
    std::atomic<bool> saw_bad_value{false};

    std::mutex              accepted_mu;
    std::condition_variable accepted_cv;

    for (int value = 0; value < waiter_count; ++value) {
        auto& executor = executors[value % executor_count];
        queue.async_push(value, asio::bind_executor(executor->ctx.get_executor(), [&](bool ok) {
                             if (ok) {
                                 if (accepted.fetch_add(1, std::memory_order_acq_rel) + 1 ==
                                     waiter_count) {
                                     std::lock_guard<std::mutex> lk(accepted_mu);
                                     accepted_cv.notify_one();
                                 }
                             } else {
                                 rejected.fetch_add(1, std::memory_order_relaxed);
                             }
                         }));
    }

    std::mutex               seen_mu;
    std::vector<int>         seen(waiter_count, 0);
    std::vector<std::thread> consumers;
    consumers.reserve(consumer_count);

    for (int consumer = 0; consumer < consumer_count; ++consumer) {
        consumers.emplace_back([&] {
            for (;;) {
                if (popped.load(std::memory_order_acquire) >= waiter_count + 1) {
                    break;
                }

                auto value = queue.try_pop();
                if (!value.has_value()) {
                    std::this_thread::yield();
                    continue;
                }

                const int pop_index = popped.fetch_add(1, std::memory_order_acq_rel) + 1;
                if (*value >= 0) {
                    if (*value >= waiter_count) {
                        saw_bad_value.store(true, std::memory_order_release);
                    } else {
                        std::lock_guard<std::mutex> lk(seen_mu);
                        ++seen[*value];
                    }
                }

                if (pop_index >= waiter_count + 1) {
                    break;
                }
            }
        });
    }

    {
        std::unique_lock<std::mutex> lk(accepted_mu);
        ASSERT_TRUE(accepted_cv.wait_for(lk, std::chrono::seconds(5), [&] {
            return accepted.load(std::memory_order_acquire) == waiter_count;
        }));
    }

    for (auto& consumer : consumers) {
        consumer.join();
    }

    for (auto& executor : executors) {
        executor->stop();
    }

    EXPECT_FALSE(saw_bad_value.load(std::memory_order_acquire));
    EXPECT_EQ(rejected.load(std::memory_order_acquire), 0);
    EXPECT_EQ(accepted.load(std::memory_order_acquire), waiter_count);
    EXPECT_EQ(popped.load(std::memory_order_acquire), waiter_count + 1);
    EXPECT_TRUE(queue.empty());

    for (int count : seen) {
        EXPECT_EQ(count, 1);
    }
}

} // namespace
