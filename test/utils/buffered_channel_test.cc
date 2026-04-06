#include "server/el_pool.h"
#include "utils/fiber/buffered_channel.h"

#include <algorithm>
#include <atomic>
#include <boost/fiber/channel_op_status.hpp>
#include <chrono>
#include <future>
#include <gtest/gtest.h>
#include <thread>
#include <vector>

namespace idlekv {
namespace {

// TODO(cyb): enable once cross-thread fiber wakeups are stable with the
// custom boost::fibers::asio::round_robin scheduler used by EventLoop.
TEST(BufferedChannelTest, DISABLED_MpmcAcrossEventLoopsDeliversEveryValue) {
    EventLoopPool pool(2);
    pool.Run();

    utils::buffered_channel<int> channel(8);

    constexpr int kProducerCount     = 4;
    constexpr int kConsumerCount     = 4;
    constexpr int kValuesPerProducer = 512;
    constexpr int kTotalValues       = kProducerCount * kValuesPerProducer;

    std::atomic<int> producer_failures{0};
    std::atomic<int> consumer_failures{0};
    std::atomic<int> producers_left{kProducerCount};

    std::vector<std::future<void>> producer_futures;
    producer_futures.reserve(kProducerCount);

    std::vector<std::future<std::vector<int>>> consumer_futures;
    consumer_futures.reserve(kConsumerCount);

    for (int i = 0; i < kConsumerCount; ++i) {
        auto promise = std::make_shared<std::promise<std::vector<int>>>();
        consumer_futures.push_back(promise->get_future());

        pool.At(static_cast<size_t>(i % 2))->Dispatch([&channel, &consumer_failures, promise]() {
            std::vector<int> values;
            for (;;) {
                int  value = 0;
                auto st    = channel.pop(value);
                if (st == boost::fibers::channel_op_status::success) {
                    values.push_back(value);
                    continue;
                }

                if (st != boost::fibers::channel_op_status::closed) {
                    consumer_failures.fetch_add(1, std::memory_order_relaxed);
                }
                break;
            }

            promise->set_value(std::move(values));
        });
    }

    for (int producer_id = 0; producer_id < kProducerCount; ++producer_id) {
        auto promise = std::make_shared<std::promise<void>>();
        producer_futures.push_back(promise->get_future());

        pool.At(static_cast<size_t>(producer_id % 2))->Dispatch([&, producer_id, promise]() {
            for (int value = 0; value < kValuesPerProducer; ++value) {
                const int payload = producer_id * kValuesPerProducer + value;
                if (channel.push(payload) != boost::fibers::channel_op_status::success) {
                    producer_failures.fetch_add(1, std::memory_order_relaxed);
                    break;
                }
            }

            if (producers_left.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                channel.close();
            }
            promise->set_value();
        });
    }

    for (auto& future : producer_futures) {
        future.get();
    }

    std::vector<int> all_values;
    for (auto& future : consumer_futures) {
        auto values = future.get();
        all_values.insert(all_values.end(), values.begin(), values.end());
    }

    pool.Stop();

    EXPECT_EQ(producer_failures.load(std::memory_order_relaxed), 0);
    EXPECT_EQ(consumer_failures.load(std::memory_order_relaxed), 0);
    EXPECT_EQ(static_cast<int>(all_values.size()), kTotalValues);

    std::sort(all_values.begin(), all_values.end());
    for (int i = 0; i < kTotalValues; ++i) {
        EXPECT_EQ(all_values[i], i);
    }
}

TEST(BufferedChannelTest, DISABLED_CloseWakesBlockedProducerAndConsumerFibers) {
    EventLoopPool pool(2);
    pool.Run();

    utils::buffered_channel<int> empty_channel(2);
    utils::buffered_channel<int> full_channel(2);

    ASSERT_EQ(full_channel.try_push(1), boost::fibers::channel_op_status::success);
    ASSERT_EQ(full_channel.try_push(2), boost::fibers::channel_op_status::success);
    ASSERT_EQ(full_channel.try_push(99), boost::fibers::channel_op_status::full);

    auto consumer_ready   = std::make_shared<std::promise<void>>();
    auto blocked_consumer = std::make_shared<std::promise<boost::fibers::channel_op_status>>();
    auto producer_ready   = std::make_shared<std::promise<void>>();
    auto blocked_producer = std::make_shared<std::promise<boost::fibers::channel_op_status>>();

    auto consumer_ready_future   = consumer_ready->get_future();
    auto blocked_consumer_future = blocked_consumer->get_future();
    auto producer_ready_future   = producer_ready->get_future();
    auto blocked_producer_future = blocked_producer->get_future();

    pool.At(0)->Dispatch([&empty_channel, consumer_ready, blocked_consumer]() {
        consumer_ready->set_value();
        int value = 0;
        blocked_consumer->set_value(empty_channel.pop(value));
    });

    pool.At(1)->Dispatch([&full_channel, producer_ready, blocked_producer]() {
        producer_ready->set_value();
        blocked_producer->set_value(full_channel.push(3));
    });

    consumer_ready_future.get();
    producer_ready_future.get();
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    empty_channel.close();
    full_channel.close();

    EXPECT_EQ(blocked_consumer_future.get(), boost::fibers::channel_op_status::closed);
    EXPECT_EQ(blocked_producer_future.get(), boost::fibers::channel_op_status::closed);

    pool.Stop();
}

TEST(BufferedChannelTest, CloseWakesBlockedProducerThreadFallback) {
    utils::buffered_channel<int> channel(2);

    ASSERT_EQ(channel.try_push(1), boost::fibers::channel_op_status::success);
    ASSERT_EQ(channel.try_push(2), boost::fibers::channel_op_status::success);
    ASSERT_EQ(channel.try_push(99), boost::fibers::channel_op_status::full);

    auto future = std::async(std::launch::async, [&channel]() { return channel.push(3); });

    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    channel.close();

    EXPECT_EQ(future.get(), boost::fibers::channel_op_status::closed);
}

} // namespace
} // namespace idlekv
