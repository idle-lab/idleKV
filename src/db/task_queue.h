#pragma once

#include "common/logger.h"
#include "server/thread_state.h"
#include "utils/block_queue/block_queue.h"
#include <asio/awaitable.hpp>
#include <asio/use_awaitable.hpp>
#include <functional>   

namespace idlekv {

class TaskQueue {
public:
    TaskQueue() : queue_(0) {}

    auto add(std::function<asio::awaitable<void>()> task) -> void {
        CHECK(queue_.try_push(std::move(task)));
    }

    auto start() -> void {
        ThreadState::tlocal()->event_loop()->dispatch([this]() -> asio::awaitable<void> {
            while (true) {
                auto task = co_await queue_.async_pop(asio::use_awaitable);
                if (!task) {
                    break;
                }
                co_await (*task)();
            }
        }());
    }

    auto close() -> void {
        queue_.close();
        started_ = false;
    }

private:
    using Task = std::function<asio::awaitable<void>()>;

    utils::BlockingQueue<Task> queue_;
    bool                      started_ = false;
};


} // namespace idlekv