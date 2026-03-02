#pragma once

#include <asio/awaitable.hpp>
#include <asio/co_spawn.hpp>
#include <asio/detached.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <thread>
#include <vector>

namespace idlekv {

class ThreadState {
public:
    ThreadState() : io_(1) {
        th_ = std::jthread([this]() {
            asio::executor_work_guard wg = asio::make_work_guard(io_);

            io_.run();
        });
    }

    auto io_context() -> asio::io_context& { return io_; }

    auto co_num() const -> uint32_t { return co_num_.load(std::memory_order_relaxed); }

    template<class T>
    auto assign(asio::awaitable<T> aw) -> void {
        asio::co_spawn(io_, std::move(aw), asio::detached);

        co_num_.fetch_add(1, std::memory_order_relaxed);
    }

private:
    asio::io_context io_;

    std::vector<asio::ip::tcp::socket> sockets_;
    std::atomic_uint32_t co_num_{0};

    std::jthread th_;
};

} // namespace idlekv
