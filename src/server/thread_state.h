#pragma once

#include "server/xmalloc.h"
#include <asio/awaitable.hpp>
#include <asio/co_spawn.hpp>
#include <asio/detached.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory_resource>
#include <mimalloc.h>
#include <sched.h>
#include <thread>
#include <vector>

namespace idlekv {

constexpr size_t kTotalCpus = CPU_SETSIZE;

class ThreadState {
public:
    ThreadState() = default;

    auto init(asio::io_context& io, uint32_t thread_id) -> void;

    auto io_context() -> asio::io_context& { return *io_; }

    auto data_heap() -> mi_heap_t* { return data_heap_; }

    auto co_num() const -> uint32_t { return co_num_.load(std::memory_order_relaxed); }

    template<class T>
    auto assign(asio::awaitable<T> aw) -> void {
        asio::co_spawn(io_, std::move(aw), asio::detached);

        co_num_.fetch_add(1, std::memory_order_relaxed);
    }

    static auto tlocal() -> ThreadState* {
        return state_;
    }
private:
    asio::io_context* io_;

    mi_heap_t* data_heap_;

    uint32_t thread_id_;

    std::atomic_uint32_t co_num_{0};

    static thread_local ThreadState* state_;
};


} // namespace idlekv
