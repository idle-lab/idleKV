#pragma once

#include <asio/awaitable.hpp>
#include <asio/co_spawn.hpp>
#include <asio/detached.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>
#include <cstddef>
#include <mimalloc.h>
#include <sched.h>
#include <thread>

namespace idlekv {

class ThreadState {
public:
    ThreadState() = default;

    static auto init(size_t pool_index, asio::io_context& io,
                     std::thread::native_handle_type thread_id) -> void;

    auto io_context() -> asio::io_context& { return *io_; }

    auto data_heap() -> mi_heap_t* { return data_heap_; }

    static auto tlocal() -> ThreadState* { return state_; }

private:
    asio::io_context* io_;

    mi_heap_t* data_heap_;

    std::thread::native_handle_type thread_id_;
    size_t                          pool_index_;

    static thread_local ThreadState* state_;
};

} // namespace idlekv
