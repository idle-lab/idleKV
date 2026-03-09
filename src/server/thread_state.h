#pragma once

#include "server/el_pool.h"
#include <asio/awaitable.hpp>
#include <asio/co_spawn.hpp>
#include <asio/detached.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>
#include <cstddef>
#include <mimalloc.h>
#include <sched.h>
#include <thread>
#include <vector>

namespace idlekv {

class ThreadState {
public:
    ThreadState() = default;

    static auto init(size_t pool_index, EventLoop* el,
                     std::thread::native_handle_type thread_id) -> void;

    static auto tlocal() -> ThreadState* { return state_; }

    auto event_loop() -> EventLoop* { return el_; }

    auto data_heap() -> mi_heap_t* { return data_heap_; }


private:
    EventLoop* el_;

    mi_heap_t* data_heap_;

    std::thread::native_handle_type thread_id_;
    size_t                          pool_index_;

    static thread_local ThreadState* state_;
};

} // namespace idlekv
