#pragma once

#include "absl/container/flat_hash_map.h"
#include "common/asio_no_exceptions.h"
#include "server/el_pool.h"

#include <asio/awaitable.hpp>
#include <asio/co_spawn.hpp>
#include <asio/detached.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <mimalloc.h>
#include <sched.h>
#include <thread>

namespace idlekv {

// stores per-thread runtime state such as allocator heap and bound event loop.
class ThreadState {
    using Clock = std::chrono::steady_clock;
    using TimePoint  = std::chrono::steady_clock::time_point;
    struct CoroState {
        uint64_t coro_id;
        uint64_t sched_epoch;
        TimePoint entered_at;
    };
public:
    ThreadState() = default;

    static auto init(size_t pool_index, EventLoop* el, std::thread::native_handle_type thread_id)
        -> void;

    // on_startup should call on every coro start up.
    static auto on_startup() -> uint64_t;
    static auto on_resume(uint64_t coro_id) -> void;
    static auto on_suspend_or_finish(uint64_t coro_id, bool done) -> void;
    static auto cur_coro() -> CoroState*;

    static auto tlocal() -> ThreadState* { return state_; }

    auto event_loop() -> EventLoop* { return el_; }

    auto data_heap() -> mi_heap_t* { return data_heap_; }

    auto pool_index() -> size_t { return pool_index_; }

private:
    EventLoop* el_;

    mi_heap_t* data_heap_;

    std::thread::native_handle_type thread_id_;
    size_t                          pool_index_;

    absl::flat_hash_map<uint64_t, CoroState> coro_state_;
    CoroState*                              cur_coro_     = nullptr;
    uint64_t                                next_coro_id_ = 1;

    static thread_local ThreadState* state_;
};

} // namespace idlekv
