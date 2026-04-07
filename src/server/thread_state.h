#pragma once

#include <cstddef>
#include <cstdint>
#include <mimalloc.h>
#include <sched.h>
#include <thread>

namespace idlekv {

class EventLoop;

// stores per-thread runtime state such as allocator heap and bound event loop.
class ThreadState {
public:
    ThreadState() = default;

    static auto Init(size_t PoolIndex, EventLoop* el, std::thread::native_handle_type ThreadId)
        -> void;

    static auto Tlocal() -> ThreadState* { return state_; }

    auto GetEventLoop() -> EventLoop* { return el_; }

    auto DataHeap() -> mi_heap_t* { return data_heap_; }

    auto PoolIndex() -> size_t { return pool_index_; }

private:
    EventLoop* el_;

    mi_heap_t* data_heap_;

    std::thread::native_handle_type thread_id_;
    size_t                          pool_index_;

    uint64_t next_coro_id_ = 1;

    static thread_local ThreadState* state_;
};

} // namespace idlekv
