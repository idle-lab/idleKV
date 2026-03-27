

#include "server/thread_state.h"

#include "common/logger.h"

#include <cstddef>
#include <mimalloc.h>

namespace idlekv {

thread_local ThreadState* ThreadState::state_ = nullptr;

auto ThreadState::Init(size_t PoolIndex, EventLoop* el, std::thread::native_handle_type ThreadId)
    -> void {
    // create thread-local resources once for the current worker thread.
    state_              = new ThreadState();
    state_->data_heap_  = mi_heap_new();
    state_->el_         = el;
    state_->thread_id_  = ThreadId;
    state_->pool_index_ = PoolIndex;
}

} // namespace idlekv
