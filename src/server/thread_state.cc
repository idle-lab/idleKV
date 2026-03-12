

#include "server/thread_state.h"

#include <cstddef>
#include <mimalloc.h>

namespace idlekv {

thread_local ThreadState* ThreadState::state_ = nullptr;

auto ThreadState::init(size_t pool_index, EventLoop* el, std::thread::native_handle_type thread_id)
    -> void {
    state_              = new ThreadState();
    state_->data_heap_  = mi_heap_new();
    state_->el_         = el;
    state_->thread_id_  = thread_id;
    state_->pool_index_ = pool_index;
}

} // namespace idlekv