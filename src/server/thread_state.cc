

#include "server/thread_state.h"
#include <cstdint>

namespace idlekv {

thread_local ThreadState* ThreadState::state_ = nullptr;

auto ThreadState::init(asio::io_context& io, uint32_t thread_id) -> void {
    io_ = &io;
    thread_id_ = thread_id;
    state_ = this;
}


} // namespace idlekv