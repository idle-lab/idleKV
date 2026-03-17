

#include "server/thread_state.h"
#include "server/coro_tracking.h"
#include "common/logger.h"

#include <cstddef>
#include <mimalloc.h>

namespace idlekv {

thread_local ThreadState* ThreadState::state_ = nullptr;

auto ThreadState::init(size_t pool_index, EventLoop* el, std::thread::native_handle_type thread_id)
    -> void {
    // create thread-local resources once for the current worker thread.
    state_              = new ThreadState();
    state_->data_heap_  = mi_heap_new();
    state_->el_         = el;
    state_->thread_id_  = thread_id;
    state_->pool_index_ = pool_index;
}

auto ThreadState::on_startup() -> uint64_t {
    auto* tl = ThreadState::tlocal();
    CHECK(tl != nullptr);

    const auto coro_id = tl->next_coro_id_;

    tl->coro_state_[coro_id] = {
        .coro_id = coro_id,
        .sched_epoch = 0,
        .entered_at = Clock::now(),
    };
    tl->cur_coro_ = &tl->coro_state_[coro_id];
    tl->next_coro_id_++;
    return coro_id;
}

auto ThreadState::on_resume(uint64_t coro_id) -> void {
    auto* tl = ThreadState::tlocal();
    CHECK(tl != nullptr);

    auto it = tl->coro_state_.find(coro_id);
    CHECK(it != tl->coro_state_.end());

    tl->cur_coro_ = &it->second;
    tl->cur_coro_->sched_epoch++;
    tl->cur_coro_->entered_at = Clock::now();
}

auto ThreadState::on_suspend_or_finish(uint64_t coro_id, bool done) -> void {
    auto* tl = ThreadState::tlocal();
    CHECK(tl != nullptr);

    if (done) {
        auto count = tl->coro_state_.erase(coro_id);
        CHECK_EQ(count, 1);
    }
    
    tl->cur_coro_ = nullptr;
}

auto ThreadState::cur_coro() -> CoroState* {
    return ThreadState::tlocal()->cur_coro_;
}

auto has_thread_state() -> bool {
    return ThreadState::tlocal() != nullptr;
}

auto coro_tracking_on_startup() -> uint64_t {
    return ThreadState::on_startup();
}

auto coro_tracking_on_resume(uint64_t coro_id) -> void {
    ThreadState::on_resume(coro_id);
}

auto coro_tracking_on_suspend_or_finish(uint64_t coro_id, bool done) -> void {
    ThreadState::on_suspend_or_finish(coro_id, done);
}

} // namespace idlekv
