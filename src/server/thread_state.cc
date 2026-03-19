

#include "server/thread_state.h"
#include "server/coro_tracking.h"
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

auto ThreadState::OnStartup() -> uint64_t {
    auto* tl = ThreadState::Tlocal();
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

auto ThreadState::OnResume(uint64_t coro_id) -> void {
    auto* tl = ThreadState::Tlocal();
    CHECK(tl != nullptr);

    auto it = tl->coro_state_.find(coro_id);
    CHECK(it != tl->coro_state_.end());

    tl->cur_coro_ = &it->second;
    tl->cur_coro_->sched_epoch++;
    tl->cur_coro_->entered_at = Clock::now();
}

auto ThreadState::OnSuspendOrFinish(uint64_t coro_id, bool done) -> void {
    auto* tl = ThreadState::Tlocal();
    CHECK(tl != nullptr);

    if (done) {
        auto count = tl->coro_state_.erase(coro_id);
        CHECK_EQ(count, 1);
    }
    
    tl->cur_coro_ = nullptr;
}

auto ThreadState::CurCoro() -> CoroState* {
    return ThreadState::Tlocal()->cur_coro_;
}

auto HasThreadState() -> bool {
    return ThreadState::Tlocal() != nullptr;
}

auto CoroTrackingOnStartup() -> uint64_t {
    return ThreadState::OnStartup();
}

auto CoroTrackingOnResume(uint64_t coro_id) -> void {
    ThreadState::OnResume(coro_id);
}

auto CoroTrackingOnSuspendOrFinish(uint64_t coro_id, bool done) -> void {
    ThreadState::OnSuspendOrFinish(coro_id, done);
}

} // namespace idlekv
