#pragma once

#include <cstdint>

namespace idlekv {

auto HasThreadState() -> bool;
auto CoroTrackingOnStartup() -> uint64_t;
auto CoroTrackingOnResume(uint64_t coro_id) -> void;
auto CoroTrackingOnSuspendOrFinish(uint64_t coro_id, bool done) -> void;

// Compatibility shims for Asio's coroutine tracking hooks.
inline auto has_thread_state() -> bool { return HasThreadState(); }
inline auto coro_tracking_on_startup() -> uint64_t { return CoroTrackingOnStartup(); }
inline auto coro_tracking_on_resume(uint64_t coro_id) -> void { CoroTrackingOnResume(coro_id); }
inline auto coro_tracking_on_suspend_or_finish(uint64_t coro_id, bool done) -> void {
    CoroTrackingOnSuspendOrFinish(coro_id, done);
}

} // namespace idlekv
