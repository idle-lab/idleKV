#pragma once

#include <cstdint>

namespace idlekv {

auto has_thread_state() -> bool;
auto coro_tracking_on_startup() -> uint64_t;
auto coro_tracking_on_resume(uint64_t coro_id) -> void;
auto coro_tracking_on_suspend_or_finish(uint64_t coro_id, bool done) -> void;

} // namespace idlekv
