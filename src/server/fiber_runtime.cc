#include "server/fiber_runtime.h"

#include "common/logger.h"
#include "server/el_pool.h"
#include "server/thread_state.h"

namespace boost {
namespace fibers {
namespace asio {

boost::asio::io_context::id round_robin::service::id;

} // namespace asio
} // namespace fibers
} // namespace boost

namespace idlekv {

auto CurrentIoContext() -> asio::io_context& {
    auto* state = ThreadState::Tlocal();
    CHECK(state != nullptr);
    return state->GetEventLoop()->IoContext();
}

auto FiberSleepFor(std::chrono::steady_clock::duration dur) -> std::error_code {
    asio::steady_timer timer(CurrentIoContext());
    timer.expires_after(dur);
    return FiberAwait<std::error_code>(
        [&](auto handler) { timer.async_wait(std::move(handler)); });
}

} // namespace idlekv
