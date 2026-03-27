#include "server/fiber_runtime.h"

#include "common/logger.h"
#include "server/el_pool.h"
#include "server/thread_state.h"
#include <boost/system/detail/error_code.hpp>

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

    boost::system::error_code ec;
    timer.async_wait(boost::fibers::asio::yield[ec]);

    return ec;
}

} // namespace idlekv
