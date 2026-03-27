#include <common/logger.h>
#include <utils/timer/timer.h>

namespace idlekv {

auto SetTimeout(std::chrono::steady_clock::duration dur) -> std::error_code {
    return FiberSleepFor(dur);
}

} // namespace idlekv
