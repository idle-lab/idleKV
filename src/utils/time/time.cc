#include "server/fiber_runtime.h"

#include <common/logger.h>
#include <utils/time/time.h>

namespace idlekv::utils {

auto SetTimeout(std::chrono::steady_clock::duration dur) -> std::error_code {
    return FiberSleepFor(dur);
}

} // namespace idlekv::utils
