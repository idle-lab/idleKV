#pragma once

#include <chrono>

namespace idlekv {

namespace utils {
auto SetTimeout(std::chrono::steady_clock::duration dur) -> std::error_code;

}

using Clock        = std::chrono::steady_clock;
using HighResClock = std::chrono::high_resolution_clock;

using TimePoint        = Clock::time_point;
using HighResTimePoint = HighResClock::time_point;

} // namespace idlekv
