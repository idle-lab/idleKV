#pragma once

#include <chrono>

namespace idlekv {

using Clock        = std::chrono::steady_clock;
using HighResClock = std::chrono::high_resolution_clock;

using TimePoint        = Clock::time_point;
using HighResTimePoint = HighResClock::time_point;

} // namespace idlekv
