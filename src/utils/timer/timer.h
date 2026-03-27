#pragma once

#include "common/asio_no_exceptions.h"
#include "server/fiber_runtime.h"

#include <chrono>

namespace idlekv {

auto SetTimeout(std::chrono::steady_clock::duration dur) -> std::error_code;

} // namespace idlekv
