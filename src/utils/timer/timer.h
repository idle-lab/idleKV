#pragma once

#include "common/asio_no_exceptions.h"

#include <asio/asio.hpp>
#include <asiochan/asiochan.hpp>
#include <chrono>

namespace idlekv {

auto TimerContext() -> asio::io_context&;

auto SetTimeout(std::chrono::steady_clock::duration dur) -> asiochan::read_channel<void>;

} // namespace idlekv
