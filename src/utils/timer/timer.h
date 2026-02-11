#pragma once

#include <asio/asio.hpp>
#include <asiochan/asiochan.hpp>
#include <chrono>

namespace idlekv {

auto timer_context() -> asio::io_context&;

auto set_timeout(std::chrono::steady_clock::duration dur) -> asiochan::read_channel<void>;

} // namespace idlekv
