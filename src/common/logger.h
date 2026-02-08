#pragma once

#include <memory>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>
#include <string>

namespace idlekv {

std::shared_ptr<spdlog::logger> make_default_logger();

#define LOG(level, ...) spdlog::level(__VA_ARGS__)

} // namespace idlekv
