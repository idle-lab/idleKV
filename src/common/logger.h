#pragma once

#include <memory>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>

namespace idlekv {

class Logger {
public:
    Logger(std::string name, spdlog::sink_ptr ptr) : lg_(name, ptr) {}

    Logger(std::string name, spdlog::sinks_init_list lists) : lg_(name, lists) {}
private:
    spdlog::logger lg_;
};

std::unique_ptr<Logger> make_default_logger() {
    auto sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();

    return std::make_unique<Logger>("default", sink);
}
    
} // namespace idlekv 

