#pragma once

#include <memory>
#include <string>
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

std::unique_ptr<Logger> make_default_logger();


} // namespace idlekv 

