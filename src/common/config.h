#pragma once

#include <CLI11/CLI11.hpp>
#include <cstdint>
#include <spdlog/spdlog.h>
#include <string>

const std::string SERVER_NAME  = "idlekv";
const std::string DEFAULT_PORT = "4396";

constexpr uint64_t B  = 1;
constexpr uint64_t KB = 1024 * B;
constexpr uint64_t MB = 1024 * KB;
constexpr uint64_t GB = 1024 * MB;

#define DISCARD_RESULT(expr) void(expr);

namespace idlekv {

class Config {
public:
    Config() : opts_(SERVER_NAME) {
        opts_.add_option("--ip", ip_, "Listen IP")->default_val("127.0.0.1");
        opts_.add_option("--port", port_, "Listen port")->default_val(DEFAULT_PORT);

        opts_.add_option("-c,--config", config_file_path, "Config file path");
        opts_.add_option("--db_num", db_num_, "number of DB");
    }

    // 不能拷贝不能移动
    Config(const Config&)            = delete;
    Config(Config&&)                 = delete;
    Config& operator=(const Config&) = delete;
    Config& operator=(Config&&)      = delete;

    void parse(int argc, char** argv) { opts_.parse(argc, argv); }

    void parse_from_file() { throw std::runtime_error("Not yet implemented"); }

    bool has_config_file() const { return !config_file_path.empty(); }

    std::string ip_, port_;
    uint16_t    io_threads_     = 1;
    uint16_t    worker_threads_ = 0;

    std::string config_file_path;

    uint8_t db_num_{0};

private:
    CLI::App opts_;
};

} // namespace idlekv
