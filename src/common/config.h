#pragma once

#include <CLI11/CLI11.hpp>
#include <string>

const std::string SERVER_NAME = "idlekv";

namespace idlekv {

class Config {
public:
    Config() : opts_(SERVER_NAME) {
        opts_.add_option("--ip", ip_, "Listen IP");
        opts_.add_option("--port", port_, "Listen port");
    }

    // 不能拷贝不能移动
    Config(const Config&) = delete;
    Config(Config&&) = delete;
    Config& operator=(const Config&) = delete;
    Config& operator=(Config&&) = delete;

    void parse(int argc, char** argv) noexcept {
        try {
            opts_.parse(argc, argv);
        } catch (const CLI::ParseError& e) {
            spdlog::error("{}", e.what());
        }
    }

    std::string ip_, port_;

private:
    CLI::App opts_;
};

} // namespace idlekv 
