#pragma once

#include <asio/asio.hpp>
#include <common/config.h>
#include <cstdint>
#include <memory>
#include <string>

namespace idlekv {

class ServerConfig {
public:
    ServerConfig() = default;

    static std::unique_ptr<ServerConfig> build(const Config& cfg);

    // ipv4地址
    uint16_t    port;
    std::string ip;

    // io 和 命令 执行的线程数
    uint8_t io_threads, worker_threads;

private:
};

} // namespace idlekv