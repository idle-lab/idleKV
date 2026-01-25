#pragma once

#include <cstdint>
#include <string>
#include <asio/asio.hpp>
#include <memory>
#include <asio/asio/ip/tcp.hpp>

#include <common/config.h>

namespace idlekv {


class ServerConfig {
public:
    ServerConfig() = default;

    static std::unique_ptr<ServerConfig> build(const Config& cfg);

    // ipv4地址
    uint16_t port;
    std::string ip;


    // io 和 命令 执行的线程数
    uint8_t io_threads, worker_threads;
private:

};



} // namespace idlekv