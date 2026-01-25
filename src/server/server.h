#pragma once

#include <memory>
#include <string>
#include <coroutine>

#include <asio.hpp>
#include <asio/ip/tcp.hpp>
#include <common/logger.h>
#include <server/config.h>

namespace idlekv {

using asio::awaitable;

class Server {
public:
    Server() = delete;

    Server(std::unique_ptr<Logger> lg, std::unique_ptr<ServerConfig> cfg);

    void listen_and_server();

    void stop();
private:
    
    asio::io_context io_context_;
    asio::thread_pool workers;
    std::unique_ptr<ServerConfig> cfg_;
    std::unique_ptr<Logger> lg_;
};

} // namespace idlekv