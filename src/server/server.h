#pragma once

#include <memory>
#include <string>
#include <coroutine>

#include <asio.hpp>
#include <asio/ip/tcp.hpp>
#include <common/logger.h>
#include <server/config.h>
#include <server/handler.h>

namespace idlekv {

using asio::awaitable;

class Server {
public:
    Server() = delete;

    Server(const Config& cfg);

    void listen_and_server();

    void register_handler(std::shared_ptr<Handler> handler);

    asio::io_context& get_io_context() { return io_context_; }
    asio::thread_pool& get_worker_pool() { return workers; }

    void stop();
private:
    
    asio::io_context io_context_;
    asio::thread_pool workers;
    std::vector<std::shared_ptr<Handler>> handlers_;
    std::unique_ptr<ServerConfig> cfg_;
};

} // namespace idlekv