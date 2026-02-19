#pragma once

#include "server/config.h"
#include "server/handler.h"

#include <asio.hpp>
#include <asio/ip/tcp.hpp>
#include <memory>
#include <thread>
#include <vector>

namespace idlekv {

using asio::awaitable;

// Provide infrastructure such as network I/O and execution context, without involving business logic.
class Server {
public:
    Server() = delete;

    Server(const Config& cfg);

    void listen_and_server();

    void register_handler(std::shared_ptr<Handler> handler);

    asio::thread_pool& get_worker_pool() { return workers; }

    void accept();

    void stop();

private:
    std::vector<std::thread>              io_threads_;
    asio::thread_pool                     workers;
    std::vector<std::shared_ptr<Handler>> handlers_;
    std::unique_ptr<ServerConfig>         cfg_;
};

} // namespace idlekv