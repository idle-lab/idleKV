#pragma once

#include "server/config.h"
#include "server/handler.h"
#include "server/thread_state.h"

#include <asio.hpp>
#include <asio/ip/tcp.hpp>
#include <memory>
#include <thread>
#include <vector>

namespace idlekv {

using asio::awaitable;

// Provide infrastructure such as network I/O and execution context, without involving business
// logic.
class Server {
public:
    Server() = delete;

    Server(const Config& cfg);

    void listen_and_server();

    void register_handler(std::shared_ptr<Handler> handler);

    auto do_accept(std::shared_ptr<Handler> h, asio::io_context& io) -> asio::awaitable<void>;

    void stop();

private:
    std::vector<std::unique_ptr<ThreadState>> threads_;
    std::vector<std::shared_ptr<Handler>> handlers_;
    std::unique_ptr<ServerConfig>         cfg_;
};

} // namespace idlekv