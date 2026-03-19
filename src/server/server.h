#pragma once

#include "common/asio_no_exceptions.h"
#include "common/config.h"
#include "server/el_pool.h"
#include "server/handler.h"
#include "server/thread_state.h"

#include <asio.hpp>
#include <asio/ip/tcp.hpp>
#include <memory>
#include <vector>

namespace idlekv {

using asio::awaitable;

// coordinates listeners, event loops, and request handlers for the server process.
class Server {
public:
    Server(const Config& cfg);

    auto DoAccept(Handler* h) -> asio::awaitable<void>;

    auto PickUpConnEl(asio::ip::tcp::socket& sock) -> EventLoop*;

    void ListenAndServe();

    void RegisterHandler(std::unique_ptr<Handler> handler);

    auto GetEventLoopPool() -> EventLoopPool* { return elp_.get(); }

    void Stop();

private:
    std::unique_ptr<EventLoopPool> elp_;

    std::vector<std::unique_ptr<Handler>> handlers_;
    const Config*                         cfg_;
};

} // namespace idlekv
