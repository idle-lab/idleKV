#include "redis/service.h"

#include "common/logger.h"
#include "server/thread_state.h"

#include <asio/as_tuple.hpp>
#include <asio/asio/error.hpp>
#include <asio/awaitable.hpp>
#include <asio/co_spawn.hpp>
#include <asio/detached.hpp>
#include <asio/error.hpp>
#include <asio/post.hpp>
#include <asio/use_awaitable.hpp>
#include <asiochan/asiochan.hpp>
#include <asiochan/select.hpp>
#include <atomic>
#include <memory>
#include <spdlog/fmt/fmt.h>

namespace idlekv {

auto RedisService::init(EventLoop* el) -> void {
    tl_ = new ServiceTLState();
    tl_->conn_pool().set_pool_size(64);
    tl_->conn_pool().set_new([this]() -> ConnectionPtr {
        return std::make_unique<Connection>(this);
    });
}

auto RedisService::handle(asio::ip::tcp::socket socket) -> asio::awaitable<void> {
    auto el = co_await asio::this_coro::executor;
    auto& conn_list = tlocal()->conn_list();
    auto& conn_pool = tlocal()->conn_pool();
    
    // get a connection from the pool and put it to the front of the list.
    auto conn = conn_pool.get();
    conn->reset(std::move(socket));

    conn_list.emplace_front(conn.get());
    auto it = conn_list.begin();

    co_await conn->handle_requests();

    conn_list.erase(it);

    conn->reset();
    conn_pool.put(std::move(conn));
}

auto RedisService::exec(Connection* c, std::vector<std::string>& args) noexcept -> asio::awaitable<void> {
    auto& sender = c->sender();

    std::transform(args[0].begin(), args[0].end(), args[0].begin(),
                [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    if (args[0] == "ping") {
        switch (args.size()) {
            case 1:
                co_await sender.send_simple_string("PONG");
                break;
            case 2:
                co_await sender.send_simple_string(std::move(args[1]));
                break;
            default:
                co_await sender.send_error("ERR wrong number of arguments for 'ping' command");
                break;
        }
        co_return ;
    }

    auto res = engine_->exec(c, args);
    if (res.ok()) {
        co_await sender.send_simple_string(std::move(res.message()));
    } else {
        co_await sender.send_error(std::move(res.message()));
    }
    co_return ;
}

thread_local RedisService::ServiceTLState* RedisService::tl_ = nullptr;

} // namespace idlekv
