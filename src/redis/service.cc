#include "redis/service.h"

#include "redis/connection.h"

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
#include <spdlog/fmt/fmt.h>

namespace idlekv {


auto RedisService::init(EventLoop* el) -> void {
    tl_ = new ServiceTLState();
    tl_->conn_pool().set_pool_size(ServiceTLState::kConnPoolSize);
    tl_->conn_pool().set_new([el]() -> ConnectionPtr {
        return std::make_unique<Connection>(el->io_context().get_executor());;
    });
}

auto RedisService::handle(asio::ip::tcp::socket socket) -> asio::awaitable<void> {
    auto& conn_list = tlocal()->conn_list();
    auto& conn_pool = tlocal()->conn_pool();

    // get a connection from the pool and put it to the front of the list.
    auto conn = conn_pool.get();
    conn->reset(std::move(socket));
    conn_list.emplace_front(conn.get());
    auto it = conn_list.begin();

    asio::co_spawn(conn->get_executor(), conn->handle_send(), asio::detached);

    co_await conn->handle_requests();

    conn_list.erase(it);

    conn->reset();
    conn_pool.put(std::move(conn));
}

thread_local RedisService::ServiceTLState* RedisService::tl_ = nullptr;

} // namespace idlekv
