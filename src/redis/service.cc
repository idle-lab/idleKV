#include "redis/service.h"

#include "common/logger.h"

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
#include <memory>

namespace idlekv {

auto RedisService::handle(asio::ip::tcp::socket socket) -> asio::awaitable<void> {
    auto conn = conn_pool_.get();
    conn->reset(std::move(socket));

    // LOG(debug, "connect a new client, {}:{}", conn->remote_endpoint().address().to_string(),
    //     conn->remote_endpoint().port());

    co_await conn->handle_requests();

    conn->reset();
    conn_pool_.put(std::move(conn));
}

auto RedisService::exec(Connection* c, const std::vector<std::string>& args) noexcept -> std::string {
    return engine_->exec(c, args);
}

thread_local utils::Pool<RedisService::ConnectionPtr> RedisService::conn_pool_;


} // namespace idlekv
