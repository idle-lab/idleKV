#include "redis/service.h"

#include "common/logger.h"
#include "redis/connection.h"
#include "redis/parser.h"

#include <array>
#include <asio/as_tuple.hpp>
#include <asio/asio/error.hpp>
#include <asio/awaitable.hpp>
#include <asio/buffer.hpp>
#include <asio/buffer_registration.hpp>
#include <asio/co_spawn.hpp>
#include <asio/detached.hpp>
#include <asio/error.hpp>
#include <asio/post.hpp>
#include <asio/use_awaitable.hpp>
#include <asiochan/asiochan.hpp>
#include <asiochan/select.hpp>
#include <cstddef>
#include <memory>
#include <optional>
#include <spdlog/fmt/fmt.h>
#include <vector>

namespace idlekv {


auto RedisService::init(EventLoop* el) -> void {
    tl_ = new ServiceTLState();
    tl_->init(el->io_context().get_executor(), kDefaultReadBufferSize);

    tl_->conn_pool().set_pool_size(ServiceTLState::kConnPoolSize);
    tl_->conn_pool().set_new([]() -> ConnectionPtr {
        auto* tl = RedisService::tlocal();
        CHECK(tl);
        
        if (auto slot = tl->get_register_buffer(); slot.has_value()) {
            return std::make_unique<Connection>(slot->buffer, [index = slot->index, tl]() {
                tl->free_buffer(index);
            });
        } else {
            // use normal buffer
            return std::make_unique<Connection>();
        }
    });
}

auto RedisService::ServiceTLState::init(asio::any_io_executor exector, size_t buf_size_per_conn) -> void {
    const size_t total = buf_size_per_conn * kConnPoolSize;

    buf_space_ = std::make_unique<byte[]>(total);
    std::vector<asio::mutable_buffer> bufs;

    for (int i = 0; i < kConnPoolSize;i++) {
        bufs.emplace_back(buf_space_.get() + i * buf_size_per_conn, buf_size_per_conn);
        free_list_.emplace_back(i);
    }

    buffer_registration_.emplace(exector, bufs);
}

auto RedisService::ServiceTLState::get_register_buffer() -> std::optional<Slot> {
    CHECK(buffer_registration_.has_value());

    if (free_list_.empty()) {
        return std::nullopt;
    }

    auto index = free_list_.back();
    free_list_.pop_back();
    return Slot{
        .buffer=buffer_registration_->at(index),
        .index=index
    };
}

auto RedisService::ServiceTLState::free_buffer(size_t i) -> void {
    free_list_.emplace_back(i);
}


auto RedisService::handle(asio::ip::tcp::socket socket) -> asio::awaitable<void> {
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

thread_local RedisService::ServiceTLState* RedisService::tl_ = nullptr;

} // namespace idlekv
