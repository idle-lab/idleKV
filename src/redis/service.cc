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


auto RedisService::Init(EventLoop* el) -> void {
    tl_ = new ServiceTLState();
    tl_->Init(el->IoContext().get_executor(), kDefaultReadBufferSize);

    tl_->ConnPool().SetPoolSize(ServiceTLState::kConnPoolSize);
    tl_->ConnPool().SetNew([]() -> ConnectionPtr {
        auto* tl = RedisService::Tlocal();
        CHECK(tl);
        
        if (auto slot = tl->GetRegisterBuffer(); slot.has_value()) {
            return std::make_unique<Connection>(slot->buffer, [index = slot->index, tl]() {
                tl->FreeBuffer(index);
            });
        } else {
            // use normal buffer
            return std::make_unique<Connection>();
        }
    });
}

auto RedisService::ServiceTLState::Init(asio::any_io_executor exector, size_t buf_size_per_conn) -> void {
    const size_t total = buf_size_per_conn * kConnPoolSize;

    buf_space_ = std::make_unique<byte[]>(total);
    std::vector<asio::mutable_buffer> bufs;

    for (int i = 0; i < kConnPoolSize;i++) {
        bufs.emplace_back(buf_space_.get() + i * buf_size_per_conn, buf_size_per_conn);
        free_list_.emplace_back(i);
    }

    buffer_registration_.emplace(exector, bufs);
}

auto RedisService::ServiceTLState::GetRegisterBuffer() -> std::optional<Slot> {
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

auto RedisService::ServiceTLState::FreeBuffer(size_t i) -> void {
    free_list_.emplace_back(i);
}


auto RedisService::Handle(asio::ip::tcp::socket socket) -> asio::awaitable<void> {
    auto& ConnList = Tlocal()->ConnList();
    auto& ConnPool = Tlocal()->ConnPool();

    // get a connection from the pool and put it to the front of the list.
    auto conn = ConnPool.Get();
    conn->Reset(std::move(socket));
    ConnList.emplace_front(conn.get());
    auto it = ConnList.begin();

    co_await conn->HandleRequests();

    ConnList.erase(it);

    conn->Reset();
    ConnPool.Put(std::move(conn));
}

thread_local RedisService::ServiceTLState* RedisService::tl_ = nullptr;

} // namespace idlekv
