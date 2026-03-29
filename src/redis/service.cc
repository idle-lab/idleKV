#include "redis/service.h"

#include "common/logger.h"
#include "redis/connection.h"
#include "redis/parser.h"

#include <boost/asio/buffer.hpp>
#include <boost/asio/buffer_registration.hpp>
#include <cstddef>
#include <memory>
#include <optional>
#include <spdlog/fmt/fmt.h>
#include <vector>

namespace idlekv {

auto RedisService::Init(EventLoop* el) -> void {
    tl_ = new ServiceTLData();
    tl_->Init(el->IoContext().get_executor(), kDefaultReadBufferSize);
}

auto RedisService::ServiceTLData::Init(asio::any_io_executor exector, size_t buf_size_per_conn)
    -> void {
    const size_t total = buf_size_per_conn * kConnPoolSize;

    buf_space_ = std::make_unique<char[]>(total);
    std::vector<asio::mutable_buffer> bufs;

    for (int i = 0; i < kConnPoolSize; i++) {
        bufs.emplace_back(buf_space_.get() + i * buf_size_per_conn, buf_size_per_conn);
        free_list_.emplace_back(i);
    }

    buffer_registration_.emplace(exector, bufs);

    // TODO(cyb): backpressure
    // no limit
    args_pool_.SetPoolSize(0);
    args_pool_.SetNew([]() -> CmdArgsPtr {
        return std::make_unique<CmdArgs>();
    });

    conn_pool_.SetPoolSize(ServiceTLData::kConnPoolSize);
    conn_pool_.SetNew([]() -> ConnectionPtr {
        auto* tl = RedisService::Tlocal();
        CHECK(tl);

        if (auto slot = tl->GetRegisterBuffer(); slot.has_value()) {
            return std::make_unique<Connection>(
                slot->buffer, [index = slot->index, tl]() { tl->FreeBuffer(index); });
        } else {
            // use normal buffer
            return std::make_unique<Connection>();
        }
    });
}

auto RedisService::ServiceTLData::GetRegisterBuffer() -> std::optional<Slot> {
    CHECK(buffer_registration_.has_value());

    if (free_list_.empty()) {
        return std::nullopt;
    }

    auto index = free_list_.back();
    free_list_.pop_back();
    return Slot{.buffer = buffer_registration_->at(index), .index = index};
}

auto RedisService::ServiceTLData::FreeBuffer(size_t i) -> void { free_list_.emplace_back(i); }

auto RedisService::ServiceTLData::GetCmdArgsOrCreate() -> CmdArgsPtr {
    auto ptr = args_pool_.Get();

    ptr->offsets_.clear();
    if (ptr->HeapMemory() > 1024) {
        ptr->storage_.clear();
    }

    return ptr;
}

auto RedisService::ServiceTLData::RecycleCmdArgs(CmdArgsPtr ptr) -> void {
    args_pool_.Put(std::move(ptr));
}

auto RedisService::Handle(asio::ip::tcp::socket socket) -> void {
    auto& ConnList = Tlocal()->ConnList();
    auto& ConnPool = Tlocal()->ConnPool();

    // get a connection from the pool and put it to the front of the list.
    auto conn = ConnPool.Get();
    conn->Reset(std::move(socket));
    ConnList.emplace_front(conn.get());
    auto it = ConnList.begin();

    conn->HandleRequests();

    ConnList.erase(it);

    conn->Reset();
    ConnPool.Put(std::move(conn));
}

thread_local RedisService::ServiceTLData* RedisService::tl_ = nullptr;

} // namespace idlekv
