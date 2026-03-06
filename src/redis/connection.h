#pragma once

#include "db/db.h"
#include "redis/protocol/parser.h"

#include <array>
#include <asio/as_tuple.hpp>
#include <asio/asio.hpp>
#include <asio/asio/awaitable.hpp>
#include <asio/error.hpp>
#include <asio/registered_buffer.hpp>
#include <asio/use_awaitable.hpp>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <unordered_set>
#include <utility>

namespace idlekv {

class Connection : public Reader, public Writer {
public:
    Connection() = default;

    explicit Connection(asio::ip::tcp::socket&& socket)
        : Reader(kDefaultReadBufferSize), Writer(kDefaultWriteBufferSize), socket_(std::move(socket)) {}

    virtual auto read_impl(byte* buf, size_t size) noexcept  -> asio::awaitable<ResultT<size_t>> override;

    virtual auto write_impl(const byte* data, size_t size) noexcept -> asio::awaitable<ResultT<size_t>> override;

    auto remote_endpoint() const -> asio::ip::tcp::endpoint { return socket_->remote_endpoint(); }

    auto closed() const -> bool { return closed_.load(std::memory_order_acquire); }

    void close() {
        if (!closed_.exchange(true, std::memory_order_acq_rel)) {
            this->socket_->shutdown(asio::ip::tcp::socket::shutdown_both);
            this->socket_->close();
        }
    }

private:
    // fill reads a new chunk into the buffer.
    auto fill() noexcept -> asio::awaitable<std::error_code>;

    std::optional<asio::ip::tcp::socket> socket_;

    // error occurred when the connection was disconnected
    std::optional<std::error_code> ec_;

    std::atomic<bool> closed_{false};
};

// TODO
class ConnectionManager {
public:
    void add(std::shared_ptr<Connection> c);
    void remove(std::shared_ptr<Connection> c);

    void   shutdown_all(); // 服务退出
    size_t size() const;

private:
    std::mutex                                      mu_;
    std::unordered_set<std::shared_ptr<Connection>> conns_;
};

} // namespace idlekv
