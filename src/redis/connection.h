#pragma once

#include "redis/protocol/parser.h"

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
#include <new>
#include <optional>
#include <string>
#include <system_error>
#include <unordered_set>

namespace idlekv {

constexpr size_t defaultBufferSize = 4096;

class Connection : public Reader {
public:
    explicit Connection(asio::ip::tcp::socket&& socket) : socket_(std::move(socket)) {
        // TODO(use jemalloc?)
        buffer_ = new char[defaultBufferSize];
    }

    // return a single line with '\n'
    virtual auto read_line() noexcept -> asio::awaitable<Payload> override;

    virtual auto read_bytes(size_t len) noexcept -> asio::awaitable<Payload> override;

    auto write(const std::string& reply) noexcept -> asio::awaitable<std::error_code> ;

    void close() {
        if (!closed_.exchange(true, std::memory_order_acq_rel)) {
            this->socket_.shutdown(asio::ip::tcp::socket::shutdown_both);
            this->socket_.close();
        }
    }

private:
    // fill reads a new chunk into the buffer.
    auto fill() noexcept -> asio::awaitable<std::error_code>;

    // claer buffer.
    void buffer_clear() { r_ = 0, w_ = 0; }

    asio::ip::tcp::socket socket_;

    char* buffer_;
    // buffer_ read and write positions
    size_t r_{0}, w_{0};
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
