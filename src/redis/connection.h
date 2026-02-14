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

    auto socket() const -> asio::ip::tcp::socket& { return socket_; }

    // return a single line with '\n'
    auto read_line() -> asio::awaitable<std::string>;

    auto read_bytes(size_t len) -> asio::awaitable<std::string>;

    void close() {
        if (!closed_.exchange(true, std::memory_order_acq_rel)) {
            this->socket_.shutdown(asio::ip::tcp::socket::shutdown_both);
            this->socket_.close();
        }
    }

private:
    // fill reads a new chunk into the buffer.
    auto fill() -> asio::awaitable<void>;

    // claer buffer.
    void buffer_clear() { r_ = 0, w_ = 0; }

    mutable asio::ip::tcp::socket socket_;

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
