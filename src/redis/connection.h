#pragma once

#include <asio/asio.hpp>
#include <asio/asio/awaitable.hpp>
#include <atomic>
#include <memory>
#include <unordered_set>

namespace idlekv {

class Connection {
public:
    explicit Connection(asio::ip::tcp::socket&& socket) : socket_(std::move(socket)) {}

    asio::ip::tcp::socket& socket() const { return socket_; }

    void close() {
        if (!closed_.exchange(true, std::memory_order_acq_rel)) {
            this->socket_.shutdown(asio::ip::tcp::socket::shutdown_both);
            this->socket_.close();
        }
    }

private:
    mutable asio::ip::tcp::socket socket_;
    std::atomic<bool>             closed_{false};
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
