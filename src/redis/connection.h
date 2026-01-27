#pragma once

#include <asio/asio.hpp>
#include <asio/asio/awaitable.hpp>

namespace idlekv {

class Connection {
public:
    explicit Connection(asio::ip::tcp::socket&& socket) : socket_(std::move(socket)) {} 

    const asio::ip::tcp::socket& socket() const { return socket_; }
    asio::ip::tcp::socket& socket() { return socket_; }

private: 
    asio::ip::tcp::socket socket_;
};

} // namespace idlekv
