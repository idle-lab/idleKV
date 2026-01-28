#pragma once

#include <asio/asio.hpp>
#include <asio/asio/awaitable.hpp>

namespace idlekv {

class Connection {
public:
    explicit Connection(asio::ip::tcp::socket&& socket) : socket_(std::move(socket)) {} 

    asio::ip::tcp::socket& socket() const { return socket_; }

private: 
    mutable asio::ip::tcp::socket socket_;
};

} // namespace idlekv
