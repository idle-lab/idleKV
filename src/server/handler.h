#pragma once

#include <asio/asio.hpp>
#include <common/logger.h>
#include <string>

namespace idlekv {

class Handler {
public:
    Handler(asio::ip::tcp::endpoint ep) : ep_(ep) {}

    Handler(const std::string& ip, uint16_t port) : ep_(asio::ip::make_address(ip), port) {}

    virtual asio::awaitable<void> start() = 0;

    virtual void stop() = 0;

    virtual asio::ip::tcp::endpoint endpoint() { return ep_; }

    virtual std::string name() { return "Unknow Handler"; }

    virtual ~Handler() = default;

protected:
    asio::ip::tcp::endpoint ep_;
};

} // namespace idlekv
