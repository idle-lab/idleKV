#pragma once

#include "common/asio_no_exceptions.h"
#include "server/el_pool.h"

#include <asio/asio.hpp>
#include <asio/awaitable.hpp>
#include <cstdio>
#include <cstdlib>
#include <string>

namespace idlekv {

namespace detail {

inline auto make_tcp_endpoint(const std::string& ip, uint16_t port) -> asio::ip::tcp::endpoint {
    std::error_code ec;
    auto            addr = asio::ip::make_address(ip, ec);
    if (ec) {
        std::fprintf(stderr, "invalid listen address '%s': %s\n", ip.c_str(), ec.message().c_str());
        std::abort();
    }

    return asio::ip::tcp::endpoint(addr, port);
}

} // namespace detail

class Handler {
public:
    Handler(asio::ip::tcp::endpoint ep) : ep_(ep) {}

    Handler(const std::string& ip, uint16_t port) : ep_(detail::make_tcp_endpoint(ip, port)) {}

    virtual auto init(EventLoop* el) -> void = 0;

    virtual auto handle(asio::ip::tcp::socket socket) -> asio::awaitable<void> = 0;

    virtual void stop() = 0;

    virtual auto endpoint() -> asio::ip::tcp::endpoint { return ep_; }

    virtual std::string name() { return "Unknow Handler"; }

    virtual ~Handler() = default;

protected:
    asio::ip::tcp::endpoint ep_;
};

} // namespace idlekv
