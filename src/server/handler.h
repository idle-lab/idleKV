#pragma once

#include "server/el_pool.h"

#include <boost/asio/ip/address.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <cstdio>
#include <cstdlib>
#include <string>

namespace idlekv {

namespace detail {

inline auto MakeTcpEndpoint(const std::string& ip, uint16_t port) -> asio::ip::tcp::endpoint {
    boost::system::error_code ec;
    auto                      addr = asio::ip::make_address(ip, ec);
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

    Handler(const std::string& ip, uint16_t port) : ep_(detail::MakeTcpEndpoint(ip, port)) {}

    virtual auto Init(EventLoop* el) -> void = 0;

    virtual auto Handle(asio::ip::tcp::socket socket) -> void = 0;

    virtual void Stop() = 0;

    virtual auto Endpoint() -> asio::ip::tcp::endpoint { return ep_; }

    virtual std::string Name() { return "Unknow Handler"; }

    virtual ~Handler() = default;

protected:
    asio::ip::tcp::endpoint ep_;
};

} // namespace idlekv
