#pragma once

#include <asio/as_tuple.hpp>
#include <asio/asio.hpp>
#include <asio/asio/awaitable.hpp>
#include <asio/error.hpp>
#include <asio/registered_buffer.hpp>
#include <asio/use_awaitable.hpp>
#include <cassert>
#include <cstring>

namespace idlekv {

class Connection;

class ServiceInterface {
public: 
    virtual auto exec(Connection*, const std::vector<std::string>&) noexcept -> std::string = 0;
};



} // namespace idlekv
