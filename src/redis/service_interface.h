#pragma once

#include "common/asio_no_exceptions.h"

#include <asio/awaitable.hpp>
#include <string>
#include <vector>

namespace idlekv {

class Connection;

class ServiceInterface {
public:
    virtual auto exec(Connection*, std::vector<std::string>&) noexcept -> asio::awaitable<void> = 0;
    virtual ~ServiceInterface() = default;
};

} // namespace idlekv
