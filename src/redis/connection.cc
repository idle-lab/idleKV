#include "redis/connection.h"

#include "common/logger.h"
#include "common/result.h"

#include <algorithm>
#include <asio/as_tuple.hpp>
#include <asio/use_awaitable.hpp>
#include <cstddef>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

namespace idlekv {

auto Connection::read_impl(byte* buf, size_t size) noexcept -> asio::awaitable<ResultT<size_t>> {
    co_return co_await socket_->async_read_some(asio::buffer(buf, size),
                                                asio::as_tuple(asio::use_awaitable));
}

auto Connection::write_impl(const byte* data, size_t size) noexcept -> asio::awaitable<ResultT<size_t>> {
    co_return co_await socket_->async_write_some(asio::buffer(data, size),
                                                 asio::as_tuple(asio::use_awaitable));
}

} // namespace idlekv
