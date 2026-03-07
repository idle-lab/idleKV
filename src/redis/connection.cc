#include "redis/connection.h"

#include "common/result.h"

#include <asio/as_tuple.hpp>
#include <asio/use_awaitable.hpp>
#include <cstddef>

namespace idlekv {

auto Connection::read_impl(byte* buf, size_t size) noexcept -> asio::awaitable<ResultT<size_t>> {
    auto [ec, n] =  co_await socket_.async_read_some(asio::buffer(buf, size),
                                                asio::as_tuple(asio::use_awaitable));
    co_return ResultT{ec, size_t(n)};
}

auto Connection::write_impl(const byte* data, size_t size) noexcept -> asio::awaitable<ResultT<size_t>> {
    auto [ec, n] = co_await socket_.async_write_some(asio::buffer(data, size),
                                                 asio::as_tuple(asio::use_awaitable));
    co_return ResultT{ec, size_t(n)};
}

auto Connection::writev_impl(const std::vector<BufView>& bufs) noexcept -> asio::awaitable<ResultT<size_t>> {
    auto [ec, n] = co_await socket_.async_write_some(bufs,
                                                 asio::as_tuple(asio::use_awaitable));
    co_return ResultT{ec, size_t(n)};
}


} // namespace idlekv
