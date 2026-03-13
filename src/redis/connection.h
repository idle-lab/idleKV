#pragma once

#include "common/asio_no_exceptions.h"
#include "common/config.h"
#include "common/logger.h"
#include "redis/parser.h"
#include "redis/service_interface.h"
#include "server/el_pool.h"

#include <asio/as_tuple.hpp>
#include <asio/asio.hpp>
#include <asio/asio/awaitable.hpp>
#include <asio/error.hpp>
#include <asio/registered_buffer.hpp>
#include <asio/use_awaitable.hpp>
#include <cassert>
#include <cstddef>
#include <cstring>
#include <optional>
#include <system_error>
#include <vector>

namespace idlekv {

class DB;
class IdleEngine;

class Connection : public Reader, public Writer {
public:
    explicit Connection(ServiceInterface* service)
        : Reader(kDefaultReadBufferSize), Writer(256), p_(this), s_(this), service_(service) {}

    virtual auto read_impl(byte* buf, size_t size) noexcept
        -> asio::awaitable<ResultT<size_t>> override;

    virtual auto write_impl(const byte* data, size_t size) noexcept
        -> asio::awaitable<ResultT<size_t>> override;
    virtual auto writev_impl(const std::vector<BufView>& bufs) noexcept
        -> asio::awaitable<ResultT<size_t>> override;

    auto handle_requests() noexcept -> asio::awaitable<void>;

    auto async_handle(asio::steady_timer& done) -> asio::awaitable<void>;

    auto flush() -> asio::awaitable<void>;

    auto reset(asio::ip::tcp::socket&& socket) -> void;
    auto reset() -> void;

    auto sender() -> Sender& { return s_; }

    auto db_index() const -> size_t { return db_index_; }

    void set_db_index(size_t db_index) { db_index_ = db_index; }

    auto remote_endpoint() const -> asio::ip::tcp::endpoint {
        if (!socket_.has_value()) {
            return {};
        }

        std::error_code ec;
        auto            ep = socket_->remote_endpoint(ec);
        return ec ? asio::ip::tcp::endpoint{} : ep;
    }

    auto closed() const -> bool { return ec_ || !(socket_.has_value() && socket_->is_open()); }

private:
    // fill reads a new chunk into the buffer.
    auto fill() noexcept -> asio::awaitable<std::error_code>;

    std::optional<asio::ip::tcp::socket> socket_;
    std::error_code                      ec_;

    Parser p_;
    Sender s_;

    ServiceInterface* service_;
    EventLoop* el_;
    size_t            db_index_ = 0;
};

} // namespace idlekv
