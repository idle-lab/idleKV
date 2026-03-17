#pragma once

#include "common/asio_no_exceptions.h"
#include "db/result.h"
#include "db/storage/data_entity.h"
#include "redis/parser.h"
#include "server/el_pool.h"
#include "utils/condition_variable/condition_variable.h"

#include <asio/any_io_executor.hpp>
#include <asio/as_tuple.hpp>
#include <asio/asio.hpp>
#include <asio/asio/awaitable.hpp>
#include <asio/error.hpp>
#include <asio/io_context.hpp>
#include <asio/registered_buffer.hpp>
#include <asio/use_awaitable.hpp>
#include <cassert>
#include <atomic>
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
    explicit Connection(const asio::any_io_executor& exector = asio::any_io_executor{})
        : Reader(kDefaultReadBufferSize), Writer(kDefaultWriteBufferSize), p_(this), s_(this), sq_cv_(exector) {}

    virtual auto read_impl(byte* buf, size_t size) noexcept
        -> asio::awaitable<ResultT<size_t>> override;
    virtual auto readv_impl(const std::vector<Buf>& bufs) noexcept
        -> asio::awaitable<ResultT<size_t>> override;

    virtual auto write_impl(const byte* data, size_t size) noexcept
        -> asio::awaitable<ResultT<size_t>> override;
    virtual auto writev_impl(const std::vector<BufView>& bufs) noexcept
        -> asio::awaitable<ResultT<size_t>> override;

    auto handle_requests() noexcept -> asio::awaitable<void>;

    auto flush() -> asio::awaitable<void>;

    auto reset(asio::ip::tcp::socket&& socket) -> void;
    auto reset() -> void;

    auto sender() -> Sender& { return s_; }

    auto db_index() const -> size_t { return db_index_; }
    auto socket() -> asio::ip::tcp::socket& { return *socket_; }
    auto get_executor() -> const asio::any_io_executor& {
        return socket_.has_value() ? socket_->get_executor() : sq_cv_.get_executor();
    }
    void set_db_index(size_t db_index) { db_index_ = db_index; }

    auto remote_endpoint() const -> asio::ip::tcp::endpoint {
        if (!socket_.has_value()) {
            return {};
        }

        std::error_code ec;
        auto            ep = socket_->remote_endpoint(ec);
        return ec ? asio::ip::tcp::endpoint{} : ep;
    }

    auto closed() const -> bool { return ec_ || !(socket_.has_value() && socket_->is_open()) || s_.get_error(); }
private:

    std::optional<asio::ip::tcp::socket>           socket_;
    std::error_code                                ec_;

    Parser p_;
    Sender s_;

    utils::ConditionVariable sq_cv_;
    std::atomic<uint64_t>    send_generation_{0};

    EventLoop*        el_;
    size_t            db_index_ = 0;
};

} // namespace idlekv
