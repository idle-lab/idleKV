#pragma once

#include "absl/functional/function_ref.h"
#include "common/asio_no_exceptions.h"
#include "db/result.h"
#include "db/storage/data_entity.h"
#include "redis/parser.h"
#include "server/el_pool.h"

#include <array>
#include <asio/any_io_executor.hpp>
#include <asio/as_tuple.hpp>
#include <asio/asio.hpp>
#include <asio/asio/awaitable.hpp>
#include <asio/buffer.hpp>
#include <asio/buffer_registration.hpp>
#include <asio/detail/is_buffer_sequence.hpp>
#include <asio/error.hpp>
#include <asio/io_context.hpp>
#include <asio/registered_buffer.hpp>
#include <asio/use_awaitable.hpp>
#include <cassert>
#include <atomic>
#include <cstddef>
#include <cstring>
#include <functional>
#include <optional>
#include <system_error>
#include <vector>

namespace idlekv {

class DB;
class IdleEngine;

class Connection : public Reader, public Writer {
public:
    explicit Connection()
        : Reader(kDefaultReadBufferSize), Writer(kDefaultWriteBufferSize), p_(this), s_(this) {}

    explicit Connection(asio::mutable_registered_buffer buf, absl::FunctionRef<void()> buffer_releaser)
        : Reader(buf), Writer(kDefaultWriteBufferSize), p_(this), s_(this), buffer_releaser_(buffer_releaser) {}

    ~Connection() { 
        if (buffer_releaser_.has_value()) {
            (*buffer_releaser_)();
        }
    }

    virtual auto ReadImpl(char* buf, size_t size) noexcept
        -> asio::awaitable<ResultT<size_t>> override;
    virtual auto ReadImpl(asio::mutable_registered_buffer reg_buf) noexcept
        -> asio::awaitable<ResultT<size_t>> override;
    virtual auto ReadvImpl(const std::vector<Buf>& bufs) noexcept
        -> asio::awaitable<ResultT<size_t>> override;

    virtual auto WriteImpl(const char* data, size_t size) noexcept
        -> asio::awaitable<ResultT<size_t>> override;
    virtual auto WritevImpl(const std::vector<BufView>& bufs) noexcept
        -> asio::awaitable<ResultT<size_t>> override;

    auto HandleRequests() noexcept -> asio::awaitable<void>;

    auto Flush() -> asio::awaitable<void>;

    auto Reset(asio::ip::tcp::socket&& socket) -> void;
    auto Reset() -> void;

    auto GetSender() -> Sender& { return s_; }

    auto DbIndex() const -> size_t { return db_index_; }
    auto GetSocket() -> asio::ip::tcp::socket& { return *socket_; }
    auto GetExecutor() -> const asio::any_io_executor& {
        CHECK(socket_.has_value());
        return socket_->get_executor();
    }
    void SetDbIndex(size_t DbIndex) { db_index_ = DbIndex; }

    auto RemoteEndpoint() const -> asio::ip::tcp::endpoint {
        if (!socket_.has_value()) {
            return {};
        }

        std::error_code ec;
        auto            ep = socket_->remote_endpoint(ec);
        return ec ? asio::ip::tcp::endpoint{} : ep;
    }

    auto IsClosed() const -> bool {
        return ec_ || !(socket_.has_value() && socket_->is_open()) || s_.GetError();
    }
private:

    std::optional<asio::ip::tcp::socket>           socket_;
    std::error_code                                ec_;

    Parser p_;
    Sender s_;

    std::optional<absl::FunctionRef<void()>> buffer_releaser_;

    EventLoop*        el_;
    size_t            db_index_ = 0;
};

} // namespace idlekv
