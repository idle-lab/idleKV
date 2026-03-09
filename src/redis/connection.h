#pragma once

#include "common/logger.h"
#include "redis/protocol/parser.h"
#include "redis/service_interface.h"
#include "server/thread_state.h"
#include "utils/condition_variable/condition_variable.h"

#include <asio/as_tuple.hpp>
#include <asio/asio.hpp>
#include <asio/asio/awaitable.hpp>
#include <asio/error.hpp>
#include <asio/registered_buffer.hpp>
#include <asio/use_awaitable.hpp>
#include <cassert>
#include <cstddef>
#include <cstring>
#include <future>
#include <optional>
#include <queue>
#include <system_error>
#include <vector>

namespace idlekv {

class Connection : public Reader, public Writer {
public:
    explicit Connection(ServiceInterface* service)
        : Reader(kDefaultReadBufferSize), Writer(256), p_(this),
          s_(this), service_(service) {}

    virtual auto read_impl(byte* buf, size_t size) noexcept
        -> asio::awaitable<ResultT<size_t>> override;

    virtual auto write_impl(const byte* data, size_t size) noexcept
        -> asio::awaitable<ResultT<size_t>> override;
    virtual auto writev_impl(const std::vector<BufView>& bufs) noexcept
        -> asio::awaitable<ResultT<size_t>> override;

    auto handle_requests() noexcept -> asio::awaitable<void>;

    auto async_handle(asio::steady_timer& done) -> asio::awaitable<void>;

    auto flush() -> asio::awaitable<void>;

    auto reset(asio::ip::tcp::socket&& socket) {
        CHECK(socket_.has_value() == false) << "override a connection that is currently in use";
        socket_.emplace(std::move(socket));
    }

    auto reset() {
        CHECK(pipeline_queue_.empty());
        if (socket_.has_value()) {
            socket_->close();
            socket_.reset();
        }
        async_handling_ = has_async_co_ = false;
        p_.clear();
        s_.clear();
    }

    auto remote_endpoint() const -> asio::ip::tcp::endpoint {
        return socket_->remote_endpoint(); 
    }

    auto closed() const -> bool {
        return !(socket_.has_value() && socket_->is_open());
    }

private:
    // fill reads a new chunk into the buffer.
    auto fill() noexcept -> asio::awaitable<std::error_code>;

    std::optional<asio::ip::tcp::socket> socket_;

    Parser p_;
    Sender s_;

    ServiceInterface* service_;

    bool async_handling_{false}, sync_handling_{false}, has_async_co_{false};
    std::queue<std::vector<std::string>> pipeline_queue_;
};

} // namespace idlekv
