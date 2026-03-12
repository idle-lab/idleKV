#include "redis/connection.h"

#include "common/logger.h"
#include "common/result.h"
#include "metric/avg.h"
#include "redis/error.h"
#include "redis/parser.h"

#include <asio/as_tuple.hpp>
#include <asio/awaitable.hpp>
#include <asio/steady_timer.hpp>
#include <asio/use_awaitable.hpp>
#include <chrono>
#include <cstddef>
#include <spdlog/spdlog.h>
#include <system_error>

namespace idlekv {

namespace {

auto is_connection_closed_error(const std::error_code& ec) -> bool {
    return ec == asio::error::eof || ec == asio::error::connection_reset ||
           ec == asio::error::connection_aborted || ec == asio::error::not_connected ||
           ec == asio::error::broken_pipe || ec == asio::error::bad_descriptor ||
           ec == asio::error::operation_aborted;
}

auto is_transient_io_error(const std::error_code& ec) -> bool {
    return ec == asio::error::try_again || ec == asio::error::would_block ||
           ec == asio::error::timed_out || ec == asio::error::interrupted;
}

auto is_protocol_format_error(const std::error_code& ec) -> bool {
    return ec == std::errc::invalid_argument || ec == std::errc::result_out_of_range;
}

auto make_parse_error_reply(const ParserResut& res) -> std::string {
    if (res == ParserResut::WRONG_TYPE_ERROR || res == ParserResut::PROTOCOL_ERROR) {
        return res.message().empty() ? fmt::format(kProtocolErrFmt, "invalid request")
                                     : res.message();
    }

    if (res == ParserResut::STD_ERROR) {
        if (is_protocol_format_error(res.error_code())) {
            return fmt::format(kProtocolErrFmt, res.error_code().message());
        }
        return fmt::format(kStandardErr, res.error_code().message());
    }

    return fmt::format(kUnknownCmdErrFmt, "unknown command");
}

auto reply_parse_error(Sender& sender, const ParserResut& res) -> asio::awaitable<std::error_code> {
    auto reply = make_parse_error_reply(res);
    if (reply.empty()) {
        co_return std::error_code{};
    }
    co_await sender.send_error(reply);
    if (sender.get_error()) {
        co_return sender.get_error();
    }

    co_await sender.flush();
    co_return sender.get_error();
}

} // namespace

Avg read_avg("read", std::chrono::seconds(2));
Avg write_avg("write", std::chrono::seconds(2));

auto Connection::read_impl(byte* buf, size_t size) noexcept -> asio::awaitable<ResultT<size_t>> {
    if (closed()) {
        co_return ResultT<size_t>(asio::error::not_connected);
    }
    auto [ec, n] = co_await socket_->async_read_some(asio::buffer(buf, size),
                                                     asio::as_tuple(asio::use_awaitable));
    if (ec) {
        ec_ = ec;
    }
    co_return ResultT{ec, size_t(n)};
}

auto Connection::write_impl(const byte* data, size_t size) noexcept
    -> asio::awaitable<ResultT<size_t>> {
    if (closed()) {
        co_return ResultT<size_t>(asio::error::not_connected);
    }
    auto [ec, n] =
        co_await asio::async_write(*socket_, asio::buffer(data, size),
                                   asio::as_tuple(asio::use_awaitable));
    if (ec) {
        ec_ = ec;
    }
    co_return ResultT{ec, size_t(n)};
}

auto Connection::writev_impl(const std::vector<BufView>& bufs) noexcept
    -> asio::awaitable<ResultT<size_t>> {
    if (closed()) {
        co_return ResultT<size_t>(asio::error::not_connected);
    }

    auto [ec, n] = co_await asio::async_write(*socket_, bufs, asio::as_tuple(asio::use_awaitable));
    if (ec) {
        ec_ = ec;
    }
    co_return ResultT{ec, size_t(n)};
}

auto Connection::handle_requests() noexcept -> asio::awaitable<void> {
    for (;;) {
        auto res = co_await p_.parse_one();
        if (!res.ok()) {
            if (res == ParserResut::STD_ERROR) {
                if (is_connection_closed_error(res.error_code())) {
                    break;
                }

                if (is_transient_io_error(res.error_code())) {
                    continue;
                }
            }

            // Parser failed. Try to send a single ERR reply then close the connection.
            auto reply_ec = co_await reply_parse_error(s_, res);
            if (reply_ec && !is_connection_closed_error(reply_ec) &&
                !is_transient_io_error(reply_ec)) {
                LOG(warn, "failed to send parse error reply: {}", reply_ec.message());
            }
            break;
        }

        auto& args = res.value();
        if (args.empty()) [[unlikely]] {
            auto parse_err = ParserResut(ParserResut::PROTOCOL_ERROR,
                                         fmt::format(kProtocolErrFmt, "empty command"));

            auto reply_ec = co_await reply_parse_error(s_, parse_err);
            if (reply_ec && !is_connection_closed_error(reply_ec) &&
                !is_transient_io_error(reply_ec)) {
                LOG(warn, "failed to send protocol error reply: {}", reply_ec.message());
            }
            break;
        }

        co_await service_->exec(this, args);

        if (s_.get_error()) {
            break;
        }

        if (res != ParserResut::HAS_MORE) {
            co_await s_.flush();
            if (s_.get_error()) {
                break;
            }
        }
    }
}

auto Connection::flush() -> asio::awaitable<void> {
    if (s_.get_error() || closed()) {
        co_return;
    }
    co_await s_.flush();
}

auto Connection::reset(asio::ip::tcp::socket&& socket) -> void {
    CHECK(socket_.has_value() == false) << "override a connection that is currently in use";
    socket_.emplace(std::move(socket));
    p_.clear();
    s_.clear();
    ec_ = std::error_code{};
}

auto Connection::reset() -> void {
    if (socket_.has_value()) {
        if (socket_->is_open()) {
            std::error_code ignored_ec;
            DISCARD_RESULT(socket_->close(ignored_ec));

            if (ignored_ec) {
                LOG(warn, "close socket failed: {}", ignored_ec.message());
            }
        }
        socket_.reset();
    }
}

} // namespace idlekv
