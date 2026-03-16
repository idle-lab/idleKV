#include "redis/connection.h"

#include "common/logger.h"
#include "common/result.h"
#include "db/engine.h"
#include "metric/request_stage.h"
#include "redis/error.h"
#include "redis/parser.h"

#include <asio/as_tuple.hpp>
#include <asio/awaitable.hpp>
#include <asio/read.hpp>
#include <asio/read_at.hpp>
#include <asio/read_until.hpp>
#include <asio/steady_timer.hpp>
#include <asio/use_awaitable.hpp>
#include <cstddef>
#include <deque>
#include <spdlog/spdlog.h>
#include <sys/uio.h>
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

auto format_remote_endpoint(const Connection& conn) -> std::string {
    const auto ep = conn.remote_endpoint();
    if (ep == asio::ip::tcp::endpoint{}) {
        return {};
    }

    return fmt::format("{}:{}", ep.address().to_string(), ep.port());
}

auto exec_result_name(const ExecResult& res) -> std::string_view {
    switch (res.type()) {
    case ExecResult::kPong:
        return "pong";
    case ExecResult::kOk:
        return "ok";
    case ExecResult::kSimpleString:
        return "simple_string";
    case ExecResult::kBulkString:
        return "bulk_string";
    case ExecResult::kNull:
        return "null";
    case ExecResult::kInteger:
        return "integer";
    case ExecResult::kError:
        return "error";
    }

    return "unknown";
}

} // namespace

auto Connection::read_impl(byte* buf, size_t size) noexcept -> asio::awaitable<ResultT<size_t>> {
    if (closed()) {
        co_return ResultT<size_t>(asio::error::not_connected);
    }
    auto [ec, n] = co_await socket_->async_read_some(asio::buffer(buf, size),
                                                     asio::as_tuple(asio::use_awaitable));
    if (ec) {
        ec_ = ec;
        if (!is_connection_closed_error(ec) && !is_transient_io_error(ec)) {
            LOG(warn, "read failed: {}", ec.message());
        }
    }
    co_return ResultT{ec, size_t(n)};
}

auto Connection::readv_impl(const std::vector<Buf>& bufs) noexcept
    -> asio::awaitable<ResultT<size_t>>  {
    if (closed()) {
        co_return ResultT<size_t>(asio::error::not_connected);
    }

    auto [ec, n] = co_await socket_->async_read_some(bufs,
                                                     asio::as_tuple(asio::use_awaitable));
    if (ec) {
        ec_ = ec;
        if (!is_connection_closed_error(ec) && !is_transient_io_error(ec)) {
            LOG(warn, "read failed: {}", ec.message());
        }
    }
    co_return ResultT{ec, size_t(n)};
}

auto Connection::write_impl(const byte* data, size_t size) noexcept
    -> asio::awaitable<ResultT<size_t>> {
    if (closed()) {
        co_return ResultT<size_t>(asio::error::not_connected);
    }
    auto [ec, n] = co_await asio::async_write(*socket_, asio::buffer(data, size),
                                              asio::as_tuple(asio::use_awaitable));
    if (ec) {
        ec_ = ec;
        if (!is_connection_closed_error(ec) && !is_transient_io_error(ec)) {
            LOG(warn, "write failed: {}", ec.message());
        }
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
        if (!is_connection_closed_error(ec) && !is_transient_io_error(ec)) {
            LOG(warn, "writev failed: {}", ec.message());
        }
    }
    co_return ResultT{ec, size_t(n)};
}

auto Connection::handle_requests() noexcept -> asio::awaitable<void> {
    for (;;) {
        auto parse_res = co_await p_.parse_one();

        if (!parse_res.ok()) {
            if (parse_res == ParserResut::STD_ERROR) {
                if (is_connection_closed_error(parse_res.error_code())) {
                    break;
                }

                if (is_transient_io_error(parse_res.error_code())) {
                    continue;
                }
            }

            // Parser failed. Try to send a single ERR reply then close the connection.
            auto reply_ec = co_await reply_parse_error(s_, parse_res);
            if (reply_ec && !is_connection_closed_error(reply_ec) &&
                !is_transient_io_error(reply_ec)) {
                LOG(warn, "failed to send parse error reply: {}", reply_ec.message());
            }
            break;
        }

        auto& args = parse_res.value();
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
        std::transform(args[0].begin(), args[0].end(), args[0].begin(),
                    [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        auto res = engine->dispatch_cmd(this, args);

        switch (res.type()) {
        case ExecResult::kPong:
            co_await sender().send_pong();
            break;
        case ExecResult::kOk:
            co_await sender().send_ok();
            break;
        case ExecResult::kSimpleString:
            co_await sender().send_simple_string(res.string());
            break;
        case ExecResult::kBulkString:
            if (res.data()) {
                co_await sender().send_bulk_string(res.data());
            } else {
                co_await sender().send_bulk_string(res.string());
            }
            break;
        case ExecResult::kNull:
            co_await sender().send_null_bulk_string();
            break;
        case ExecResult::kInteger:
            co_await sender().send_integer(res.integer());
            break;
        case ExecResult::kError:
            co_await sender().send_error(res.string());
            break;
        }

        if (parse_res != ParserResut::HAS_MORE) {
            co_await s_.flush();
        }
    }
}

auto Connection::handle_send() noexcept -> asio::awaitable<void> {
    const auto generation = send_generation_.load(std::memory_order_acquire);
    auto is_active = [this, generation]() {
        return generation == send_generation_.load(std::memory_order_acquire) && !closed();
    };

    while (is_active()) {
        while (sending_queue_.empty()) {
            co_await sq_cv_.async_wait();
            if (!is_active()) {
                co_return;
            }
        }

        while (!sending_queue_.empty()) {
            if (!is_active()) {
                co_return;
            }

            auto promise = std::move(sending_queue_.front());
            sending_queue_.pop();

            co_await promise->async_wait();
            if (!is_active()) {
                co_return;
            }
            if (promise->has_stage_tracking()) {
                RequestStageMetrics::instance().observe_queue_to_send(
                    PromiseResult::clock::now() - promise->send_ready_at());
            }

            auto& res = promise->result();
            switch (res.type()) {
            case ExecResult::kPong:
                co_await sender().send_pong();
                break;
            case ExecResult::kOk:
                co_await sender().send_ok();
                break;
            case ExecResult::kSimpleString:
                co_await sender().send_simple_string(res.string());
                break;
            case ExecResult::kBulkString:
                if (res.data()) {
                    co_await sender().send_bulk_string(res.data());
                } else {
                    co_await sender().send_bulk_string(res.string());
                }
                break;
            case ExecResult::kNull:
                co_await sender().send_null_bulk_string();
                break;
            case ExecResult::kInteger:
                co_await sender().send_integer(res.integer());
                break;
            case ExecResult::kError:
                co_await sender().send_error(res.string());
                break;
            }

            if (s_.get_error()) {
                co_return ;
            }
        }

        co_await s_.flush();

        if (!is_active() || s_.get_error()) {
            co_return;
        }
    }
}

auto Connection::enqueue_result(std::shared_ptr<PromiseResult> promise) -> void {
    sending_queue_.emplace(std::move(promise));
    sq_cv_.notify();
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
    clear_pending_results();
    p_.clear();
    s_.clear();
    ec_       = std::error_code{};
    db_index_ = 0;
}

auto Connection::reset() -> void {
    send_generation_.fetch_add(1, std::memory_order_acq_rel);
    clear_pending_results();
    sq_cv_.notify();

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
    p_.clear();
    s_.clear();
    ec_ = std::error_code{};
    db_index_ = 0;
}

} // namespace idlekv
