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
#include <asio/registered_buffer.hpp>
#include <asio/steady_timer.hpp>
#include <asio/use_awaitable.hpp>
#include <cstddef>
#include <spdlog/spdlog.h>
#include <sys/uio.h>
#include <system_error>
#include <vector>

namespace idlekv {

namespace {

auto IsConnectionClosedError(const std::error_code& ec) -> bool {
    return ec == asio::error::eof || ec == asio::error::connection_reset ||
           ec == asio::error::connection_aborted || ec == asio::error::not_connected ||
           ec == asio::error::broken_pipe || ec == asio::error::bad_descriptor ||
           ec == asio::error::operation_aborted;
}

auto IsTransientIoError(const std::error_code& ec) -> bool {
    return ec == asio::error::try_again || ec == asio::error::would_block ||
           ec == asio::error::timed_out || ec == asio::error::interrupted;
}

auto IsProtocolFormatError(const std::error_code& ec) -> bool {
    return ec == std::errc::invalid_argument || ec == std::errc::result_out_of_range;
}

auto MakeParseErrorReply(const ParserResut& res) -> std::string {
    if (res == ParserResut::WRONG_TYPE_ERROR || res == ParserResut::PROTOCOL_ERROR) {
        return res.Message().empty() ? fmt::format(kProtocolErrFmt, "invalid request")
                                     : res.Message();
    }

    if (res == ParserResut::STD_ERROR) {
        if (IsProtocolFormatError(res.ErrorCode())) {
            return fmt::format(kProtocolErrFmt, res.ErrorCode().message());
        }
        return fmt::format(kStandardErr, res.ErrorCode().message());
    }

    return fmt::format(kUnknownCmdErrFmt, "unknown command");
}

auto ReplyParseError(Sender& sender, const ParserResut& res) -> asio::awaitable<std::error_code> {
    auto reply = MakeParseErrorReply(res);
    if (reply.empty()) {
        co_return std::error_code{};
    }
    co_await sender.SendError(reply);
    if (sender.GetError()) {
        co_return sender.GetError();
    }

    co_await sender.Flush();
    co_return sender.GetError();
}

auto FormatRemoteEndpoint(const Connection& conn) -> std::string {
    const auto ep = conn.RemoteEndpoint();
    if (ep == asio::ip::tcp::endpoint{}) {
        return {};
    }

    return fmt::format("{}:{}", ep.address().to_string(), ep.port());
}

auto ExecResultName(const ExecResult& res) -> std::string_view {
    switch (res.GetType()) {
    case ExecResult::kPong:
        return "pong";
    case ExecResult::kOk:
        return "ok";
    case ExecResult::kSimpleString:
        return "SimpleString";
    case ExecResult::kBulkString:
        return "BulkString";
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

auto Connection::ReadImpl(char* buf, size_t size) noexcept -> asio::awaitable<ResultT<size_t>> {
    if (IsClosed()) {
        co_return ResultT<size_t>(asio::error::not_connected);
    }
    auto [ec, n] = co_await socket_->async_read_some(asio::buffer(buf, size),
                                                     asio::as_tuple(asio::use_awaitable));
    if (ec) {
        ec_ = ec;
        if (!IsConnectionClosedError(ec) && !IsTransientIoError(ec)) {
            LOG(warn, "read failed: {}", ec.message());
        }
    }
    co_return ResultT{ec, size_t(n)};
}

auto Connection::ReadImpl(asio::mutable_registered_buffer reg_buf) noexcept -> asio::awaitable<ResultT<size_t>> {
    if (IsClosed()) {
        co_return ResultT<size_t>(asio::error::not_connected);
    }
    auto [ec, n] = co_await socket_->async_read_some(reg_buf,
                                                     asio::as_tuple(asio::use_awaitable));
    if (ec) {
        ec_ = ec;
        if (!IsConnectionClosedError(ec) && !IsTransientIoError(ec)) {
            LOG(warn, "read failed: {}", ec.message());
        }
    }
    co_return ResultT{ec, size_t(n)};
}

auto Connection::ReadvImpl(const std::vector<Buf>& bufs) noexcept
    -> asio::awaitable<ResultT<size_t>>  {
    if (IsClosed()) {
        co_return ResultT<size_t>(asio::error::not_connected);
    }

    auto [ec, n] = co_await socket_->async_read_some(bufs,
                                                     asio::as_tuple(asio::use_awaitable));
    if (ec) {
        ec_ = ec;
        if (!IsConnectionClosedError(ec) && !IsTransientIoError(ec)) {
            LOG(warn, "read failed: {}", ec.message());
        }
    }
    co_return ResultT{ec, size_t(n)};
}

auto Connection::WriteImpl(const char* data, size_t size) noexcept
    -> asio::awaitable<ResultT<size_t>> {
    if (IsClosed()) {
        co_return ResultT<size_t>(asio::error::not_connected);
    }
    auto [ec, n] = co_await asio::async_write(*socket_, asio::buffer(data, size),
                                              asio::as_tuple(asio::use_awaitable));
    if (ec) {
        ec_ = ec;
        if (!IsConnectionClosedError(ec) && !IsTransientIoError(ec)) {
            LOG(warn, "write failed: {}", ec.message());
        }
    }
    co_return ResultT{ec, size_t(n)};
}

auto Connection::WritevImpl(const std::vector<BufView>& bufs) noexcept
    -> asio::awaitable<ResultT<size_t>> {
    if (IsClosed()) {
        co_return ResultT<size_t>(asio::error::not_connected);
    }

    auto [ec, n] = co_await asio::async_write(*socket_, bufs, asio::as_tuple(asio::use_awaitable));
    if (ec) {
        ec_ = ec;
        if (!IsConnectionClosedError(ec) && !IsTransientIoError(ec)) {
            LOG(warn, "writev failed: {}", ec.message());
        }
    }
    co_return ResultT{ec, size_t(n)};
}

auto Connection::HandleRequests() noexcept -> asio::awaitable<void> {
    std::vector<std::string> args;
    for (;;) {
        auto parse_res = co_await p_.ParseOne(args);

        if (!parse_res.Ok()) {
            if (parse_res == ParserResut::STD_ERROR) {
                if (IsConnectionClosedError(parse_res.ErrorCode())) {
                    break;
                }

                if (IsTransientIoError(parse_res.ErrorCode())) {
                    continue;
                }
            }

            // Parser failed. Try to send a single ERR reply then close the connection.
            auto reply_ec = co_await ReplyParseError(s_, parse_res);
            if (reply_ec && !IsConnectionClosedError(reply_ec) &&
                !IsTransientIoError(reply_ec)) {
                LOG(warn, "failed to send parse error reply: {}", reply_ec.message());
            }
            break;
        }

        auto& args = parse_res.Value();
        if (args.empty()) [[unlikely]] {
            auto parse_err = ParserResut(ParserResut::PROTOCOL_ERROR,
                                         fmt::format(kProtocolErrFmt, "empty command"));

            auto reply_ec = co_await ReplyParseError(s_, parse_err);
            if (reply_ec && !IsConnectionClosedError(reply_ec) &&
                !IsTransientIoError(reply_ec)) {
                LOG(warn, "failed to send protocol error reply: {}", reply_ec.message());
            }
            break;
        }
        std::transform(args[0].begin(), args[0].end(), args[0].begin(),
                    [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        auto res = engine->DispatchCmd(this, args);

        switch (res.GetType()) {
        case ExecResult::kPong:
            co_await GetSender().SendPong();
            break;
        case ExecResult::kOk:
            co_await GetSender().SendOk();
            break;
        case ExecResult::kSimpleString:
            co_await GetSender().SendSimpleString(res.GetString());
            break;
        case ExecResult::kBulkString:
            if (res.GetData()) {
                co_await GetSender().SendBulkString(res.GetData());
            } else {
                co_await GetSender().SendBulkString(res.GetString());
            }
            break;
        case ExecResult::kNull:
            co_await GetSender().SendNullBulkString();
            break;
        case ExecResult::kInteger:
            co_await GetSender().SendInteger(res.GetInteger());
            break;
        case ExecResult::kError:
            co_await GetSender().SendError(res.GetString());
            break;
        }

        if (parse_res != ParserResut::HAS_MORE) {
            co_await s_.Flush();
        }
    }
}

auto Connection::Flush() -> asio::awaitable<void> {
    if (s_.GetError() || IsClosed()) {
        co_return;
    }
    co_await s_.Flush();
}

auto Connection::Reset(asio::ip::tcp::socket&& socket) -> void {
    CHECK(socket_.has_value() == false) << "override a connection that is currently in use";
    socket_.emplace(std::move(socket));
    p_.Clear();
    s_.Clear();
    ec_       = std::error_code{};
    db_index_ = 0;
}

auto Connection::Reset() -> void {
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
    p_.Clear();
    s_.Clear();
    ec_ = std::error_code{};
    db_index_ = 0;
}

} // namespace idlekv
