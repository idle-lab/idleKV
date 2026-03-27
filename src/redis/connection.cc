#include "redis/connection.h"

#include "common/logger.h"
#include "common/result.h"
#include "db/engine.h"
#include "metric/request_stage.h"
#include "redis/error.h"
#include "redis/parser.h"
#include "server/fiber_runtime.h"

#include <boost/asio/read.hpp>
#include <boost/asio/registered_buffer.hpp>
#include <cstddef>
#include <spdlog/spdlog.h>
#include <sys/uio.h>
#include <system_error>
#include <vector>

namespace idlekv {

namespace {

template <typename ErrorEnum>
auto ToStdErrorCode(ErrorEnum ec) -> std::error_code {
    return boost::system::error_code(ec);
}

auto IsConnectionClosedError(const std::error_code& ec) -> bool {
    return ec == ToStdErrorCode(asio::error::eof) ||
           ec == ToStdErrorCode(asio::error::connection_reset) ||
           ec == ToStdErrorCode(asio::error::connection_aborted) ||
           ec == ToStdErrorCode(asio::error::not_connected) ||
           ec == ToStdErrorCode(asio::error::broken_pipe) ||
           ec == ToStdErrorCode(asio::error::bad_descriptor) ||
           ec == ToStdErrorCode(asio::error::operation_aborted);
}

auto IsTransientIoError(const std::error_code& ec) -> bool {
    return ec == ToStdErrorCode(asio::error::try_again) ||
           ec == ToStdErrorCode(asio::error::would_block) ||
           ec == ToStdErrorCode(asio::error::timed_out) ||
           ec == ToStdErrorCode(asio::error::interrupted);
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

auto ReplyParseError(Sender& sender, const ParserResut& res) -> std::error_code {
    auto reply = MakeParseErrorReply(res);
    if (reply.empty()) {
        return std::error_code{};
    }
    sender.SendError(reply);
    if (sender.GetError()) {
        return sender.GetError();
    }

    sender.Flush();
    return sender.GetError();
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

auto Connection::ReadImpl(char* buf, size_t size) noexcept -> ResultT<size_t> {
    if (IsClosed()) {
        return ResultT<size_t>(ToStdErrorCode(asio::error::not_connected));
    }

    auto n = socket_->async_read_some(asio::buffer(buf, size), boost::fibers::asio::yield[ec_]);
    if (ec_) {
        if (!IsConnectionClosedError(ec_) && !IsTransientIoError(ec_)) {
            LOG(warn, "read failed: {}", ec_.message());
        }
    }
    return ResultT{ec_, size_t(n)};
}

auto Connection::ReadImpl(asio::mutable_registered_buffer reg_buf) noexcept -> ResultT<size_t> {
    if (IsClosed()) {
        return ResultT<size_t>(ToStdErrorCode(asio::error::not_connected));
    }
    auto n = socket_->async_read_some(reg_buf, boost::fibers::asio::yield[ec_]);
    if (ec_) {
        if (!IsConnectionClosedError(ec_) && !IsTransientIoError(ec_)) {
            LOG(warn, "read register failed: {}", ec_.message());
        }
    }
    return ResultT{ec_, size_t(n)};
}

auto Connection::ReadvImpl(const std::vector<Buf>& bufs) noexcept -> ResultT<size_t> {
    if (IsClosed()) {
        return ResultT<size_t>(ToStdErrorCode(asio::error::not_connected));
    }

    auto n = socket_->async_read_some(bufs, boost::fibers::asio::yield[ec_]);
    if (ec_) {
        if (!IsConnectionClosedError(ec_) && !IsTransientIoError(ec_)) {
            LOG(warn, "read vector failed: {}", ec_.message());
        }
    }
    return ResultT{ec_, size_t(n)};
}

auto Connection::WriteImpl(const char* data, size_t size) noexcept -> ResultT<size_t> {
    if (IsClosed()) {
        return ResultT<size_t>(ToStdErrorCode(asio::error::not_connected));
    }
    auto n = asio::async_write(*socket_, asio::buffer(data, size), boost::fibers::asio::yield[ec_]);
    if (ec_) {
        if (!IsConnectionClosedError(ec_) && !IsTransientIoError(ec_)) {
            LOG(warn, "write failed: {}", ec_.message());
        }
    }
    return ResultT{ec_, size_t(n)};
}

auto Connection::WritevImpl(const std::vector<BufView>& bufs) noexcept -> ResultT<size_t> {
    if (IsClosed()) {
        return ResultT<size_t>(ToStdErrorCode(asio::error::not_connected));
    }

    auto n = asio::async_write(*socket_, bufs, boost::fibers::asio::yield[ec_]);
    if (ec_) {
        if (!IsConnectionClosedError(ec_) && !IsTransientIoError(ec_)) {
            LOG(warn, "write vector failed: {}", ec_.message());
        }
    }
    return ResultT{ec_, size_t(n)};
}

auto Connection::HandleRequests() noexcept -> void {
    std::vector<std::string> args;
    while (!IsClosed()) {
        auto parse_res = p_.ParseOne(args);

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
            auto reply_ec = ReplyParseError(s_, parse_res);
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

            auto reply_ec = ReplyParseError(s_, parse_err);
            if (reply_ec && !IsConnectionClosedError(reply_ec) &&
                !IsTransientIoError(reply_ec)) {
                LOG(warn, "failed to send protocol error reply: {}", reply_ec.message());
            }
            break;
        }
        std::transform(args[0].begin(), args[0].end(), args[0].begin(),
                    [](unsigned char c) { return static_cast<char>(std::tolower(c)); });

        if (parse_res != ParserResut::HAS_MORE) {
            s_.SetBatch(false);
        }

        engine->DispatchCmd(this, args);
    }
}

auto Connection::Flush() -> void {
    if (s_.GetError() || IsClosed()) {
        return;
    }
    s_.Flush();
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
            boost::system::error_code ignored_ec;
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
