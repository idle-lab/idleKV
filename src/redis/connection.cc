#include "redis/connection.h"

#include "common/logger.h"
#include "common/result.h"
#include "db/engine.h"
#include "redis/error.h"
#include "redis/parser.h"
#include "redis/service.h"
#include "server/el_pool.h"
#include "server/fiber_runtime.h"

#include <array>
#include <boost/asio/read.hpp>
#include <boost/asio/registered_buffer.hpp>
#include <boost/fiber/context.hpp>
#include <boost/fiber/mutex.hpp>
#include <boost/fiber/operations.hpp>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <spdlog/spdlog.h>
#include <sys/uio.h>
#include <system_error>
#include <utility>
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

auto Connection::ReadvImpl(const std::array<Buf, 2>& bufs) noexcept -> ResultT<size_t> {
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
    cur_args_ = RedisService::Tlocal()->GetCmdArgsOrCreate();

    for (uint32_t count = 0; !IsClosed(); count++) {
        auto parse_res = p_.ParseOne(*cur_args_);

        if (!parse_res.Ok()) [[unlikely]] {
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
            if (reply_ec && !IsConnectionClosedError(reply_ec) && !IsTransientIoError(reply_ec)) {
                LOG(warn, "failed to send parse error reply: {}", reply_ec.message());
            }
            break;
        }

        auto& args = *cur_args_;
        if (args.empty()) [[unlikely]] {
            auto parse_err = ParserResut(ParserResut::PROTOCOL_ERROR,
                                         fmt::format(kProtocolErrFmt, "empty command"));

            auto reply_ec = ReplyParseError(s_, parse_err);
            if (reply_ec && !IsConnectionClosedError(reply_ec) && !IsTransientIoError(reply_ec)) {
                LOG(warn, "failed to send protocol error reply: {}", reply_ec.message());
            }
            break;
        }
        std::transform(args[0].begin(), args[0].end(), const_cast<char*>(args[0].begin()),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });

        bool can_sync_dispatch = parse_res != ParserResut::HAS_MORE && pipeline_queue_.empty() &&
                                 !async_fiber_.joinable();

        if (can_sync_dispatch) {
            s_.SetBatch(false);
            engine->DispatchCmd(this, args);
        } else {
            if (!async_fiber_.joinable()) [[unlikely]] {
                async_fiber_ = boost::fibers::fiber([this] { AsyncHandle(); });
            }

            pipeline_queue_.emplace_back(std::move(cur_args_));
            cur_args_ = RedisService::Tlocal()->GetCmdArgsOrCreate();

            async_cv_.notify_one();
        }

        // check every 32 commands.
        if ((count & ((1 << 5) - 1)) == 0) {
            MaybeYieldOnCpuBudget(kMaxBusyCpuTime);
        }

        cur_args_->clear();
    }
}

auto Connection::AsyncHandle() noexcept -> void {
    boost::fibers::no_op_lock mu;

    std::unique_lock<boost::fibers::no_op_lock> lk(mu);

    uint64_t pre_shed_count = -1;
    while (!IsClosed()) {
        async_cv_.wait(lk, [this]() -> bool { return IsClosed() || !pipeline_queue_.empty(); });

        uint64_t cur_count = boost::this_fiber::properties<FiberCpuProps>().switch_count;
        if (pipeline_queue_.size() == 1 && cur_count == pre_shed_count) {
            boost::this_fiber::yield();
        }
        pre_shed_count = cur_count;

        if (IsClosed()) {
            return;
        }

        s_.SetBatch(pipeline_queue_.size() > 1);

        // Squash pipeline cmd
        for (uint32_t count = 0; !pipeline_queue_.empty(); count++) {
            auto args = std::move(pipeline_queue_.front());
            pipeline_queue_.pop_front();

            engine->DispatchCmd(this, *args);

            // check every 32 commands.
            if ((count & ((1 << 5) - 1)) == 0) {
                MaybeYieldOnCpuBudget(kMaxBusyCpuTime);
            }

            // recycle CmdArgs.
            RedisService::Tlocal()->RecycleCmdArgs(std::move(args));
        }

        if (!p_.HashMore()) {
            s_.Flush();
        }
    }
};

auto Connection::Flush() -> void {
    if (s_.GetError() || IsClosed()) {
        return;
    }
    s_.Flush();
}

auto Connection::Reset(asio::ip::tcp::socket&& socket) -> void {
    CHECK(socket_.has_value() == false) << "override a connection that is currently in use";
    socket_.emplace(std::move(socket));
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
    if (async_fiber_.joinable()) {
        async_cv_.notify_all();
        async_fiber_.join();
    }
    p_.Clear();
    s_.Clear();
    ec_ = std::error_code{};
}

} // namespace idlekv
