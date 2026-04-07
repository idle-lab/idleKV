#include "redis/connection.h"

#include "common/logger.h"
#include "common/result.h"
#include "db/command.h"
#include "db/engine.h"
#include "metric/prometheus.h"
#include "redis/error.h"
#include "redis/parser.h"
#include "redis/service.h"
#include "server/el_pool.h"
#include "server/fiber_runtime.h"
#include "utils/coroutine/generator.h"

#include <array>
#include <boost/asio/read.hpp>
#include <boost/asio/registered_buffer.hpp>
#include <boost/asio/write.hpp>
#include <boost/fiber/context.hpp>
#include <boost/fiber/mutex.hpp>
#include <boost/fiber/operations.hpp>
#include <boost/fiber/policy.hpp>
#include <boost/fiber/type.hpp>
#include <cstddef>
#include <memory>
#include <mutex>
#include <spdlog/spdlog.h>
#include <sys/uio.h>
#include <system_error>
#include <utility>
#include <vector>

namespace idlekv {

namespace {

using RequestClock = std::chrono::steady_clock;

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

    boost::system::error_code ec;
    auto n = socket_->async_read_some(asio::buffer(buf, size), boost::fibers::asio::yield[ec]);
    if (ec) {
        if (!IsConnectionClosedError(ec) && !IsTransientIoError(ec)) {
            LOG(warn, "read failed: {}", ec.message());
        }
    }
    return ResultT{ec, size_t(n)};
}

auto Connection::ReadImpl(asio::mutable_registered_buffer reg_buf) noexcept -> ResultT<size_t> {
    if (IsClosed()) {
        return ResultT<size_t>(ToStdErrorCode(asio::error::not_connected));
    }
    boost::system::error_code ec;
    auto n = socket_->async_read_some(reg_buf, boost::fibers::asio::yield[ec]);
    if (ec) {
        if (!IsConnectionClosedError(ec) && !IsTransientIoError(ec)) {
            LOG(warn, "read register failed: {}", ec.message());
        }
    }
    return ResultT{ec, size_t(n)};
}

auto Connection::ReadvImpl(const std::array<Buf, 2>& bufs) noexcept -> ResultT<size_t> {
    if (IsClosed()) {
        return ResultT<size_t>(ToStdErrorCode(asio::error::not_connected));
    }

    boost::system::error_code ec;
    auto n = socket_->async_read_some(bufs, boost::fibers::asio::yield[ec]);
    if (ec) {
        if (!IsConnectionClosedError(ec) && !IsTransientIoError(ec)) {
            LOG(warn, "read vector failed: {}", ec.message());
        }
    }
    return ResultT{ec, size_t(n)};
}

auto Connection::WriteImpl(const char* data, size_t size) noexcept -> ResultT<size_t> {
    if (IsClosed()) {
        return ResultT<size_t>(ToStdErrorCode(asio::error::not_connected));
    }
    boost::system::error_code ec;
    auto n = asio::async_write(*socket_, asio::buffer(data, size), boost::fibers::asio::yield[ec]);
    if (ec) {
        if (!IsConnectionClosedError(ec) && !IsTransientIoError(ec)) {
            LOG(warn, "write failed: {}", ec.message());
        }
    }
    return ResultT{ec, size_t(n)};
}

auto Connection::WritevImpl(const std::vector<BufView>& bufs) noexcept -> ResultT<size_t> {
    if (IsClosed()) {
        return ResultT<size_t>(ToStdErrorCode(asio::error::not_connected));
    }

    boost::system::error_code ec;
    auto n = asio::async_write(*socket_, bufs, boost::fibers::asio::yield[ec]);
    if (ec) {
        if (!IsConnectionClosedError(ec) && !IsTransientIoError(ec)) {
            LOG(warn, "write vector failed: {}", ec.message());
        }
    }
    return ResultT{ec, size_t(n)};
}

auto Connection::HandleRequests() noexcept -> void {
    while (!IsClosed()) {
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
            const auto started_at = RequestClock::now();
            s_.SetBatch(false);
            engine->DispatchCmd(ctx_.get(), args);
            PrometheusMetrics::Instance().ObserveRequestDuration(RequestClock::now() - started_at);

            args.ClearForReuse();
        } else {
            if (!async_fiber_.joinable()) [[unlikely]] {
                async_fiber_ =
                    boost::fibers::fiber(boost::fibers::launch::post, [this] { AsyncHandle(); });
            }

            pipeline_queue_.emplace_back(std::move(cur_args_), RequestClock::now());
            cur_args_ = RedisService::Tlocal()->GetCmdArgsOrCreate();

            async_cv_.notify_one();
        }

        MaybeYieldOnCpuBudget(kMaxParseCpuCycles);
    }
}

auto Connection::AsyncHandle() noexcept -> void {
    boost::this_fiber::properties<FiberProps>().SetName("ConnectionAsyncHandler");
    boost::fibers::no_op_lock mu;

    std::unique_lock<boost::fibers::no_op_lock> lk(mu);
    bool                                        yielded_for_batch = false;

    while (!IsClosed()) {
        async_cv_.wait(lk, [this]() -> bool { return IsClosed() || !pipeline_queue_.empty(); });

        if (IsClosed()) {
            return;
        }

        if (!yielded_for_batch && pipeline_queue_.size() == 1 && p_.HashMore()) {
            yielded_for_batch = true;
            boost::this_fiber::yield();
            LOG(debug, "pipeline depth {} after yielding for batch", pipeline_queue_.size());
        }

        s_.SetBatch(pipeline_queue_.size() > 1);

        // If the pipeline queue is too long, we squash all pending requests in the queue without
        // yielding.
        if (pipeline_queue_.size() > kPipelineSquashThreshold) {
            // Squash pipeline cmd
            auto gen = [this]() -> utils::Generator<PendingRequest> {
                for (size_t idx = 0; idx < pipeline_queue_.size(); idx++) {
                    auto& req = pipeline_queue_[idx];
                    co_yield PendingRequest{.args = req.args_ptr.get(), .started_at = req.started_at};
                }
            }();

            size_t squash_limit = pipeline_queue_.size();

            // ======= start squash =======
            size_t processed = engine->DispatchManyCmd(ctx_.get(), gen, squash_limit);
            // ======== end squash ========

            for (size_t i = 0; i < processed; i++) {
                auto& request = pipeline_queue_.front();
                RedisService::Tlocal()->RecycleCmdArgs(std::move(request.args_ptr));
                pipeline_queue_.pop_front();
            }
        } else {
            auto request = std::move(pipeline_queue_.front());
            pipeline_queue_.pop_front();

            engine->DispatchCmd(ctx_.get(), *request.args_ptr);

            PrometheusMetrics::Instance().ObserveRequestDuration(RequestClock::now() -
                                                                 request.started_at);
            RedisService::Tlocal()->RecycleCmdArgs(std::move(request.args_ptr));
        }


        if (pipeline_queue_.empty()) {
            yielded_for_batch = false;
            if (!p_.HashMore()) {
                s_.Flush();
            }
        }
    }
};

auto Connection::Flush() -> void {
    if (s_.GetError() || IsClosed()) {
        return;
    }
    s_.Flush();
}

auto Connection::Init(asio::ip::tcp::socket&& socket) -> void {
    CHECK(socket_.has_value() == false) << "override a connection that is currently in use";
    socket_.emplace(std::move(socket));
    closing_ = false;
    db_index_ = 0;
    cur_args_ = RedisService::Tlocal()->GetCmdArgsOrCreate();
    ctx_ = std::make_unique<ExecContext>(this);
}

auto Connection::Reset() -> void {
    closing_ = true;

    if (socket_.has_value() && socket_->is_open()) {
        boost::system::error_code ignored_ec;
        DISCARD_RESULT(socket_->cancel(ignored_ec));
        if (ignored_ec && !IsConnectionClosedError(ignored_ec) && !IsTransientIoError(ignored_ec)) {
            LOG(warn, "cancel socket failed: {}", ignored_ec.message());
        }
    }

    if (async_fiber_.joinable()) {
        async_cv_.notify_all();
        async_fiber_.join();
    }

    if (socket_.has_value()) {
        if (socket_->is_open()) {
            boost::system::error_code ignored_ec;
            DISCARD_RESULT(socket_->close(ignored_ec));

            if (ignored_ec && !IsConnectionClosedError(ignored_ec) &&
                !IsTransientIoError(ignored_ec)) {
                LOG(warn, "close socket failed: {}", ignored_ec.message());
            }
        }
        socket_.reset();
    }

    while (!pipeline_queue_.empty()) {
        auto& request = pipeline_queue_.front();
        RedisService::Tlocal()->RecycleCmdArgs(std::move(request.args_ptr));
        pipeline_queue_.pop_front();
    }
    if (cur_args_) {
        RedisService::Tlocal()->RecycleCmdArgs(std::move(cur_args_));
    }

    p_.Clear();
    s_.Clear();
    ctx_.reset();
}

} // namespace idlekv
