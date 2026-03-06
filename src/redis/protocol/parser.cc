#include "redis/protocol/parser.h"

#include "common/logger.h"
#include "redis/protocol/reply.h"

#include <asio/awaitable.hpp>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstring>
#include <memory>
#include <ranges>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <variant>
#include <vector>

namespace idlekv {

const char* CRLF = "\r\n";

auto Reader::read_line() noexcept -> asio::awaitable<ResultT<std::string>> {
    std::string line;

    for (;;) {
        auto rv  = buf_.read_view();
        auto pos = std::find(rv.data(), rv.end(), '\n');
        if (pos == rv.end()) {
            line.append(rv.data(), rv.size());

            buf_.clear();
            auto ec = co_await fill();
            if (ec) {
                co_return ec;
            }
            continue;
        }

        line.append(rv.data(), pos + 1);
        buf_.consume(pos + 1 - rv.data());
        co_return line;
    }
}

auto Reader::read_bytes(size_t len) noexcept -> asio::awaitable<ResultT<std::string>> {
    std::string data;

    for (;;) {
        auto rv = buf_.read_view();
        if (rv.size() >= len) {
            data.append(rv.data(), len);
            buf_.consume(len);
            co_return data;
        } else {
            data.append(rv.data(), rv.size());
            len -= rv.size();

            buf_.clear();
            auto ec = co_await fill();
            if (ec) {
                co_return ec;
            }
        }
    }
}

auto Reader::fill() -> asio::awaitable<std::error_code> {
    auto wv  = buf_.write_view();
    auto res = co_await read_impl(wv.data(), wv.size());
    if (!res.ok()) {
        co_return res.err();
    }

    buf_.commit(res.value());
    co_return std::error_code{};
}

auto Writer::write(std::string_view s) -> asio::awaitable<std::error_code> {
    auto wv = buf_.write_view();
    if (s.size() > wv.size()) {
        auto ec = co_await flush();
        if (ec) {
            co_return ec;
        }

        if (s.size() > buf_.capacity()) {
            // it's too large, so it can only be written directly
            auto res = co_await write_impl(s.data(), s.size());
            if (!res.ok()) {
                co_return res.err();
            }
            co_return std::error_code{};
        }
        wv = buf_.write_view();
    }

    // now we have enough space
    std::memcpy(wv.data(), s.data(), s.size());
    buf_.commit(s.size());
    co_return std::error_code{};
}

auto Writer::write(uint32_t n) -> asio::awaitable<std::error_code> {
    char digits[10]; // max uint32 is 4294967295
    auto [ptr, ec] = std::to_chars(digits, digits + sizeof(digits), n);
    if (ec != std::errc()) [[unlikely]] {
        co_return std::make_error_code(ec);
    }

    co_return co_await write(std::string_view(digits, static_cast<size_t>(ptr - digits)));
}


auto Writer::flush() -> asio::awaitable<std::error_code> {
    auto rv = buf_.read_view();
    if (rv.size() == 0) {
        co_return std::error_code{};
    }

    auto res = co_await write_impl(rv.data(), rv.size());
    if (!res.ok()) {
        co_return res.err();
    }

    // believe the data in the rv has been fully written
    buf_.clear();
    co_return std::error_code{};
}

// debug function
std::string escape_string(const std::string& s) {
    std::string out;
    out.reserve(s.size() * 2);

    for (unsigned char c : s) {
        switch (c) {
        case '\n':
            out += "\\n";
            break;
        case '\r':
            out += "\\r";
            break;
        case '\t':
            out += "\\t";
            break;
        case '\0':
            out += "\\0";
            break;
        case '\\':
            out += "\\\\";
            break;
        case '\"':
            out += "\\\"";
            break;
        default:
            if (std::isprint(c)) {
                out += c;
            } else {
                char buf[5];
                std::snprintf(buf, sizeof(buf), "\\x%02X", c);
                out += buf;
            }
        }
    }

    return out;
}

auto Parser::parse_one() noexcept -> asio::awaitable<Result> {
    auto headerRes = co_await rd_->read_line();
    if (!headerRes.ok()) {
        co_return Result{std::vector<std::string>{},
                         std::make_unique<StandardErr>(headerRes.err())};
    }

    auto& header = headerRes.value();
    if (header[0] != static_cast<char>(DataType::Arrays)) [[unlikely]] {
        co_return Result{std::vector<std::string>{}, std::make_unique<WrongTypeErr>()};
    }

    int arrLen;
    auto [ptr, err] =
        std::from_chars(header.c_str() + 1, header.c_str() + header.size() - 2, arrLen);
    if (err != std::errc()) [[unlikely]] {
        co_return Result{std::vector<std::string>{},
                         std::make_unique<ProtocolErr>(std::make_error_code(err).message())};
    }

    std::vector<std::string> args(arrLen);

    for (auto i : std::views::iota(0, arrLen)) {
        auto lineRes = co_await rd_->read_line();
        if (!lineRes.ok()) {
            co_return Result{std::vector<std::string>{},
                             std::make_unique<StandardErr>(lineRes.err())};
        }

        auto& line = lineRes.value();
        if (line.size() < 4 || line[0] != static_cast<char>(DataType::BulkString)) [[unlikely]] {
            co_return Result{std::vector<std::string>{}, std::make_unique<WrongTypeErr>()};
        }

        int strLen;
        auto [ptr, err] = std::from_chars(
            line.c_str() + 1, line.c_str() + line.size() - 2 /* exclude CRLF */, strLen);
        if (err != std::errc()) [[unlikely]] {
            co_return Result{std::vector<std::string>{},
                             std::make_unique<ProtocolErr>(std::make_error_code(err).message())};
        }

        // empty bulk string
        if (strLen == -1) {
            continue;
        }

        auto bytesRes = co_await rd_->read_bytes(strLen + 2 /* include CRLF */);
        if (!bytesRes.ok()) {
            co_return Result{std::vector<std::string>{},
                             std::make_unique<StandardErr>(bytesRes.err())};
        }

        args[i] = std::move(bytesRes.value());

        // pop CRLF
        args[i].pop_back();
        args[i].pop_back();
    }

    co_return std::make_pair(std::move(args), nullptr);
}

auto Sender::send_simple_string(std::string&& s) -> void {
    reply_batch_.emplace(std::make_unique<SimpleString>(std::move(s)));
    size_ += reply_batch_.back()->size();
    reply_count_++;
}

auto Sender::send_bulk_string(std::string&& s, size_t len) -> void {
    reply_batch_.emplace(std::make_unique<BulkString>(std::move(s), len));
    size_ += reply_batch_.back()->size();
    reply_count_++;
}

auto Sender::send_ok() -> void {
    reply_batch_.emplace(std::make_unique<OKReply>());
    size_ += reply_batch_.back()->size();
    reply_count_++;
}

auto Sender::send_pong() -> void {
    reply_batch_.emplace(std::make_unique<PongReply>());
    size_ += reply_batch_.back()->size();
    reply_count_++;
}

auto Sender::flush() -> asio::awaitable<std::error_code> {
    while (!reply_batch_.empty()) {
        auto data = std::move(reply_batch_.front());
        reply_batch_.pop();

        auto ec = co_await wr_->write(data->to_bytes());
        if (ec) {
            co_return ec; 
        }
    }

    auto ec = co_await wr_->flush();
    if (ec) {
        co_return ec;
    }
    last_flushed_ = std::chrono::steady_clock::now();
    co_return std::error_code{};
}

auto Sender::has_pending() -> bool {
    return reply_count_ > 0;
}

auto Sender::should_flush() -> bool {
    if (reply_batch_.size() >= kMaxReplyFlushCount) {
        return true;
    }

    if (size_ >= kMaxReplyFlushSize) {
        return true;
    }

    auto now = std::chrono::steady_clock::now();
    return now - last_flushed_ >= std::chrono::microseconds(50);
}


} // namespace idlekv
