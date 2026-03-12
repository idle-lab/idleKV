#include "redis/parser.h"

#include "common/logger.h"
#include "redis/error.h"

#include <algorithm>
#include <asio/awaitable.hpp>
#include <charconv>
#include <climits>
#include <cstddef>
#include <cstring>
#include <spdlog/spdlog.h>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <variant>
#include <vector>

namespace idlekv {

auto operator==(DataType dt, char prefix) -> bool { return static_cast<char>(dt) == prefix; };

namespace {

// debug function
[[maybe_unused]] std::string escape_string(std::string_view s) {
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

} // namespace

auto Reader::read_line() noexcept -> asio::awaitable<ResultT<std::string>> {
    std::string line;

    for (;;) {
        auto rv  = buf_.read_view();
        auto pos = std::find(rv.begin(), rv.end(), '\n');
        if (pos == rv.end()) {
            line.append(rv.data(), rv.size());

            buf_.clear();
            auto ec = co_await fill();
            if (ec) {
                co_return ec;
            }
            continue;
        }

        line.append(rv.begin(), pos + 1);
        buf_.consume(pos + 1 - rv.data());
        co_return line;
    }
}

auto Reader::read_line_view() noexcept -> asio::awaitable<ResultT<std::string_view>> {
    for (;;) {
        auto rv  = buf_.read_view();
        auto pos = std::find(rv.begin(), rv.end(), '\n');
        if (pos == rv.end()) {
            CHECK(rv.size() < buf_.capacity()) << "line length exceeds buffer size";
            buf_.defrag();
            auto ec = co_await fill();
            if (ec) {
                co_return ec;
            }
            continue;
        }

        std::string_view line(rv.data(), pos - rv.data() + 1);
        buf_.consume(pos + 1 - rv.data());
        co_return line;
    }
}


auto Reader::read_bytes_to(byte* buf, size_t len) noexcept -> asio::awaitable<ResultT<std::monostate>> {
    size_t offset = 0;

    for (;;) {
        auto rv = buf_.read_view();
        if (rv.size() >= len) {
            std::memcpy(buf + offset, rv.data(), len);
            buf_.consume(len);
            co_return std::monostate{};
        } else {
            std::memcpy(buf + offset, rv.data(), rv.size());
            offset += rv.size();
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

auto Reader::has_more() -> bool { return buf_.buffered() > 0; }

auto Writer::write(std::string_view s) -> void { vecs_.emplace_back(s.data(), s.size()); }

auto Writer::reserve_buf(size_t expected_buffer_cap) -> void {
    CHECK_LE(expected_buffer_cap, kMaxBufferSize);
    auto cap = std::bit_ceil(expected_buffer_cap);

    if (cap > kMaxBufferSize) {
        cap = expected_buffer_cap;
    }

    if (cap > buf_.capacity()) {
        buf_.reserve(cap);
    }
}

auto Writer::flush() -> asio::awaitable<std::error_code> {
    if (vecs_.empty()) {
        co_return std::error_code{};
    }

    auto res = co_await writev_impl(vecs_);

    vecs_.clear();
    co_return res.err();
}

auto Parser::parse_one() noexcept -> asio::awaitable<ParserResut> {
    auto headerRes = co_await rd_->read_line_view();
    if (!headerRes.ok()) {
        co_return headerRes.err();
    }
    
    auto& header = headerRes.value();
    if (header[0] != DataType::Arrays) [[unlikely]] {
        co_return ParserResut(ParserResut::WRONG_TYPE_ERROR, kWrongTypeErr);
    }
    int arrLen;
    auto [ptr, err] =
        std::from_chars(header.data() + 1, header.data() + header.size() - 2, arrLen);
    if (err != std::errc()) [[unlikely]] {
        co_return std::make_error_code(err);
    }

    std::vector<std::string> args(arrLen);

    for (int i = 0; i < arrLen; ++i) {
        auto lineRes = co_await rd_->read_line_view();
        if (!lineRes.ok()) {
            co_return lineRes.err();
        }

        auto& line = lineRes.value();
        if (line.size() < 4 || line[0] != DataType::BulkString) [[unlikely]] {
            co_return ParserResut(ParserResut::WRONG_TYPE_ERROR, kWrongTypeErr);
        }

        int strLen;
        auto [ptr, err] = std::from_chars(
            line.data() + 1, line.data() + line.size() - 2 /* exclude CRLF */, strLen);
        if (err != std::errc()) [[unlikely]] {
            co_return std::make_error_code(err);
        }

        // empty bulk string
        if (strLen == -1) {
            continue;
        }

        args[i].resize(strLen + 2);

        auto bytesRes = co_await rd_->read_bytes_to(args[i].data(), strLen + 2 /* include CRLF */);
        if (!bytesRes.ok()) {
            co_return bytesRes.err();
        }

        // pop the trailing CRLF
        args[i].resize(strLen);
    }

    co_return ParserResut(rd_->has_more() ? ParserResut::HAS_MORE : ParserResut::OK,
                          std::move(args));
}

auto build_simple_string_on(byte* buf, std::string_view s) -> size_t {
    size_t total_len = s.size() + 3; // prefix + CRLF
    std::memcpy(buf, SIMPLE_STRING_PREFIX, 1);
    std::memcpy(buf + 1, s.data(), s.size());
    std::memcpy(buf + 1 + s.size(), CRLF, 2);
    return total_len;
}

auto build_error_on(byte* buf, std::string_view s) -> size_t {
    size_t total_len = s.size() + 3; // prefix + CRLF
    std::memcpy(buf, ERROR_PREFIX, 1);
    std::memcpy(buf + 1, s.data(), s.size());
    std::memcpy(buf + 1 + s.size(), CRLF, 2);
    return total_len;
}

auto Sender::send_simple_string(std::string_view s) -> asio::awaitable<void> {
    batched_reply_[batched_count_].resize(s.size() + 3);
    build_simple_string_on(batched_reply_[batched_count_].data(), s);
    wr_->write(batched_reply_[batched_count_]);
    batched_count_ += 1;
    batched_size_ += s.size() + 3;

    if (should_flush()) {
        co_await flush();
    }
}

auto Sender::send_ok() -> asio::awaitable<void> {
    wr_->write("+OK\r\n");
    batched_size_ += 5;
    if (should_flush()) {
        co_await flush();
    }
}

auto Sender::send_pong() -> asio::awaitable<void> {
    wr_->write("+PONG\r\n");
    batched_size_ += 7;
    if (should_flush()) {
        co_await flush();
    }
}

auto Sender::send_error(std::string_view s) -> asio::awaitable<void> {
    batched_reply_[batched_count_].resize(s.size() + 3);
    build_error_on(batched_reply_[batched_count_].data(), s);
    wr_->write(batched_reply_[batched_count_]);

    batched_count_ += 1;
    batched_size_ += s.size() + 3;

    if (should_flush()) {
        co_await flush();
    }
}

auto Sender::should_flush() -> bool {
    return batched_size_ >= kMaxReplyFlushBytes || batched_count_ >= kMaxReplyFlushCount;
}

auto Sender::flush() -> asio::awaitable<void> {
    if (batched_size_ == 0) {
        co_return;
    }

    ec_       = co_await wr_->flush();

    batched_count_ = 0;
    batched_size_ = 0;
}

} // namespace idlekv
