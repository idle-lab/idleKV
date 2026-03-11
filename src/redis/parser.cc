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
#include <vector>

namespace idlekv {

auto operator==(DataType dt, char prefix) -> bool {
    return static_cast<char>(dt) == prefix;
};

namespace {
// debug function
std::string escape_string(std::string_view s) {
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
    auto headerRes = co_await rd_->read_line();
    if (!headerRes.ok()) {
        co_return headerRes.err();
    }

    auto& header = headerRes.value();
    if (header[0] != DataType::Arrays) [[unlikely]] {
        co_return ParserResut(ParserResut::WRONG_TYPE_ERROR, kWrongTypeErr);
    }

    int arrLen;
    auto [ptr, err] =
        std::from_chars(header.c_str() + 1, header.c_str() + header.size() - 2, arrLen);
    if (err != std::errc()) [[unlikely]] {
        co_return std::make_error_code(err);
    }

    std::vector<std::string> args(arrLen);

    for (int i = 0; i < arrLen; ++i) {
        auto lineRes = co_await rd_->read_line();
        if (!lineRes.ok()) {
            co_return lineRes.err();
        }

        auto& line = lineRes.value();
        if (line.size() < 4 || line[0] != DataType::BulkString) [[unlikely]] {
            co_return ParserResut(ParserResut::WRONG_TYPE_ERROR, kWrongTypeErr);
        }

        int strLen;
        auto [ptr, err] = std::from_chars(
            line.c_str() + 1, line.c_str() + line.size() - 2 /* exclude CRLF */, strLen);
        if (err != std::errc()) [[unlikely]] {
            co_return std::make_error_code(err);
        }

        // empty bulk string
        if (strLen == -1) {
            continue;
        }

        auto bytesRes = co_await rd_->read_bytes(strLen + 2 /* include CRLF */);
        if (!bytesRes.ok()) {
            co_return bytesRes.err();
        }

        args[i] = std::move(bytesRes.value());

        // pop CRLF
        args[i].pop_back();
        args[i].pop_back();
    }

    co_return ParserResut(rd_->has_more() ? ParserResut::HAS_MORE : ParserResut::OK,
                          std::move(args));
}

auto Sender::send_simple_string(std::string&& s) -> asio::awaitable<void> {
    pieces_count_ += 3;
    batched_size_ += s.size() + 3;
    batched_reply_.emplace_back(std::move(s));

    wr_->write(SIMPLE_STRING_PREFIX);
    wr_->write(batched_reply_.back());
    wr_->write(CRLF);


    if (should_flush()) {
        co_await flush();
    }
}

auto Sender::send_error(std::string&& s) -> asio::awaitable<void> {
    pieces_count_ += 3;
    batched_size_ += s.size() + 3;
    batched_reply_.emplace_back(std::move(s));

    wr_->write(ERROR_PREFIX);
    wr_->write(batched_reply_.back());
    wr_->write(CRLF);

    if (should_flush()) {
        co_await flush();
    }
}

auto Sender::send(std::string&& s) -> asio::awaitable<void> {
    pieces_count_++;
    batched_size_ += s.size();
    batched_reply_.emplace_back(std::move(s));

    wr_->write(batched_reply_.back());

    if (should_flush()) {
        co_await flush();
    }
}

auto Sender::should_flush() -> bool {
    return batched_size_ >= kMaxReplyFlushBytes || pieces_count_ >= kMaxReplyFlushCount;
}

auto Sender::flush() -> asio::awaitable<void> {
    if (batched_reply_.empty()) {
        co_return ;
    }
    if (flushing_) {
        co_return ;
    }
    
    flushing_ = true;
    ec_ = co_await wr_->flush();

    pieces_count_ = 0;
    batched_size_  = 0;
    batched_reply_.clear();
    flushing_ = false;
}

} // namespace idlekv
