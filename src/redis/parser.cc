#include "redis/parser.h"

#include "common/logger.h"
#include "common/result.h"
#include "metric/avg.h"
#include "redis/error.h"

#include <algorithm>
#include <asio/awaitable.hpp>
#include <charconv>
#include <chrono>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <spdlog/fmt/fmt.h>
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

template <typename T>
    requires(std::integral<std::remove_cvref_t<T>> &&
             !std::same_as<std::remove_cvref_t<T>, bool>)
auto decimal_size(T value) -> size_t {
    using Value = std::remove_cvref_t<T>;

    char tmp[std::numeric_limits<Value>::digits10 + 3];
    auto [ptr, ec] = std::to_chars(tmp, tmp + sizeof(tmp), value);
    (void)ec;
    return static_cast<size_t>(ptr - tmp);
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

auto Reader::read_bytes_to(byte* buf, size_t len) noexcept
    -> asio::awaitable<ResultT<std::monostate>> {
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

// auto Writer::reserve_buf(size_t required_piece_size) -> std::error_code {
//     if (required_piece_size <= buf_.capacity()) {
//         return std::error_code{};
//     }

//     const size_t doubled_capacity = buf_.capacity() == 0 ? required_piece_size : buf_.capacity() * 2;
//     const size_t next_capacity =
//         std::min(kDefaultWriteBufferSize, std::max(required_piece_size, doubled_capacity));
//     if (next_capacity < required_piece_size) {
//         return std::make_error_code(std::errc::no_buffer_space);
//     }

//     buf_.reserve(next_capacity);
//     return std::error_code{};
// }

auto Writer::reset_write_state() -> void {
    buf_.clear();
    vecs_.clear();
    queued_size_ = 0;
}

auto Writer::write(std::string_view s) -> asio::awaitable<std::error_code> {
    if (buf_.write_size() < s.size() || queued_size_ + s.size() >= kMaxReplyFlushBytes) {
        auto ec = co_await flush();
        if (ec) {
            co_return ec;
        }
    }

    if (buf_.write_size() < s.size()) {
        buf_.reserve(buf_.buffered() + s.size());
    }

    const size_t offset = buf_.buffered();
    byte*        begin  = buf_.data() + offset;
    std::memcpy(begin, s.data(), s.size());

    queued_size_ += s.size();
    buf_.commit(s.size());
    vecs_.emplace_back(begin, s.size());

    if (vecs_.size() >= kMaxReplyFlushCount || queued_size_ >= kMaxReplyFlushBytes) {
        auto ec = co_await flush();
        if (ec) {
            co_return ec;
        }
    }

    co_return std::error_code{};
}

auto Writer::flush() -> asio::awaitable<std::error_code> {
    if (vecs_.empty()) {
        co_return std::error_code{};
    }

    // vecs_ preserves the original enqueue order, so a single writev keeps
    // mixed owned/external pieces in the same packet order they were queued.
    auto res = co_await writev_impl(vecs_);

    reset_write_state();
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
    auto [ptr, err] = std::from_chars(header.data() + 1, header.data() + header.size() - 2, arrLen);
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
        auto [ptr, err] = std::from_chars(line.data() + 1,
                                          line.data() + line.size() - 2 /* exclude CRLF */, strLen);
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

auto Sender::send_simple_string(std::string_view s) -> asio::awaitable<void> {
    ec_ = co_await wr_->write_pieces(SIMPLE_STRING_PREFIX, s, CRLF);
}

auto Sender::send_ok() -> asio::awaitable<void> {
    ec_ = co_await wr_->write_pieces("+OK\r\n");
}

auto Sender::send_pong() -> asio::awaitable<void> {
    ec_ = co_await wr_->write_pieces("+PONG\r\n");
}

auto Sender::send_bulk_string(std::string_view s) -> asio::awaitable<void> {
    ec_ = co_await wr_->write_pieces(BULK_STRING_PREFIX, s.size(), CRLF);
    ec_ = co_await wr_->write(s);
    ec_ = co_await wr_->write(CRLF);
}

auto Sender::send_null_bulk_string() -> asio::awaitable<void> {
    ec_ = co_await wr_->write_pieces("$-1\r\n");
}

auto Sender::send_integer(int64_t value) -> asio::awaitable<void> {
    ec_ = co_await wr_->write_pieces(INTEGER_PREFIX, value, CRLF);
}

auto Sender::send_error(std::string_view s) -> asio::awaitable<void> {
    ec_ = co_await wr_->write_pieces(ERROR_PREFIX, s, CRLF);
}

auto Sender::flush() -> asio::awaitable<void> {
    ec_ = co_await wr_->flush();
}

} // namespace idlekv
