#include "redis/parser.h"

#include "common/logger.h"
#include "common/result.h"
#include "db/storage/kvstore.h"
#include "redis/error.h"

#include <algorithm>
#include <asio/awaitable.hpp>
#include <asio/buffer_registration.hpp>
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


auto Reader::read_line_view() noexcept -> asio::awaitable<ResultT<std::string_view>> {
    for (;;) {
        auto rv  = buf_.read_view();
        // auto pos = static_cast<const byte*>(std::memchr(rv.data(), '\n', rv.size()));
        auto pos = std::find(rv.begin(), rv.end(),  '\n');
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

    auto rv = buf_.read_view();
    if (rv.size() >= len) {
        std::memcpy(buf + offset, rv.data(), len);
        buf_.consume(len);
        co_return std::monostate{};
    }

    std::memcpy(buf + offset, rv.data(), rv.size());
    offset += rv.size();
    len -= rv.size();

    buf_.clear();

    bufs_.resize(2);
    bufs_[0]= Buf{buf + offset, len};
    auto wv  = buf_.write_view();
    bufs_[1]= Buf{wv.data(), wv.size()};

    for (;;) {
        auto res = co_await readv_impl(bufs_);
        if (!res.ok()) {
            co_return res.err();
        }
        if (res.value() < len) {
            offset +=  res.value();
            len -= res.value();
            bufs_[0] = Buf{buf + offset, len};
        } else {
            buf_.commit(res.value() - len);
            co_return std::monostate{};
        }
    }
}

auto Reader::fill() -> asio::awaitable<std::error_code> {
    ResultT<size_t> res{std::error_code{}};
    if (reg_buf_.data()) {
        auto reg_wv = asio::buffer(reg_buf_ + buf_.write_offset(), buf_.write_size());
        res  = co_await read_impl(reg_wv);
    } else {
        auto wv  = buf_.write_view();
        res = co_await read_impl(wv.data(), wv.size());
    }

    if (!res.ok()) {
        co_return res.err();
    }
    buf_.commit(res.value());
    co_return std::error_code{};
}

auto Reader::has_more() -> bool { return buf_.buffered() > 0; }

auto Writer::reset_write_state() -> void {
    buf_.clear();
    vecs_.clear();
    keepalive_.clear();
    queued_size_ = 0;
}

auto Writer::write_view(std::string_view s) -> asio::awaitable<std::error_code> {
    if (s.empty()) {
        co_return std::error_code{};
    }

    if (!vecs_.empty() &&
        (vecs_.size() >= kMaxReplyFlushCount || queued_size_ + s.size() >= kMaxReplyFlushBytes)) {
        auto ec = co_await flush();
        if (ec) {
            co_return ec;
        }
    }

    queued_size_ += s.size();
    vecs_.emplace_back(s.data(), s.size());

    co_return std::error_code{};
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

    co_return std::error_code{};
}

auto Writer::write_ref(std::string_view s, std::shared_ptr<const void> holder)
    -> asio::awaitable<std::error_code> {
    if (s.empty()) {
        co_return std::error_code{};
    }

    if (!vecs_.empty() &&
        (vecs_.size() >= kMaxReplyFlushCount || queued_size_ + s.size() >= kMaxReplyFlushBytes)) {
        auto ec = co_await flush();
        if (ec) {
            co_return ec;
        }
    }

    queued_size_ += s.size();
    vecs_.emplace_back(s.data(), s.size());
    if (holder) {
        keepalive_.push_back(std::move(holder));
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


auto Parser::parse_one(std::vector<std::string>& args) noexcept -> asio::awaitable<ParserResut> {
    auto headerRes = co_await rd_->read_line_view();
    if (!headerRes.ok()) {
        co_return headerRes.err();
    }

    auto& header = headerRes.value();
    if (header[0] != DataType::Arrays) [[unlikely]] {
        co_return ParserResut(ParserResut::PROTOCOL_ERROR, fmt::format("need '*' but '{}'", header[0]));
    }
    int arrLen;
    auto [ptr, err] = std::from_chars(header.data() + 1, header.data() + header.size() - 2, arrLen);
    if (err != std::errc()) [[unlikely]] {
        co_return std::make_error_code(err);
    }

    args.resize(arrLen);
    for (int i = 0; i < arrLen; ++i) {
        auto lineRes = co_await rd_->read_line_view();
        if (!lineRes.ok()) {
            co_return lineRes.err();
        }

        auto& line = lineRes.value();
        if (line.size() < 4 || line[0] != DataType::BulkString) [[unlikely]] {
            co_return ParserResut(ParserResut::PROTOCOL_ERROR, fmt::format("need $ but '{}'", line[0]));
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
    if (!ec_) {
        ec_ = co_await wr_->write_view(s);
    }
    if (!ec_) {
        ec_ = co_await wr_->write_pieces(CRLF);
    }
}

auto Sender::send_bulk_string(const std::shared_ptr<const DataEntity>& data) -> asio::awaitable<void> {
    if (!data) {
        co_await send_null_bulk_string();
        co_return;
    }

    const auto& value = data->as_string();
    ec_ = co_await wr_->write_pieces(BULK_STRING_PREFIX, value.size(), CRLF);
    if (!ec_) {
        ec_ = co_await wr_->write_ref(value, data);
    }
    if (!ec_) {
        ec_ = co_await wr_->write(CRLF);
    }
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
