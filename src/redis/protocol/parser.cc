#include "redis/protocol/parser.h"

#include "common/logger.h"
#include "redis/protocol/reply.h"

#include <algorithm>
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

template <typename T> size_t piece_size(const T& v) {
    if constexpr (std::is_integral_v<T>) {
        return 32;
    }
    else  {
        // string_view
        return v.size();
    }
}

template <size_t S> byte* write_piece(const byte (&arr)[S], byte* dest) {
  return (byte*)memcpy(dest, arr, S - 1) + (S - 1);
}

template <typename T>
    requires std::is_integral_v<T>
auto write_piece(T num, byte* dest) {
  static_assert(!std::is_same_v<T, char>, "Use arrays for single chars");
  constexpr size_t kMaxIntegralPieceSize = 32;
  auto [ptr, ec] = std::to_chars(dest, dest + kMaxIntegralPieceSize, num);
  CHECK_EQ(ec, std::errc()) << "write_piece integer conversion failed";
  return ptr;
}

byte* write_piece(std::string_view str, byte* dest) {
  return (byte*)memcpy(dest, str.data(), str.size()) + str.size();
}

}

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

auto Reader::has_more() -> bool {
    return buf_.buffered() > 0;
}


// template <typename... Ts> 
// auto Writer::write_pieces(Ts&&... pieces) -> asio::awaitable<void> {
//     if (size_t required = (piece_size(pieces) + ...); buf_.write_size() <= required) {
//         CHECK_LE(required, kMaxBufferSize);
//         co_await flush();
//         reserve_buf(required);
//     }

//     auto wv = buf_.write_view();
//     auto* ptr = wv.data();
//     ([&]() { ptr = write_piece(pieces, ptr); }(), ...);

//     size_t written = ptr - wv.data();
//     buf_.commit(written);
// }

// auto Writer::write_ref(std::string_view s) -> asio::awaitable<void> {
//     if (s.size() > kMaxBufferSize) {
//         std::vector<BufView> bufs;
//         bufs.emplace_back(buf_.read_view());
//         bufs.emplace_back(BufView{s.data(), s.size()});

//         auto res = co_await writev_impl(bufs);
//         if (!res.ok()) {
//             ec_ = res.err();
//         }
//         buf_.clear();
//         co_return;
//     }

//     co_await write_pieces(s);
// }

auto Writer::write(std::string_view s) -> void {
    vecs_.emplace_back(s.data(), s.size());
}

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
    if (header[0] != static_cast<char>(DataType::Arrays)) [[unlikely]] {
        co_return ParserResut(ParserResut::WRONG_TYPE_ERROR, WrongTypeErr::make_reply());
    }

    int arrLen;
    auto [ptr, err] =
        std::from_chars(header.c_str() + 1, header.c_str() + header.size() - 2, arrLen);
    if (err != std::errc()) [[unlikely]] {
        co_return std::make_error_code(err);
    }

    std::vector<std::string> args(arrLen);

    for (auto i : std::views::iota(0, arrLen)) {
        auto lineRes = co_await rd_->read_line();
        if (!lineRes.ok()) {
            co_return lineRes.err();            
        }

        auto& line = lineRes.value();
        if (line.size() < 4 || line[0] != static_cast<char>(DataType::BulkString)) [[unlikely]] {
            co_return ParserResut(ParserResut::WRONG_TYPE_ERROR, WrongTypeErr::make_reply());
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

    co_return ParserResut(rd_->has_more() ? ParserResut::HAS_MORE : ParserResut::OK,std::move(args));
}

auto Sender::send(std::string&& s) -> asio::awaitable<void> {
    batched_count_++;
    batched_size_ += s.size();
    batched_reply_.emplace_back(std::move(s));

    wr_->write(batched_reply_.back());

    if (should_flush()) {
        co_await flush();
    }
}

auto Sender::should_flush() -> bool {
    return batched_size_ >= kMaxReplyFlushBytes || batched_count_ >= kMaxReplyFlushCount;
}

auto Sender::flush() -> asio::awaitable<void> {
    ec_ = co_await wr_->flush();

    batched_count_ = 0;
    batched_size_ = 0;
    batched_reply_.clear();
}


} // namespace idlekv
