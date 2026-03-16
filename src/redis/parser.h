#pragma once

#include "common/asio_no_exceptions.h"
#include "common/config.h"
#include "common/logger.h"
#include "common/result.h"

#include <array>
#include <asio/asio.hpp>
#include <asio/awaitable.hpp>
#include <asiochan/asiochan.hpp>
#include <charconv>
#include <chrono>
#include <climits>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>
#include <vector>

namespace idlekv {

constexpr size_t kDefaultReadBufferSize = 4096;
constexpr size_t kDefaultWriteBufferSize = 2048;
constexpr size_t kMaxReplyFlushCount    = IOV_MAX - 2;
constexpr size_t kMaxReplyFlushBytes    = 5 * KB;
using byte                              = char;

constexpr const char* CRLF                 = "\r\n";
constexpr const char* SIMPLE_STRING_PREFIX = "+";
constexpr const char* ERROR_PREFIX         = "-";
constexpr const char* INTEGER_PREFIX       = ":";
constexpr const char* BULK_STRING_PREFIX   = "$";
constexpr const char* ARRAY_PREFIX         = "*";

enum class DataType : char {
    String     = '+',
    Error      = '-',
    Integers   = ':',
    BulkString = '$',
    Arrays     = '*'
};

auto operator==(DataType dt, char prefix) -> bool;

class Buf {
public:
    Buf() = default;
    Buf(byte* data, size_t size) : data_(data), size_(size) {}

    Buf(Buf&&)                         = default;
    Buf(const Buf&)                    = default;
    auto operator=(Buf&&) -> Buf&      = default;
    auto operator=(const Buf&) -> Buf& = default;

    auto begin() -> byte* { return data_; }
    auto end() -> byte* { return data_ + size_; }
    auto data() -> byte* { return const_cast<byte*>(data_); }
    auto size() const -> size_t { return size_; }
    operator asio::mutable_buffer() const noexcept { return asio::mutable_buffer(data_, size_); }
    operator asio::const_buffer() const noexcept { return asio::const_buffer(data_, size_); }

private:
    byte* data_;
    size_t      size_;  
};

class BufView {
public:
    BufView() = default;
    BufView(const byte* data, size_t size) : data_(data), size_(size) {}

    BufView(BufView&&)                         = default;
    BufView(const BufView&)                    = default;
    auto operator=(BufView&&) -> BufView&      = default;
    auto operator=(const BufView&) -> BufView& = default;

    auto begin() const -> const byte* { return data_; }
    auto end() const -> const byte* { return data_ + size_; }
    auto data() -> byte* { return const_cast<byte*>(data_); }
    auto data() const -> const byte* { return data_; }
    auto size() const -> size_t { return size_; }
    operator asio::const_buffer() const noexcept { return asio::const_buffer(data_, size_); }

private:
    const byte* data_;
    size_t      size_;
};

class IOBuf {
public:
    IOBuf(size_t cap) { reserve(cap); }
    IOBuf(const IOBuf&)                    = delete;
    auto operator=(const IOBuf&) -> IOBuf& = delete;

    IOBuf(IOBuf&& other) noexcept { move_from(std::move(other)); }
    auto operator=(IOBuf&& other) noexcept -> IOBuf& {
        if (this != &other) {
            release_owned_buffer();
            move_from(std::move(other));
        }
        return *this;
    }

    ~IOBuf() { release_owned_buffer(); }

    auto defrag() -> void {
        if (r_ == 0) {
            return;
        }

        if (w_ > r_) {
            memmove(buf_, buf_ + r_, w_ - r_);
            w_ -= r_;
            r_ = 0;
        } else {
            w_ = r_ = 0;
        }
    }

    auto write_view() -> BufView { return BufView{buf_ + w_, cap_ - w_}; }
    auto write_size() const -> size_t { return cap_ - w_; }
    auto commit(size_t n) -> void { w_ += n; }

    auto read_view() -> BufView { return BufView{buf_ + r_, w_ - r_}; }
    auto buffered() const -> size_t { return w_ - r_; }
    auto consume(size_t n) -> void { r_ += n; }

    auto capacity() const -> size_t { return cap_; }
    auto empty() const -> bool { return r_ == w_; }
    auto clear() -> void {
        r_ = 0;
        w_ = 0;
    }

    auto data() -> byte* { return buf_; }
    auto data() const -> const byte* { return buf_; }

    auto reserve(size_t sz) -> void {
        if (sz <= cap_)
            return;

        sz       = std::bit_ceil(sz);
        byte* nb = new (std::align_val_t{alignment_}) byte[sz];
        if (buf_) {
            if (w_ > r_) {
                memcpy(nb, buf_ + r_, w_ - r_);
                w_ -= r_;
                r_ = 0;
            } else {
                w_ = r_ = 0;
            }
            ::operator delete[](buf_, std::align_val_t{alignment_});
        }

        buf_         = nb;
        cap_         = sz;
    }

private:
    auto release_owned_buffer() -> void {
        if (buf_) {
            ::operator delete[](buf_, std::align_val_t{alignment_});
        }

        buf_         = nullptr;
        cap_         = 0;
        r_           = 0;
        w_           = 0;
    }

    auto move_from(IOBuf&& other) -> void {
        cap_         = std::exchange(other.cap_, 0);
        buf_         = std::exchange(other.buf_, nullptr);
        alignment_   = other.alignment_;
        r_           = std::exchange(other.r_, 0);
        w_           = std::exchange(other.w_, 0);
    }

    size_t cap_{0};
    byte*  buf_{nullptr};
    size_t alignment_{8};
    size_t r_{0}, w_{0};
};

class Reader {
public:
    Reader(size_t cap) : buf_(cap) {}

    auto read_line_view() noexcept -> asio::awaitable<ResultT<std::string_view>>;
    auto read_bytes_to(byte* buf, size_t len) noexcept -> asio::awaitable<ResultT<std::monostate>>;

    auto fill() -> asio::awaitable<std::error_code>;
    auto has_more() -> bool;
    auto claer() -> void { buf_.clear(); }

    virtual ~Reader() = default;
protected:
    virtual auto read_impl(byte* buf, size_t size) noexcept -> asio::awaitable<ResultT<size_t>> = 0;
    virtual auto readv_impl(const std::vector<Buf>& bufs) noexcept -> asio::awaitable<ResultT<size_t>> = 0;

private:
    IOBuf buf_;
    std::vector<Buf> bufs_;
};

class Writer {
public:
    Writer(size_t cap) : buf_(cap) {}

    // Copy small pieces into the internal buffer and append a single owned
    // slice to vecs_. When the current buffer cannot fit the whole packet,
    // this coroutine flushes pending output before growing the buffer.
    template <typename... Ts>
    auto write_pieces(Ts&&... pieces) -> asio::awaitable<std::error_code>;

    // caller should ensure that s is valid until the next flush.
    auto write(std::string_view s) -> asio::awaitable<std::error_code>;

    // Flush all queued slices, including both owned buffer slices and
    // externally referenced views added via write().
    auto flush() -> asio::awaitable<std::error_code>;

    auto clear() -> void { reset_write_state(); }

    virtual ~Writer() = default;
protected:
    virtual auto write_impl(const byte* data, size_t size) noexcept
        -> asio::awaitable<ResultT<size_t>> = 0;
    virtual auto writev_impl(const std::vector<BufView>& bufs) noexcept
        -> asio::awaitable<ResultT<size_t>> = 0;

private:
    auto reset_write_state() -> void;
    // Grow the owned buffer on demand. The new capacity is at least 2x the
    // current one, but never exceeds kMaxBufferSize.
    // auto reserve_buf(size_t required_piece_size) -> std::error_code;
    auto write_piece_size(std::string_view piece) const -> size_t;

    template <typename T>
        requires(std::integral<std::remove_cvref_t<T>> &&
                 !std::same_as<std::remove_cvref_t<T>, bool>)
    auto write_piece_size(T value) const -> size_t;

    auto write_piece(byte*& out, std::string_view piece) -> void;

    template <typename T>
        requires(std::integral<std::remove_cvref_t<T>> &&
                 !std::same_as<std::remove_cvref_t<T>, bool>)
    auto write_piece(byte*& out, T value) -> void;

    IOBuf buf_;
    std::vector<BufView> vecs_;
    size_t queued_size_{0};
    std::chrono::high_resolution_clock::time_point last_ = std::chrono::high_resolution_clock::now();
};

template <typename... Ts>
auto Writer::write_pieces(Ts&&... pieces) -> asio::awaitable<std::error_code> {
    const size_t total_size =
        (size_t{0} + ... + write_piece_size(std::forward<Ts>(pieces)));

    if (total_size == 0) {
        co_return std::error_code{};
    }

    CHECK_LT(total_size, kDefaultWriteBufferSize);

    if (buf_.write_size() < total_size || queued_size_ + total_size >= kMaxReplyFlushBytes) {
        auto ec = co_await flush();
        if (ec) {
            co_return ec;
        }
    }

    const size_t offset = buf_.buffered();
    byte* begin = buf_.data() + offset;
    byte* out   = begin;
    (write_piece(out, std::forward<Ts>(pieces)), ...);

    queued_size_ += total_size;
    buf_.commit(total_size);
    vecs_.emplace_back(begin, total_size);
    co_return std::error_code{};
}

inline auto Writer::write_piece_size(std::string_view piece) const -> size_t { return piece.size(); }

template <typename T>
    requires(std::integral<std::remove_cvref_t<T>> &&
             !std::same_as<std::remove_cvref_t<T>, bool>)
auto Writer::write_piece_size(T value) const -> size_t {
    using Value = std::remove_cvref_t<T>;

    char tmp[std::numeric_limits<Value>::digits10 + 3];
    auto [ptr, ec] = std::to_chars(tmp, tmp + sizeof(tmp), value);
    (void)ec;
    return static_cast<size_t>(ptr - tmp);
}

inline auto Writer::write_piece(byte*& out, std::string_view piece) -> void {
    if (!piece.empty()) {
        std::memcpy(out, piece.data(), piece.size());
        out += piece.size();
    }
}

template <typename T>
    requires(std::integral<std::remove_cvref_t<T>> &&
             !std::same_as<std::remove_cvref_t<T>, bool>)
auto Writer::write_piece(byte*& out, T value) -> void {
    using Value = std::remove_cvref_t<T>;

    char tmp[std::numeric_limits<Value>::digits10 + 3];
    auto [ptr, ec] = std::to_chars(tmp, tmp + sizeof(tmp), value);
    (void)ec;

    const size_t size = static_cast<size_t>(ptr - tmp);
    std::memcpy(out, tmp, size);
    out += size;
}

class ParserResut {
public:
    enum Status : uint8_t {
        OK,
        HAS_MORE,
        STD_ERROR,
        WRONG_TYPE_ERROR,
        PROTOCOL_ERROR,
    };

    ParserResut(Status s, std::vector<std::string>&& args)
        : s_(s), args_(std::move(args)) {}
    ParserResut(Status s, std::string&& msg)
        : s_(s), err_msg_(std::move(msg)) {}
    ParserResut(std::error_code ec)
        : s_(Status::STD_ERROR), err_msg_(ec.message()), ec_(ec) {}

    auto ok() const -> bool { return s_ == Status::OK || s_ == Status::HAS_MORE; }

    auto value() -> std::vector<std::string>& { return args_; }

    auto message() const -> const std::string& { return err_msg_; }

    auto error_code() const -> std::error_code { return ec_; }

    auto operator==(Status s) const -> bool { return s == s_; }

private:
    Status                   s_;
    std::string              err_msg_;
    std::error_code          ec_;
    std::vector<std::string> args_;
};

class Parser {
public:
    Parser(Reader* rd) : rd_(rd) {}

    // parse a Redis command
    auto parse_one() noexcept -> asio::awaitable<ParserResut>;

    auto clear() -> void { rd_->claer(); }

private:
    Reader* rd_;
};

class Sender {
public:
    Sender(Writer* wr) : wr_(wr) {}

    auto send_simple_string(std::string_view s) -> asio::awaitable<void>;
    auto send_ok() -> asio::awaitable<void>;
    auto send_pong() -> asio::awaitable<void>;
    auto send_bulk_string(std::string_view s) -> asio::awaitable<void>;
    auto send_null_bulk_string() -> asio::awaitable<void>;
    auto send_integer(int64_t value) -> asio::awaitable<void>;
    auto send_error(std::string_view s) -> asio::awaitable<void>;

    auto flush() -> asio::awaitable<void>;

    auto get_error() const -> std::error_code { return ec_; }

    auto clear() -> void {
        ec_                            = std::error_code{};
        wr_->clear();
    }

private:
    std::error_code ec_;

    Writer* wr_;
};

} // namespace idlekv
