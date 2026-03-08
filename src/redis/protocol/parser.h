#pragma once

#include "common/config.h"
#include "common/result.h"

#include <asio/asio.hpp>
#include <asio/awaitable.hpp>
#include <asiochan/asiochan.hpp>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

namespace idlekv {

constexpr size_t kDefaultReadBufferSize = 2048;
constexpr size_t kMaxBufferSize         = 8192;
using byte                              = char;

class BufView {
public:
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

    auto write_view() -> BufView { return BufView{buf_ + w_, cap_ - w_}; }

    auto write_size() -> size_t { return cap_ - w_; }

    auto commit(size_t n) -> void { w_ += n; }

    auto read_view() -> BufView { return BufView{buf_ + r_, w_ - r_}; }

    auto buffered() const -> bool { return w_ - r_; }

    auto consume(size_t n) -> void { r_ += n; }

    auto capacity() const -> size_t { return cap_; }

    auto empty() const -> bool { return r_ == w_; }

    auto clear() -> void {
        r_ = 0;
        w_ = 0;
    }

    auto reserve(size_t sz) -> void {
        if (sz < cap_)
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

        buf_ = nb;
        cap_ = sz;
    }

private:
    size_t cap_{0};
    byte*  buf_{nullptr};
    size_t alignment_{8};
    size_t r_{0}, w_{0};
};

class Reader {
public:
    Reader(size_t cap) : buf_(cap) {}

    // return a single line with '\n'
    auto read_line() noexcept -> asio::awaitable<ResultT<std::string>>;

    auto read_bytes(size_t len) noexcept -> asio::awaitable<ResultT<std::string>>;

    auto fill() -> asio::awaitable<std::error_code>;

    auto has_more() -> bool;

    auto claer() -> void {
        buf_.clear();
    }

protected:
    virtual auto read_impl(byte* buf, size_t size) noexcept -> asio::awaitable<ResultT<size_t>> = 0;

private:
    IOBuf buf_;
};

class Writer {
public:
    Writer(size_t cap) : buf_(cap) {}

    // template <typename... Ts>
    // auto write_pieces(Ts&&... pieces) -> asio::awaitable<void>;    // Copy pieces into buffer and
    // reference buffer auto write_ref(std::string_view s) -> asio::awaitable<void>;

    auto write(std::string_view s) -> void;

    auto flush() -> asio::awaitable<std::error_code>;

    auto clear() -> void {
        buf_.clear();
        vecs_.clear();
    }

protected:
    virtual auto write_impl(const byte* data, size_t size) noexcept
        -> asio::awaitable<ResultT<size_t>> = 0;

    virtual auto writev_impl(const std::vector<BufView>& bufs) noexcept
        -> asio::awaitable<ResultT<size_t>> = 0;

private:
    auto reserve_buf(size_t expected_buffer_cap) -> void;

    IOBuf                buf_;
    std::vector<BufView> vecs_;
};

class ParserResut {
public:
    enum Status : uint8_t {
        OK,
        HAS_MORE,
        STD_ERROR,
        WRONG_TYPE_ERROR,
        PROTOCOL_ERROR,
    };

    ParserResut(Status s, std::vector<std::string>&& args) : s_(s), args_(std::move(args)) {}
    ParserResut(Status s, std::string&& msg) : s_(s), err_msg_(std::move(msg)) {}
    ParserResut(std::error_code ec) : s_(Status::STD_ERROR), err_msg_(ec.message()), ec_(ec) {}

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

    auto clear() -> void {
        rd_->claer();
    }

private:
    Reader* rd_;
};

constexpr auto kMaxReplyFlushCount = 64;
constexpr auto kMaxReplyFlushBytes = 32 * KB;

class Sender {
public:
    Sender(Writer* wr) : wr_(wr) { }

    auto send(std::string&&) -> asio::awaitable<void>;

    auto flush() -> asio::awaitable<void>;

    auto should_flush() -> bool;

    auto get_error() -> std::error_code { return ec_; }

    auto set_mode(bool batched) -> void { batched_ = batched; }

    auto clear() -> void {
        batched_reply_.clear();
        batched_size_ = batched_count_ = 0;
        ec_ = std::error_code{};
        wr_->clear();
    }

private:
    // hold the ownership of reply
    std::deque<std::string> batched_reply_;
    size_t                  batched_size_{0}, batched_count_{0};

    bool batched_{true};

    std::error_code ec_;

    Writer* wr_;
};

} // namespace idlekv
