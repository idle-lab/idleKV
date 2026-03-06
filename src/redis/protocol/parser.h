#pragma once

#include "common/config.h"
#include "common/result.h"
#include "redis/protocol/error.h"
#include "redis/protocol/reply.h"

#include <asio/asio.hpp>
#include <asio/awaitable.hpp>
#include <asiochan/asiochan.hpp>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <stdatomic.h>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

namespace idlekv {

constexpr size_t kDefaultReadBufferSize = 2048;
constexpr size_t kDefaultWriteBufferSize = 4096;
using byte                         = char;

class IOBuf {
public:
    class BufView {
    public:
        BufView(byte* data, size_t size) : data_(data), size_(size) {}
        BufView(BufView&&) = default;
        auto operator=(BufView&&) -> BufView& = default;

        auto begin() const -> byte* { return data_; }
        auto end() const -> byte* { return data_ + size_; }
        auto data() const -> byte* { return data_; }
        auto size() const -> size_t { return size_; }
    private:
        byte*  data_;
        size_t size_;
    };

    IOBuf(size_t cap) : cap_(cap), buf_(new byte[cap]) {}

    auto defrag() -> void {
        if (r_ > 0) {
            if (empty()) {
                clear();
                return;
            }
            std::memmove(buf_, buf_ + r_, w_ - r_);
            w_ -= r_;
            r_ = 0;
        }
    }

    auto write_view() -> BufView {
        // defrag buf_ before return to free up more space
        defrag();
        return BufView{buf_ + w_, cap_ - w_}; 
    }

    auto commit(size_t n) -> void { w_ += n; }

    auto read_view() -> BufView { return BufView{buf_ + r_, w_ - r_}; }

    auto consume(size_t n) -> void { r_ += n; }

    auto capacity() const -> size_t { return cap_; }

    auto empty() const -> bool { return r_ == w_; }

    auto clear() -> void {
        r_ = 0;
        w_ = 0;
    }

private:
    size_t cap_;
    byte*  buf_;
    size_t r_{0}, w_{0};
};

class Reader {
public:
    Reader(size_t cap) : buf_(cap) {}

    // return a single line with '\n'
    auto read_line() noexcept -> asio::awaitable<ResultT<std::string>>;

    auto read_bytes(size_t len) noexcept -> asio::awaitable<ResultT<std::string>>;

    auto fill() -> asio::awaitable<std::error_code>;

protected:
    virtual auto read_impl(byte* buf, size_t size) noexcept -> asio::awaitable<ResultT<size_t>> = 0;

private:
    IOBuf buf_;
};

class Writer {
public:
    Writer(size_t cap) : buf_(cap) {}
    
    auto write(std::string_view s) -> asio::awaitable<std::error_code>;

    auto write(uint32_t n) -> asio::awaitable<std::error_code>;

    auto flush() -> asio::awaitable<std::error_code>;

protected:
    virtual auto write_impl(const byte* data, size_t size) noexcept
        -> asio::awaitable<ResultT<size_t>> = 0;

private:
    IOBuf buf_;
};

class Parser {
public:
    using Result = std::pair<std::vector<std::string>, std::unique_ptr<Err>>;

    Parser(Reader* rd) : rd_(rd) {}

    // 解析一条 redis 指令
    auto parse_one() noexcept -> asio::awaitable<Result>;

private:
    Reader* rd_;
};

constexpr size_t kMaxReplyFlushCount = 32;
constexpr auto kMaxReplyFlushInterval = std::chrono::microseconds(50); // us
constexpr size_t kMaxReplyFlushSize = 16 * KB;

class Sender {
public:
    Sender(Writer* wr) : wr_(wr) {}

    auto send_simple_string(std::string&& s) -> void;

    auto send_bulk_string(std::string&& s, size_t len) -> void;

    auto send_ok() -> void ;

    auto send_pong() -> void ;

    auto flush() -> asio::awaitable<std::error_code>;

    auto has_pending() -> bool;
    auto should_flush() -> bool;
private:
    std::queue<std::unique_ptr<Reply>> reply_batch_;
    std::chrono::steady_clock::time_point last_flushed_ = std::chrono::steady_clock::now();
    size_t size_{0}, reply_count_{0};

    Writer* wr_;
};

} // namespace idlekv
