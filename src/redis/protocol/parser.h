#pragma once

#include <asio/asio.hpp>
#include <asio/awaitable.hpp>
#include <asiochan/asiochan.hpp>
#include <cstdint>
#include <string>
#include <vector>

namespace idlekv {

struct Payload {
    std::string msg;
    bool        done;

    Payload(std::string&& m, bool d) : msg(std::move(m)), done(d) {}

    Payload(std::string& m, bool d) : msg(std::move(m)), done(d) {}
};

class Parser {
public:
    Parser(asiochan::read_channel<Payload> in) : in_(in) {}

    auto read_line() -> asio::awaitable<std::string>;

    auto read_bytes(size_t len) -> asio::awaitable<std::string>;

    // 解析一条 redis 指令
    auto parse_one() -> asio::awaitable<std::vector<std::string>>;

private:
    asio::awaitable<char> buffer_fill() {
        if (r_ != 0) {
            buffer_ = buffer_.substr(r_);
            r_       = 0;
        }

        auto [chunk, done] = co_await in_.read();
        if (done) {
            throw std::runtime_error("Connection closed");
        }

        buffer_ += chunk;
    }

    size_t buffer_size() const noexcept { return buffer_.size() - r_; }

    // 返回从 r 开始到第一个 c （包含 c）的字符串的长度，如果没有找到则返回 npos
    size_t buffer_find(char c) const noexcept {
        if (size_t pos = buffer_.find(c, r_); pos != std::string::npos) {
            return pos - r_ + 1;
        } else {
            return std::string::npos;
        }
    }

    // 获取从 r 开始的 len 个字符
    std::string buffer_get(size_t len) noexcept {
        assert(r_ + len <= buffer_.size());

        r_ += len;
        return buffer_.substr(r_ - len, len);
    }

    // 第一个还未读取的字符在 buffer_ 中的位置
    size_t                          r_ = 0;
    std::string                     buffer_;
    asiochan::read_channel<Payload> in_;
};

} // namespace idlekv
