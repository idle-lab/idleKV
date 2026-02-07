#pragma once

#include <asio/asio.hpp>
#include <asiochan/asiochan.hpp>
#include <string>

namespace idlekv {

enum class DataType : char {
    String     = '+',
    Error      = '-',
    Integers   = ':',
    BulkString = '$',
    Arrays     = '*'
};

extern const char* CRLF;

struct Payload {
    std::string msg;
    bool        done;

    Payload(std::string&& m, bool d) : msg(std::move(m)), done(d) {}

    Payload(std::string& m, bool d) : msg(std::move(m)), done(d) {}
};

class Encoder {
public:

private:
    asiochan::write_channel<Payload> out_;
};

class Decoder {
public:
    Decoder(asiochan::read_channel<Payload> in) : in_(in) {}

    auto read_line() -> asio::awaitable<std::string>;

    auto read_bytes(size_t len) -> asio::awaitable<std::string>;

private:
    asio::awaitable<char> buffer_fill() {
        if (r != 0) {
            buffer_ = buffer_.substr(r);
            r       = 0;
        }

        auto [chunk, done] = co_await in_.read();
        if (done) {
            throw std::runtime_error("Connection closed");
        }

        buffer_ += chunk;
    }

    size_t buffer_size() const noexcept { return buffer_.size() - r; }

    // 返回从 r 开始到第一个 c （包含 c）的字符串的长度，如果没有找到则返回 npos
    size_t buffer_find(char c) const noexcept {
        if (size_t pos = buffer_.find(c, r); pos != std::string::npos) {
            return pos - r + 1;
        } else {
            return std::string::npos;
        }
    }

    // 获取从 r 开始的 len 个字符
    std::string buffer_get(size_t len) noexcept {
        assert(r + len <= buffer_.size());

        r += len;
        return buffer_.substr(r - len, len);
    }

    // 第一个还未读取的字符在 buffer_ 中的位置
    size_t                          r = 0;
    std::string                     buffer_;
    asiochan::read_channel<Payload> in_;
};

} // namespace idlekv



