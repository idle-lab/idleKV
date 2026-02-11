#include "redis/protocol/parser.h"

#include "redis/protocol/types.h"

#include <asio/awaitable.hpp>
#include <charconv>
#include <iomanip>
#include <ranges>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>
#include <iostream>

namespace idlekv {

auto Parser::read_line() -> asio::awaitable<std::string> {
    std::string line;

    // 如果缓冲区中没有完整的一行数据，则直接从 in_ 中读取，直到读完一整行
    if (size_t len = buffer_find('\n'); len != std::string::npos) {
        line = buffer_get(len);
    } else {
        line = buffer_.substr(r_);
        r_    = 0;
        buffer_.clear();

        while (true) {
            auto [chunk, done] = co_await in_.read();
            if (done) {
                throw std::runtime_error("Connection closed");
            }

            if (size_t p = chunk.find('\n'); p != std::string::npos) {
                buffer_ = chunk.substr(p + 1);
                line += chunk.substr(0, p + 1);

                break;
            } else {
                line += chunk;
            }
        }
    }

    co_return line;
}


auto Parser::read_bytes(size_t len) -> asio::awaitable<std::string> {
    while (buffer_size() < len) {
        co_await buffer_fill();
    }

    co_return buffer_get(len);
}

std::string escape_string(const std::string& s) {
    std::string out;
    out.reserve(s.size() * 2);

    for (unsigned char c : s) {
        switch (c) {
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            case '\0': out += "\\0"; break;
            case '\\': out += "\\\\"; break;
            case '\"': out += "\\\""; break;
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


auto Parser::parse_one() -> asio::awaitable<std::vector<std::string>> {
    std::string header = co_await read_line();

    if (header[0] != static_cast<char>(DataType::Arrays)) [[unlikely]] {
        throw std::runtime_error("illegal array header");
    }

    int arrLen;
    auto [ptr, ec] = std::from_chars(
        header.c_str() + 1, header.c_str() + header.size() - 2, arrLen);
    
    if (ec != std::errc()) [[unlikely]] {
        throw std::runtime_error(std::make_error_code(ec).message());
    }


    std::vector<std::string> args(arrLen);

    for (auto i : std::views::iota(0, arrLen)) {
        std::string line = co_await read_line();

        if (line.size() < 4 || line[0] != static_cast<char>(DataType::BulkString)) [[unlikely]] {
            throw std::runtime_error("illegal bulk string header");
        }

        int strLen;
        auto [ptr, ec] = std::from_chars(
            line.c_str() + 1, line.c_str() + line.size() - 2 /* exclude CRLF */, strLen);
        if (ec != std::errc()) [[unlikely]] {
            throw std::runtime_error(std::make_error_code(ec).message());
        }

        if (strLen == -1) {
            continue;
        }

        args[i] = co_await read_bytes(strLen + 2 /* include CRLF */);

        // pop CRLF
        args[i].pop_back();
        args[i].pop_back();
    }

    co_return args;
}

} // namespace idlekv
