#include "redis/protocol/parser.h"

#include "redis/protocol/reply.h"

#include <asio/awaitable.hpp>
#include <charconv>
#include <ranges>
#include <stdexcept>
#include <string>
#include <system_error>
#include <vector>

namespace idlekv {

// ½öÓÃÓÚ debug
std::string escape_string(const std::string& s) {
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

auto Parser::parse_one() -> asio::awaitable<std::vector<std::string>> {
    std::string header = co_await rd_->read_line();

    if (header[0] != static_cast<char>(DataType::Arrays)) [[unlikely]] {
        throw std::runtime_error("illegal array header");
    }
    int arrLen;
    auto [ptr, ec] =
        std::from_chars(header.c_str() + 1, header.c_str() + header.size() - 2, arrLen);

    if (ec != std::errc()) [[unlikely]] {
        throw std::runtime_error(std::make_error_code(ec).message());
    }

    std::vector<std::string> args(arrLen);

    for (auto i : std::views::iota(0, arrLen)) {
        std::string line = co_await rd_->read_line();

        if (line.size() < 4 || line[0] != static_cast<char>(DataType::BulkString)) [[unlikely]] {
            throw std::runtime_error("illegal bulk string header");
        }

        int strLen;
        auto [ptr, ec] = std::from_chars(line.c_str() + 1,
                                         line.c_str() + line.size() - 2 /* exclude CRLF */, strLen);
        if (ec != std::errc()) [[unlikely]] {
            throw std::runtime_error(std::make_error_code(ec).message());
        }

        if (strLen == -1) {
            continue;
        }

        args[i] = co_await rd_->read_bytes(strLen + 2 /* include CRLF */);

        // pop CRLF
        args[i].pop_back();
        args[i].pop_back();
    }

    co_return args;
}

} // namespace idlekv
