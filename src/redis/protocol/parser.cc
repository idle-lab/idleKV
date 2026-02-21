#include "redis/protocol/parser.h"

#include "redis/protocol/reply.h"

#include <asio/awaitable.hpp>
#include <charconv>
#include <memory>
#include <ranges>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

namespace idlekv {

// just for debug
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

auto Parser::parse_one() noexcept -> asio::awaitable<Result> {
    auto [header, ec] = co_await rd_->read_line();
    if (ec != std::errc()) {
        co_return Result{std::vector<std::string>{}, std::make_unique<StandardErr>(ec)};
    }

    if (header[0] != static_cast<char>(DataType::Arrays)) [[unlikely]] {
        co_return Result{std::vector<std::string>{}, std::make_unique<WrongTypeErr>()};
    }

    int arrLen;
    auto [ptr, err] =
        std::from_chars(header.c_str() + 1, header.c_str() + header.size() - 2, arrLen);
    if (err != std::errc()) [[unlikely]] {
        co_return Result{std::vector<std::string>{},
                         std::make_unique<ProtocolErr>(std::make_error_code(err).message())};
    }

    std::vector<std::string> args(arrLen);

    for (auto i : std::views::iota(0, arrLen)) {
        auto [line, ec] = co_await rd_->read_line();
        if (ec != std::errc()) {
            co_return Result{std::vector<std::string>{}, std::make_unique<StandardErr>(ec)};
        }

        if (line.size() < 4 || line[0] != static_cast<char>(DataType::BulkString)) [[unlikely]] {
            co_return Result{std::vector<std::string>{}, std::make_unique<WrongTypeErr>()};
        }

        int strLen;
        auto [ptr, err] = std::from_chars(
            line.c_str() + 1, line.c_str() + line.size() - 2 /* exclude CRLF */, strLen);
        if (err != std::errc()) [[unlikely]] {
            co_return Result{std::vector<std::string>{},
                             std::make_unique<ProtocolErr>(std::make_error_code(err).message())};
        }

        // empty bulk string
        if (strLen == -1) {
            continue;
        }

        auto [data, ec0] = co_await rd_->read_bytes(strLen + 2 /* include CRLF */);
        if (ec0 != std::errc()) {
            co_return Result{std::vector<std::string>{}, std::make_unique<StandardErr>(ec0)};
        }

        args[i] = data;

        // pop CRLF
        args[i].pop_back();
        args[i].pop_back();
    }

    co_return std::make_pair(std::move(args), nullptr);
}

} // namespace idlekv
