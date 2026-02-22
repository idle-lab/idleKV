#include "redis/connection.h"
#include "common/logger.h"

#include <algorithm>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

namespace idlekv {

auto Connection::fill() noexcept -> asio::awaitable<std::error_code> {
    if (r_ > 0 && r_ != w_) {
        std::memmove(buffer_, buffer_ + r_, w_ - r_);
        w_ -= r_;
        r_ = 0;
    }

    auto [ec, n] =
        co_await socket_.async_read_some(asio::buffer(buffer_ + w_, defaultBufferSize - w_),
                                         asio::as_tuple(asio::use_awaitable));
    if (ec) {
        ec_.emplace(ec);
        co_return ec;
    }

    w_ += n;

    co_return std::error_code();
}

auto Connection::read_line() noexcept -> asio::awaitable<Payload> {
    std::string line;

    for (;;) {
        auto pos = std::find(buffer_ + r_, buffer_ + w_, '\n');
        if (pos == buffer_ + w_) {
            line += std::string_view(buffer_ + r_, pos);
            buffer_clear();
            auto ec = co_await fill();

            if (ec != std::errc()) {
                co_return std::make_pair("", ec);
            }

            continue;
        }

        line += std::string_view(buffer_ + r_, pos + 1 /* include '\n' */);
        r_ += line.size();
        co_return std::make_pair(line, std::error_code());
    }
}

auto Connection::read_bytes(size_t len) noexcept -> asio::awaitable<Payload> {
    std::string data;

    for (;;) {
        data += std::string_view(buffer_ + r_, std::min(w_ - r_, len));
        if (w_ - r_ >= len) {
            r_ += len;
            co_return std::make_pair(data, std::error_code());
        } else {
            len -= (w_ - r_);
            buffer_clear();
            auto ec = co_await fill();
            if (ec != std::errc()) {
                co_return std::make_pair("", ec);
            }
        }
    }
}

auto Connection::write(const std::string& reply) noexcept -> asio::awaitable<std::error_code> {
    auto [ec, n] = co_await asio::async_write(socket_, asio::buffer(reply),
                                              asio::as_tuple(asio::use_awaitable));
    if (ec) {
        ec_.emplace(ec);
        co_return ec;
    }
    co_return std::error_code();
}

} // namespace idlekv
