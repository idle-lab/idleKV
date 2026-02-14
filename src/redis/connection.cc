#include "redis/connection.h"

#include <algorithm>
#include <string>
#include <string_view>

namespace idlekv {

auto Connection::fill() -> asio::awaitable<void> {
    if (r_ > 0 && r_ != w_) {
        std::memmove(buffer_, buffer_ + r_, w_ - r_);
        w_ -= r_;
        r_ = 0;
    }

    auto [ec, n] =
        co_await socket_.async_read_some(asio::buffer(buffer_ + w_, defaultBufferSize - w_ + 1),
                                         asio::as_tuple(asio::use_awaitable));
    if (ec) {
        ec_.emplace(ec);
        if (!(ec == asio::error::eof || ec == asio::error::connection_reset ||
              ec == asio::error::operation_aborted)) {
            throw std::system_error(ec);
        }

        co_return;
    }

    w_ += n;
}

auto Connection::read_line() -> asio::awaitable<std::string> {
    std::string line;

    for (;;) {
        auto pos = std::find(buffer_ + r_, buffer_ + w_, '\n');
        if (pos == buffer_ + w_) {
            line += std::string_view(buffer_ + r_, pos);
            buffer_clear();
            co_await fill();

            continue;
        }

        line += std::string_view(buffer_ + r_, pos + 1 /* include '\n' */);
        co_return line;
    }
}

auto Connection::read_bytes(size_t len) -> asio::awaitable<std::string> {
    std::string data;

    for (;;) {
        data += std::string_view(buffer_ + r_, std::min(w_ - r_, len));
        if (w_ - r_ >= len) {
            r_ += len;
            co_return data;
        } else {
            len -= (w_ - r_);
            buffer_clear();
            co_await fill();
        }
    }
}

} // namespace idlekv
