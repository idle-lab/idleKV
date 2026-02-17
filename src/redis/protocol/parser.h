#pragma once

#include "redis/protocol/error.h"

#include <asio/asio.hpp>
#include <asio/awaitable.hpp>
#include <asiochan/asiochan.hpp>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace idlekv {

class Reader {
public:
    using Payload = std::pair<std::string, std::error_code>;

    virtual auto read_line() noexcept -> asio::awaitable<Payload> = 0;

    virtual auto read_bytes(size_t len) noexcept -> asio::awaitable<Payload> = 0;
};

class Parser {
public:
    using Result = std::pair<std::vector<std::string>, std::unique_ptr<Err>>;

    Parser(std::shared_ptr<Reader> rd) : rd_(rd) {}

    // 解析一条 redis 指令
    auto parse_one() noexcept -> asio::awaitable<Result>;

private:
    // 第一个还未读取的字符在 buffer_ 中的位置
    size_t                  r_ = 0;
    std::string             buffer_;
    std::shared_ptr<Reader> rd_;
};

} // namespace idlekv
