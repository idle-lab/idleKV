#pragma once

#include <asio/asio.hpp>
#include <asio/awaitable.hpp>
#include <asiochan/asiochan.hpp>
#include <memory>
#include <string>
#include <vector>

namespace idlekv {

class Reader {
public:
    virtual auto read_line() -> asio::awaitable<std::string> = 0;

    virtual auto read_bytes(size_t len) -> asio::awaitable<std::string> = 0;
};

class Parser {
public:
    Parser(std::shared_ptr<Reader> rd) : rd_(rd) {}

    // 解析一条 redis 指令
    auto parse_one() -> asio::awaitable<std::vector<std::string>>;

private:


    // 第一个还未读取的字符在 buffer_ 中的位置
    size_t                          r_ = 0;
    std::string                     buffer_;
    std::shared_ptr<Reader> rd_;
};

} // namespace idlekv
