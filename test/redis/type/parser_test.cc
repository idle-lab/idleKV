#include "redis/protocol/parser.h"

#include <asio/asio.hpp>
#include <asiochan/channel.hpp>
#include <gtest/gtest.h>
#include <stdexcept>
#include <vector>

using namespace idlekv;

auto run_test(std::vector<Payload>                                             msgs,
              std::function<asio::awaitable<void>(asiochan::channel<Payload>)> f) {
    asio::io_context           io;
    asiochan::channel<Payload> in;

    asio::co_spawn(
        io,
        [in, &msgs]() mutable -> asio::awaitable<void> {
            for (auto& msg : msgs) {
                co_await in.write(std::move(msg));
            }
        },
        asio::detached);

    asio::co_spawn(io, f(in), asio::detached);

    io.run();
}

TEST(ParserTest, ReadLine) {
    std::vector<std::string> res;

    std::vector<Payload> msgs{
        {"1111", false},
        {"222\r3333\r\n", false},
        {"+OK\r\n", false},
        {"$5\r\nhello\r\n", false},
    };

    run_test(msgs, [&res](asiochan::channel<Payload> in) mutable -> asio::awaitable<void> {
        auto d = Parser{in};
        res.push_back(co_await d.read_line());
        res.push_back(co_await d.read_line());
        res.push_back(co_await d.read_line());
        res.push_back(co_await d.read_line());
    });

    ASSERT_EQ(res[0], "1111222\r3333\r\n");
    ASSERT_EQ(res[1], "+OK\r\n");
    ASSERT_EQ(res[2], "$5\r\n");
    ASSERT_EQ(res[3], "hello\r\n");
}

TEST(ParserTest, ReadBytes) {
    std::vector<std::string> res;

    std::vector<Payload> msgs{
        {"1111", false},
        {"222\r3333\r\n", false},
    };

    run_test(msgs, [&res](asiochan::channel<Payload> in) mutable -> asio::awaitable<void> {
        auto d = Parser{in};
        res.push_back(co_await d.read_bytes(2));
        res.push_back(co_await d.read_bytes(5));
        res.push_back(co_await d.read_bytes(3));
        res.push_back(co_await d.read_line());
    });

    ASSERT_EQ(res[0], "11");
    ASSERT_EQ(res[1], "11222");
    ASSERT_EQ(res[2], "\r33");
    ASSERT_EQ(res[3], "33\r\n");
}

TEST(ParserTest, ParseOneSimple) {
    std::vector<std::vector<std::string>> res;

    std::vector<Payload> msgs{
        {"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n", false},
    };

    run_test(msgs, [&res](asiochan::channel<Payload> in) mutable -> asio::awaitable<void> {
        Parser d{in};
        res.push_back(co_await d.parse_one());
    });

    ASSERT_EQ(res.size(), 1);
    ASSERT_EQ(res[0].size(), 2);
    ASSERT_EQ(res[0][0], "GET");
    ASSERT_EQ(res[0][1], "key");
}

TEST(ParserTest, ParseOneFragmented) {
    std::vector<std::vector<std::string>> res;

    std::vector<Payload> msgs{
        {"*2\r\n$3\r\nG", false},
        {"ET\r\n$3\r", false},
        {"\nkey\r\n", false},
    };

    run_test(msgs, [&res](asiochan::channel<Payload> in) mutable -> asio::awaitable<void> {
        Parser d{in};
        res.push_back(co_await d.parse_one());
    });

    ASSERT_EQ(res.size(), 1);
    ASSERT_EQ(res[0].size(), 2);
    ASSERT_EQ(res[0][0], "GET");
    ASSERT_EQ(res[0][1], "key");
}

TEST(ParserTest, ParseMultipleCommands) {
    std::vector<std::vector<std::string>> res;

    std::vector<Payload> msgs{
        {
            "*1\r\n$4\r\nPING\r\n"
            "*2\r\n$3\r\nSET\r\n$3\r\nkey\r\n",
            false
        },
    };

    run_test(msgs, [&res](asiochan::channel<Payload> in) mutable -> asio::awaitable<void> {
        Parser d{in};
        res.push_back(co_await d.parse_one());
        res.push_back(co_await d.parse_one());
    });

    ASSERT_EQ(res.size(), 2);

    ASSERT_EQ(res[0].size(), 1);
    ASSERT_EQ(res[0][0], "PING");

    ASSERT_EQ(res[1].size(), 2);
    ASSERT_EQ(res[1][0], "SET");
    ASSERT_EQ(res[1][1], "key");
}

TEST(ParserTest, ParseEmptyArray) {
    std::vector<std::vector<std::string>> res;

    std::vector<Payload> msgs{
        {"*0\r\n", false},
    };

    run_test(msgs, [&res](asiochan::channel<Payload> in) mutable -> asio::awaitable<void> {
        Parser d{in};
        res.push_back(co_await d.parse_one());
    });

    ASSERT_EQ(res.size(), 1);
    ASSERT_EQ(res[0].size(), 0);
}

TEST(ParserTest, ParseNullBulkString) {
    std::vector<std::vector<std::string>> res;

    std::vector<Payload> msgs{
        {"*1\r\n$-1\r\n", false},
    };

    run_test(msgs, [&res](asiochan::channel<Payload> in) mutable -> asio::awaitable<void> {
        Parser d{in};
        res.push_back(co_await d.parse_one());
    });

    ASSERT_EQ(res.size(), 1);
    ASSERT_EQ(res[0].size(), 1);
    ASSERT_TRUE(res[0][0].empty());
}

TEST(ParserTest, ParseIllegalHeader) {
    std::vector<Payload> msgs{
        {"$3\r\nSET\r\n", false},
    };


    run_test(msgs,
    [&](asiochan::channel<Payload> in) -> asio::awaitable<void> {
        Parser d{in};
        EXPECT_THROW(co_await d.parse_one(), std::runtime_error);
    });
}
