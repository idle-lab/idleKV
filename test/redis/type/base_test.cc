#include <gtest/gtest.h>
#include <asio/asio.hpp>
#include <vector>
#include <redis/type/base.h>

using namespace idlekv;

auto run_test(std::vector<Payload>                                             msgs,
              std::function<asio::awaitable<void>(asiochan::channel<Payload>)> f) {
    asio::io_context io;
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

TEST(DecoderTest, ReadLine) {
    std::vector<std::string> res;

    std::vector<Payload> msgs{
        {"1111", false},
        {"222\r3333\r\n", false},
        {"+OK\r\n", false},
        {"$5\r\nhello\r\n", false},
    };

	run_test(msgs, [&res](asiochan::channel<Payload> in) mutable -> asio::awaitable<void> {
        auto d = Decoder{in};
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

TEST(DecoderTest, ReadBytes) {
    std::vector<std::string> res;

    std::vector<Payload> msgs{
        {"1111", false},
        {"222\r3333\r\n", false},
    };

	run_test(msgs, [&res](asiochan::channel<Payload> in) mutable -> asio::awaitable<void> {
        auto d = Decoder{in};
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