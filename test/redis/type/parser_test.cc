#include "redis/protocol/parser.h"

#include <asio/asio.hpp>
#include <asio/co_spawn.hpp>
#include <asio/detached.hpp>
#include <asio/io_context.hpp>
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <system_error>
#include <vector>

using namespace idlekv;

class MockReader : public Reader {
public:
    explicit MockReader(const std::string& data) : data_(data) {}

    auto read_line() noexcept -> asio::awaitable<Payload> override { co_return read_line_impl(); }

    auto read_bytes(size_t len) noexcept -> asio::awaitable<Payload> override {
        co_return read_bytes_impl(len);
    }

    void set_data(const std::string& data) { data_ = data; }

    void reset() { pos_ = 0; }

private:
    auto read_line_impl() -> Payload {
        if (pos_ >= data_.size()) {
            return Payload{"", std::make_error_code(std::errc::no_message)};
        }
        size_t crlf_pos = data_.find("\r\n", pos_);
        if (crlf_pos == std::string::npos) {
            return Payload{"", std::make_error_code(std::errc::no_message)};
        }
        std::string line = data_.substr(pos_, crlf_pos - pos_ + 2);
        pos_             = crlf_pos + 2;
        return Payload{line, std::error_code()};
    }

    auto read_bytes_impl(size_t len) -> Payload {
        if (pos_ + len > data_.size()) {
            return Payload{"", std::make_error_code(std::errc::no_message)};
        }
        std::string bytes = data_.substr(pos_, len);
        pos_ += len;
        return Payload{bytes, std::error_code()};
    }

    std::string data_;
    size_t      pos_ = 0;
};

class ParserTest : public ::testing::Test {
protected:
    void SetUp() override {
        reader_ = std::make_shared<MockReader>("");
        parser_ = std::make_unique<Parser>(reader_);
    }

    auto parse(const std::string& data) -> std::vector<std::string> {
        reader_->set_data(data);
        reader_->reset();

        asio::io_context         ctx;
        std::vector<std::string> result;
        asio::co_spawn(
            ctx,
            [&]() -> asio::awaitable<void> {
                auto [args, err] = co_await parser_->parse_one();
                result           = std::move(args);
                co_return;
            },
            asio::detached);
        ctx.run();
        return result;
    }

    auto parse_and_expect_error(const std::string& data) -> bool {
        reader_->set_data(data);
        reader_->reset();

        asio::io_context ctx;
        bool             has_error = false;
        asio::co_spawn(
            ctx,
            [&]() -> asio::awaitable<void> {
                auto [args, err] = co_await parser_->parse_one();
                has_error        = (err != nullptr);
                co_return;
            },
            asio::detached);
        ctx.run();
        return has_error;
    }

    std::shared_ptr<MockReader> reader_;
    std::unique_ptr<Parser>     parser_;
};

TEST_F(ParserTest, ParseSimpleSetCommand) {
    std::string data   = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
    auto        result = parse(data);

    ASSERT_EQ(result.size(), 3);
    EXPECT_EQ(result[0], "SET");
    EXPECT_EQ(result[1], "key");
    EXPECT_EQ(result[2], "value");
}

TEST_F(ParserTest, ParseSimpleGetCommand) {
    std::string data   = "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
    auto        result = parse(data);

    ASSERT_EQ(result.size(), 2);
    EXPECT_EQ(result[0], "GET");
    EXPECT_EQ(result[1], "key");
}

TEST_F(ParserTest, ParseDelCommand) {
    std::string data   = "*2\r\n$3\r\nDEL\r\n$6\r\nmykey1\r\n";
    auto        result = parse(data);

    ASSERT_EQ(result.size(), 2);
    EXPECT_EQ(result[0], "DEL");
    EXPECT_EQ(result[1], "mykey1");
}

TEST_F(ParserTest, ParseMgetCommand) {
    std::string data   = "*4\r\n$4\r\nMGET\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n";
    auto        result = parse(data);

    ASSERT_EQ(result.size(), 4);
    EXPECT_EQ(result[0], "MGET");
    EXPECT_EQ(result[1], "key1");
    EXPECT_EQ(result[2], "key2");
    EXPECT_EQ(result[3], "key3");
}

TEST_F(ParserTest, ParseEmptyArray) {
    std::string data   = "*0\r\n";
    auto        result = parse(data);
    EXPECT_EQ(result.size(), 0);
}

TEST_F(ParserTest, ParseSingleElement) {
    std::string data   = "*1\r\n$4\r\nPING\r\n";
    auto        result = parse(data);

    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result[0], "PING");
}

TEST_F(ParserTest, ParseEmptyBulkString) {
    std::string data   = "*1\r\n$0\r\n\r\n";
    auto        result = parse(data);

    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result[0], "");
}

TEST_F(ParserTest, ParseNullBulkString) {
    std::string data   = "*1\r\n$-1\r\n";
    auto        result = parse(data);

    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result[0], "");
}

TEST_F(ParserTest, ParseMixedEmptyAndNormal) {
    std::string data   = "*3\r\n$4\r\nCMD1\r\n$0\r\n\r\n$4\r\ncmd2\r\n";
    auto        result = parse(data);

    ASSERT_EQ(result.size(), 3);
    EXPECT_EQ(result[0], "CMD1");
    EXPECT_EQ(result[1], "");
    EXPECT_EQ(result[2], "cmd2");
}

TEST_F(ParserTest, ParseSpecialCharacters) {
    std::string data   = "*2\r\n$3\r\nSET\r\n$5\r\nk:e:y\r\n";
    auto        result = parse(data);

    ASSERT_EQ(result.size(), 2);
    EXPECT_EQ(result[0], "SET");
    EXPECT_EQ(result[1], "k:e:y");
}

TEST_F(ParserTest, ParseBinaryData) {
    std::string data   = "*2\r\n$3\r\nSET\r\n$4\r\n\x01\x02\x03\x04\r\n";
    auto        result = parse(data);

    ASSERT_EQ(result.size(), 2);
    EXPECT_EQ(result[0], "SET");
    EXPECT_EQ(result[1].size(), 4);
    EXPECT_EQ(result[1][0], '\x01');
    EXPECT_EQ(result[1][1], '\x02');
    EXPECT_EQ(result[1][2], '\x03');
    EXPECT_EQ(result[1][3], '\x04');
}

TEST_F(ParserTest, ParseLongString) {
    std::string long_value(1000, 'a');
    std::string data   = "*2\r\n$3\r\nSET\r\n$1000\r\n" + long_value + "\r\n";
    auto        result = parse(data);

    ASSERT_EQ(result.size(), 2);
    EXPECT_EQ(result[0], "SET");
    EXPECT_EQ(result[1].size(), 1000);
    EXPECT_EQ(result[1], long_value);
}

TEST_F(ParserTest, ParseComplexCommand) {
    std::string data =
        "*5\r\n$4\r\nHSET\r\n$7\r\nmyhash1\r\n$5\r\nfield\r\n$5\r\nvalue\r\n$2\r\nex\r\n";
    auto result = parse(data);

    ASSERT_EQ(result.size(), 5);
    EXPECT_EQ(result[0], "HSET");
    EXPECT_EQ(result[1], "myhash1");
    EXPECT_EQ(result[2], "field");
    EXPECT_EQ(result[3], "value");
    EXPECT_EQ(result[4], "ex");
}

TEST_F(ParserTest, InvalidArrayHeader) {
    std::string data = "+OK\r\n";
    EXPECT_TRUE(parse_and_expect_error(data));
}

TEST_F(ParserTest, InvalidArrayHeaderNoAsterisk) {
    std::string data = "3\r\n$3\r\nSET\r\n";
    EXPECT_TRUE(parse_and_expect_error(data));
}

TEST_F(ParserTest, InvalidBulkStringHeader) {
    std::string data = "*1\r\n+OK\r\n";
    EXPECT_TRUE(parse_and_expect_error(data));
}

TEST_F(ParserTest, InvalidBulkStringLength) {
    std::string data = "*1\r\n$abc\r\nSET\r\n";
    EXPECT_TRUE(parse_and_expect_error(data));
}

TEST_F(ParserTest, InvalidArrayLength) {
    std::string data = "*abc\r\n$3\r\nSET\r\n";
    EXPECT_TRUE(parse_and_expect_error(data));
}

TEST_F(ParserTest, IncompleteData) {
    std::string data = "*2\r\n$3\r\nSET\r\n";
    EXPECT_TRUE(parse_and_expect_error(data));
}

TEST_F(ParserTest, ParseLpushCommand) {
    std::string data   = "*4\r\n$5\r\nLPUSH\r\n$7\r\nmylist1\r\n$1\r\na\r\n$1\r\nb\r\n";
    auto        result = parse(data);

    ASSERT_EQ(result.size(), 4);
    EXPECT_EQ(result[0], "LPUSH");
    EXPECT_EQ(result[1], "mylist1");
    EXPECT_EQ(result[2], "a");
    EXPECT_EQ(result[3], "b");
}

TEST_F(ParserTest, ParseSaddCommand) {
    std::string data   = "*4\r\n$4\r\nSADD\r\n$6\r\nmyset1\r\n$6\r\nmember\r\n$5\r\nvalue\r\n";
    auto        result = parse(data);

    ASSERT_EQ(result.size(), 4);
    EXPECT_EQ(result[0], "SADD");
    EXPECT_EQ(result[1], "myset1");
    EXPECT_EQ(result[2], "member");
    EXPECT_EQ(result[3], "value");
}

TEST_F(ParserTest, ParseZaddCommand) {
    std::string data   = "*4\r\n$4\r\nZADD\r\n$7\r\nmyzset1\r\n$3\r\n100\r\n$6\r\nmember\r\n";
    auto        result = parse(data);

    ASSERT_EQ(result.size(), 4);
    EXPECT_EQ(result[0], "ZADD");
    EXPECT_EQ(result[1], "myzset1");
    EXPECT_EQ(result[2], "100");
    EXPECT_EQ(result[3], "member");
}

TEST_F(ParserTest, ParseCommandWithSpaces) {
    std::string data   = "*2\r\n$3\r\nSET\r\n$11\r\nhello world\r\n";
    auto        result = parse(data);

    ASSERT_EQ(result.size(), 2);
    EXPECT_EQ(result[0], "SET");
    EXPECT_EQ(result[1], "hello world");
}

TEST_F(ParserTest, ParseCommandWithNewlineInValue) {
    std::string data   = "*2\r\n$3\r\nSET\r\n$11\r\nhello\nworld\r\n";
    auto        result = parse(data);

    ASSERT_EQ(result.size(), 2);
    EXPECT_EQ(result[0], "SET");
    EXPECT_EQ(result[1], "hello\nworld");
}

TEST_F(ParserTest, ParseCommandWithCRLFInValue) {
    std::string data   = "*2\r\n$3\r\nSET\r\n$12\r\nhello\r\nworld\r\n";
    auto        result = parse(data);

    ASSERT_EQ(result.size(), 2);
    EXPECT_EQ(result[0], "SET");
    EXPECT_EQ(result[1], "hello\r\nworld");
}

TEST_F(ParserTest, ParseSelectCommand) {
    std::string data   = "*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n";
    auto        result = parse(data);

    ASSERT_EQ(result.size(), 2);
    EXPECT_EQ(result[0], "SELECT");
    EXPECT_EQ(result[1], "0");
}

TEST_F(ParserTest, ParseAuthCommand) {
    std::string data   = "*2\r\n$4\r\nAUTH\r\n$8\r\npassword\r\n";
    auto        result = parse(data);

    ASSERT_EQ(result.size(), 2);
    EXPECT_EQ(result[0], "AUTH");
    EXPECT_EQ(result[1], "password");
}

TEST_F(ParserTest, ParseKeysCommand) {
    std::string data   = "*2\r\n$4\r\nKEYS\r\n$1\r\n*\r\n";
    auto        result = parse(data);

    ASSERT_EQ(result.size(), 2);
    EXPECT_EQ(result[0], "KEYS");
    EXPECT_EQ(result[1], "*");
}
