#pragma once

#include <charconv>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <system_error>
#include <type_traits>
#include <unordered_map>
#include <vector>

namespace idlekv {

enum class DataType : char {
    String     = '+',
    Error      = '-',
    Integers   = ':',
    BulkString = '$',
    Arrays     = '*'
};
extern std::unordered_map<DataType, char> dmp;
extern const char*                        CRLF;

namespace detail {

template <class IntType>
auto decimal_len(IntType value) -> size_t {
    static_assert(std::is_integral_v<IntType>, "IntType must be integral");

    char buffer[std::numeric_limits<IntType>::digits10 + 3];
    auto [ptr, ec] = std::to_chars(buffer, buffer + sizeof(buffer), value);
    (void)ec;
    return static_cast<size_t>(ptr - buffer);
}

} // namespace detail

class Reply {
public:
    Reply()                                = default;
    Reply(const Reply&)                    = delete;
    auto operator=(const Reply&) -> Reply& = delete;

    virtual auto type() const -> DataType = 0;

    virtual auto to_bytes() const -> std::string = 0;

    virtual auto size() const -> size_t = 0;
};

template <class IntType>
    requires std::is_integral_v<IntType>
class Integer : public Reply {
public:
    Integer(uint64_t data) : data_(data) {}

    template <class T>
        requires std::is_integral_v<T>
    static auto make_reply(T data) -> std::string {
        constexpr size_t kMaxDigits = std::numeric_limits<T>::digits10 + 3;
        std::string      reply;
        reply.resize(1 + kMaxDigits + 2);

        reply[0]    = static_cast<char>(DataType::Integers);
        auto* begin = reply.data() + 1;
        auto* end   = begin + kMaxDigits;

        auto [ptr, ec] = std::to_chars(begin, end, data);
        if (ec != std::errc()) [[unlikely]] {
            return {};
        }

        *ptr++ = '\r';
        *ptr++ = '\n';
        reply.resize(static_cast<size_t>(ptr - reply.data()));
        return reply;
    }

    virtual auto type() const -> DataType override { return DataType::Integers; }

    virtual auto to_bytes() const -> std::string override { return make_reply(data_); }

    virtual auto size() const -> size_t override { return 1 + detail::decimal_len(data_) + 2; }

private:
    IntType data_;
};

class SimpleString : public Reply {
public:
    SimpleString(std::string&& data) : data_(std::move(data)) {}

    static auto make_reply(std::string_view data) -> std::string;

    virtual auto type() const -> DataType override { return DataType::String; }

    auto data() const noexcept -> const std::string& { return data_; }

    virtual auto size() const -> size_t override { return 1 + data_.size() + 2; }

    virtual auto to_bytes() const -> std::string override { return make_reply(data_); }

    virtual ~SimpleString() = default;

protected:
    std::string data_;
};

class BulkString : public SimpleString {
public:
    BulkString(std::string&& data, int32_t len) : SimpleString(std::move(data)), len_(len) {}

    static auto make_reply(std::string_view data, int32_t len) -> std::string;

    virtual auto type() const -> DataType override { return DataType::BulkString; }

    virtual auto to_bytes() const -> std::string override { return make_reply(data_, len_); }

    virtual auto size() const -> size_t override {
        // null bulk string: "$-1\r\n".
        if (len_ < 0) {
            return 1 + detail::decimal_len(len_) + 2;
        }
        return 1 + detail::decimal_len(len_) + 2 + data_.size() + 2;
    }

private:
    int32_t len_; // for null bulk string
};

// 目前 Array 只支持 BulkString 作为元素
class Array : public Reply {
public:
    Array(std::vector<BulkString>&& data) : data_(std::move(data)) {}

    virtual auto type() const -> DataType override { return DataType::Arrays; }

    static auto make_reply(const std::vector<BulkString>& data) -> std::string;

    virtual auto to_bytes() const -> std::string override {
        std::string res =
            static_cast<char>(DataType::BulkString) + std::to_string(data_.size()) + CRLF;

        for (auto& s : data_) {
            res.append(s.to_bytes());
        }

        return res + CRLF;
    }

    virtual auto size() const -> size_t override {
        size_t total = 1 + detail::decimal_len(data_.size()) + 2;
        for (const auto& s : data_) {
            total += s.size();
        }
        // Keep consistent with current to_bytes() behavior.
        return total + 2;
    }

private:
    std::vector<BulkString> data_;
};

class PongReply : public Reply {
public:
    static auto make_reply() -> std::string;

    virtual auto type() const -> DataType override { return DataType::String; }

    virtual auto to_bytes() const -> std::string override { return make_reply(); }

    virtual auto size() const -> size_t override {
        return 1 + 4 + 2; // +PONG\r\n
    }
};

class OKReply : public Reply {
public:
    static auto make_reply() -> std::string;

    virtual auto type() const -> DataType override { return DataType::String; }

    virtual auto to_bytes() const -> std::string override { return make_reply(); }

    virtual auto size() const -> size_t override {
        return 1 + 2 + 2; // +OK\r\n
    }
};

} // namespace idlekv
