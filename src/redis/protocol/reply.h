#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace idlekv {

enum class DataType : char {
    String     = '+',
    Error      = '-',
    Integers   = ':',
    BulkString = '$',
    Arrays     = '*'
};

extern const char* CRLF;

class Type {
public:
    virtual auto type() -> DataType = 0;

    virtual auto to_bytes() -> std::string = 0;
};

class Integer : public Type {
public:
    Integer(uint64_t data) : data_(data) {}

    virtual auto type() -> DataType override { return DataType::Integers; }

    auto data() const -> int64_t { return data_; }

    virtual auto to_bytes() -> std::string override {
        return static_cast<char>(DataType::Integers) + std::to_string(data_) + CRLF;
    }

private:
    int64_t data_;
};

class SimpleString : public Type {
public:
    SimpleString(std::string&& data) : data_(std::move(data)) {}

    virtual auto type() -> DataType override { return DataType::String; }

    auto data() const noexcept -> const std::string& { return data_; }

    auto size() const noexcept -> size_t { return data_.size(); }

    virtual auto to_bytes() -> std::string override {
        return static_cast<char>(type()) + data_ + CRLF;
    }

    virtual ~SimpleString() = default;

protected:
    std::string data_;
};

class BulkString : public SimpleString {
public:
    virtual auto type() -> DataType override { return DataType::BulkString; }

    virtual auto to_bytes() -> std::string override {
        return static_cast<char>(type()) + std::to_string(len_) + CRLF + data_ + CRLF;
    }

private:
    int32_t len_; // for null bulk string
};

// 目前 Array 只支持 BulkString 作为元素
class Array : public Type {
public:
    virtual auto type() -> DataType override { return DataType::Arrays; }

    virtual auto to_bytes() -> std::string override {
        std::string res =
            static_cast<char>(DataType::BulkString) + std::to_string(data_.size()) + CRLF;

        for (auto s : data_) {
            res.append(s.to_bytes());
        }

        return res + CRLF;
    }

private:
    std::vector<BulkString> data_;
};

} // namespace idlekv
