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

private:
    int64_t data_;
};

class SimpleString : public Type {
public:
    SimpleString() = default;
    SimpleString(std::string&& data) : data_(std::move(data)) {}

    virtual auto type() -> DataType override { return DataType::String; }

    auto data() const noexcept -> const std::string& { return data_; }

    auto size() const noexcept -> size_t { return data_.size(); }

    virtual ~SimpleString() = default;

private:
    std::string data_;
};

class Error : public SimpleString {
public:
    virtual auto type() -> DataType override { return DataType::Error; }
};

class BulkString : public SimpleString {
public:
    virtual auto type() -> DataType override { return DataType::BulkString; }
};

class Array : public Type {
public:
    virtual auto type() -> DataType override { return DataType::Arrays; }

private:
    std::vector<BulkString> data_;
};

} // namespace idlekv
