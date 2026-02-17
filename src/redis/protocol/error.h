#pragma once

#include "redis/protocol/reply.h"
#include <spdlog/fmt/fmt.h>
#include <string>
#include <system_error>

namespace idlekv {

class Err : public Type {
public:
    virtual auto type() -> DataType override { return DataType::Error; }

    virtual auto to_bytes() -> std::string override { return "-Err unknown\r\n"; }
};

// SyntaxErrReply represents meeting unexpected arguments
class SyntaxErr : public Err {
public:
    virtual auto to_bytes() -> std::string override { return "-Err syntax error\r\n"; }
};

// WrongTypeErrReply represents operation against a key holding the wrong kind of value
class WrongTypeErr : public Err {
public:
    virtual auto to_bytes() -> std::string override { return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"; }
};

// ProtocolErrReply represents meeting unexpected byte during parse requests
class ProtocolErr : public Err {
public:
    ProtocolErr(std::string msg) : msg_(msg) {}

    virtual auto to_bytes() -> std::string override { return fmt::format("-ERR Protocol error: '{}'\r\n", msg_); }
private:
    std::string msg_;
};

// StandardErrReply represents server error
class StandardErrReply : public Err {
public:
    StandardErrReply(std::error_code ec) : ec_(ec) {}

    virtual auto to_bytes() -> std::string override { return fmt::format("-ERR error: '{}'\r\n", ec_.message()); }
private:
    std::error_code ec_;
};

} // namespace idlekv
