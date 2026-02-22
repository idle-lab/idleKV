#pragma once

#include "redis/protocol/reply.h"

#include <spdlog/fmt/fmt.h>
#include <string>
#include <system_error>
#include <utility>

namespace idlekv {

class Err : public Reply {
public:
    virtual auto type() const -> DataType override { return DataType::Error; }

    virtual auto to_bytes() const -> std::string override { return "-Err unknown\r\n"; }

    virtual auto is_standard_error() const -> bool { return false; }
};

// SyntaxErrReply represents meeting unexpected arguments
class SyntaxErr : public Err {
public:
    virtual auto to_bytes() const -> std::string override { return "-Err syntax error\r\n"; }
};

// WrongTypeErrReply represents operation against a key holding the wrong kind of value
class WrongTypeErr : public Err {
public:
    virtual auto to_bytes() const -> std::string override {
        return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    }
};

// ProtocolErrReply represents meeting unexpected byte during parse requests
class ProtocolErr : public Err {
public:
    ProtocolErr(std::string msg) : msg_(msg) {}

    virtual auto to_bytes() const -> std::string override {
        return fmt::format("-ERR Protocol error: '{}'\r\n", msg_);
    }

private:
    std::string msg_;
};

class ArgNumErr : public Err {
public:
    ArgNumErr(std::string cmd) : cmd_(cmd) {}

    virtual auto to_bytes() const -> std::string override {
        return fmt::format("-ERR wrong number of arguments for '{}' command\r\n", cmd_);
    }

private:
    std::string cmd_;
};

class UnknownCmdErr : public Err {
public:
    UnknownCmdErr(std::string cmd) : cmd_(std::move(cmd)) {}

    virtual auto to_bytes() const -> std::string override {
        return fmt::format("-ERR unknown command '{}'\r\n", cmd_);
    }

private:
    std::string cmd_;
};

// StandardErrReply represents server error
class StandardErr : public Err {
public:
    StandardErr(std::error_code ec) : ec_(ec) {}

    virtual auto to_bytes() const -> std::string override {
        return fmt::format("-ERR error: '{}'\r\n", ec_.message());
    }

    auto error_code() const -> std::error_code { return ec_; }

    virtual auto is_standard_error() const -> bool override { return true; }

private:
    std::error_code ec_;
};

} // namespace idlekv
