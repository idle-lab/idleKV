#pragma once

#include <spdlog/fmt/fmt.h>
#include <string>
#include <system_error>
#include <utility>

namespace idlekv {

constexpr const char* kSyntaxErr        = "Err syntax error";
constexpr const char* kWrongTypeErr     = "Err wrong type error";
constexpr const char* kProtocolErrFmt   = "Err protocol error: '{}'";
constexpr const char* kArgNumErrFmt     = "ERR wrong number of arguments for '{}' command";
constexpr const char* kUnknownCmdErrFmt = "Err unknown command '{}'";
constexpr const char* kStandardErr      = "Err standard error";

// class Err {
// public:
//     static auto make_reply() -> std::string;

//     virtual auto to_bytes() const -> std::string { return make_reply(); }

//     virtual auto size() const -> size_t { return sizeof("-Err unknown\r\n") - 1; }

//     virtual auto is_standard_error() const -> bool { return false; }
// };

// // SyntaxErrReply represents meeting unexpected arguments
// class SyntaxErr : public Err {
// public:
//     static auto make_reply() -> std::string;

//     virtual auto to_bytes() const -> std::string override { return make_reply(); }

//     virtual auto size() const -> size_t override { return sizeof("-Err syntax error\r\n") - 1; }
// };

// // WrongTypeErrReply represents operation against a key holding the wrong kind of value
// class WrongTypeErr : public Err {
// public:
//     static auto make_reply() -> std::string;

//     virtual auto to_bytes() const -> std::string override { return make_reply(); }

//     virtual auto size() const -> size_t override {
//         return sizeof("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n") -
//         1;
//     }
// };

// // ProtocolErrReply represents meeting unexpected byte during parse requests
// class ProtocolErr : public Err {
// public:
//     ProtocolErr(std::string msg) : msg_(msg) {}

//     static auto make_reply(const std::string& msg) -> std::string;

//     virtual auto to_bytes() const -> std::string override { return make_reply(msg_); }

//     virtual auto size() const -> size_t override {
//         return (sizeof("-ERR Protocol error: '") - 1) + msg_.size() + (sizeof("'\r\n") - 1);
//     }

// private:
//     std::string msg_;
// };

// class ArgNumErr : public Err {
// public:
//     ArgNumErr(std::string cmd) : cmd_(cmd) {}

//     static auto make_reply(const std::string& cmd) -> std::string;

//     virtual auto to_bytes() const -> std::string override { return make_reply(cmd_); }

//     virtual auto size() const -> size_t override {
//         return (sizeof("-ERR wrong number of arguments for '") - 1) + cmd_.size() +
//                (sizeof("' command\r\n") - 1);
//     }

// private:
//     std::string cmd_;
// };

// class UnknownCmdErr : public Err {
// public:
//     UnknownCmdErr(std::string cmd) : cmd_(std::move(cmd)) {}

//     static auto make_reply(const std::string& cmd) -> std::string;

//     virtual auto to_bytes() const -> std::string override { return make_reply(cmd_); }

//     virtual auto size() const -> size_t override {
//         return (sizeof("-ERR unknown command '") - 1) + cmd_.size() + (sizeof("'\r\n") - 1);
//     }

// private:
//     std::string cmd_;
// };

// // StandardErrReply represents server error
// class StandardErr : public Err {
// public:
//     StandardErr(std::error_code ec) : ec_(ec) {}

//     static auto make_reply(const std::error_code& ec) -> std::string;

//     virtual auto to_bytes() const -> std::string override { return make_reply(ec_); }

//     virtual auto size() const -> size_t override {
//         return (sizeof("-ERR error: '") - 1) + ec_.message().size() + (sizeof("'\r\n") - 1);
//     }

//     auto error_code() const -> std::error_code { return ec_; }

//     virtual auto is_standard_error() const -> bool override { return true; }

// private:
//     std::error_code ec_;
// };

} // namespace idlekv
