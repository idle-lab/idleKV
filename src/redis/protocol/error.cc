#include "redis/protocol/error.h"

#include <spdlog/fmt/fmt.h>

#include <string>
#include <system_error>

namespace idlekv {

auto Err::make_reply() -> std::string { return "-Err unknown\r\n"; }

auto SyntaxErr::make_reply() -> std::string { return "-Err syntax error\r\n"; }

auto WrongTypeErr::make_reply() -> std::string {
    return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
}

auto ProtocolErr::make_reply(const std::string& msg) -> std::string {
    return fmt::format("-ERR Protocol error: '{}'\r\n", msg);
}

auto ArgNumErr::make_reply(const std::string& cmd) -> std::string {
    return fmt::format("-ERR wrong number of arguments for '{}' command\r\n", cmd);
}

auto UnknownCmdErr::make_reply(const std::string& cmd) -> std::string {
    return fmt::format("-ERR unknown command '{}'\r\n", cmd);
}

auto StandardErr::make_reply(const std::error_code& ec) -> std::string {
    return fmt::format("-ERR error: '{}'\r\n", ec.message());
}

} // namespace idlekv
