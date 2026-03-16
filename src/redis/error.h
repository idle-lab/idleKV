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


} // namespace idlekv
