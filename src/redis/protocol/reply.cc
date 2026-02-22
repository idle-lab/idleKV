#include "redis/protocol/reply.h"

#include <cstdint>
#include <string>
#include <vector>

namespace idlekv {

auto Integer::make_reply(uint64_t data) -> std::string {
    return static_cast<char>(DataType::Integers) + std::to_string(data) + CRLF;
}

auto SimpleString::make_reply(const std::string& data) -> std::string {
    return static_cast<char>(DataType::String) + data + CRLF;
}

auto BulkString::make_reply(const std::string& data, int32_t len) -> std::string {
    if (len < 0) {
        return static_cast<char>(DataType::BulkString) + std::to_string(len) + CRLF;
    }

    return static_cast<char>(DataType::BulkString) + std::to_string(len) + CRLF + data + CRLF;
}

auto Array::make_reply(const std::vector<BulkString>& data) -> std::string {
    std::string res = static_cast<char>(DataType::Arrays) + std::to_string(data.size()) + CRLF;

    for (auto& s : data) {
        res.append(s.to_bytes());
    }

    return res;
}

} // namespace idlekv
