#include "redis/protocol/reply.h"

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace idlekv {

std::unordered_map<DataType, char> dmp = {
    {DataType::String, '+'},
    {DataType::Error, '-'},
    {DataType::Integers, ':'},
    {DataType::BulkString, '$'},
    {DataType::Arrays, '*'},
};

auto SimpleString::make_reply(std::string_view data) -> std::string {
    std::string str;
    str.reserve(1 + data.size() + 2);
    str.push_back(dmp[DataType::String]);
    str.append(data);
    str.append(CRLF);
    return str;
}

auto BulkString::make_reply(std::string_view data, int32_t len) -> std::string {
    if (len < 0) {
        return "$-1\r\n";
    }

    char len_buf[std::numeric_limits<int32_t>::digits10 + 1];
    auto [len_ptr, ec] = std::to_chars(len_buf, len_buf + sizeof(len_buf), len);
    if (ec != std::errc()) [[unlikely]] {
        return {};
    }

    const size_t len_digits = static_cast<size_t>(len_ptr - len_buf);

    std::string res;
    res.reserve(1 + len_digits + 2 + data.size() + 2);
    res.push_back(static_cast<char>(DataType::BulkString));
    res.append(len_buf, len_digits);
    res.append(CRLF);
    res.append(data);
    res.append(CRLF);
    return res;
}

auto Array::make_reply(const std::vector<BulkString>& data) -> std::string {
    std::string res = static_cast<char>(DataType::Arrays) + std::to_string(data.size()) + CRLF;

    for (auto& s : data) {
        res.append(s.to_bytes());
    }

    return res;
}

auto PongReply::make_reply() -> std::string { return SimpleString::make_reply("PONG"); }

auto OKReply::make_reply() -> std::string { return SimpleString::make_reply("OK"); }

} // namespace idlekv
