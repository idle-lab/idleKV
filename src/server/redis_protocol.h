#pragma once

#include <memory>
#include <string>
#include <vector>

namespace idlekv {

class IdleKVEngine;

// Redis命令类型
enum class RedisCommandType {
    GET,
    SET,
    DEL,
    EXISTS,
    MGET,
    MSET,
    SCAN,
    PING,
    INFO,
    FLUSHALL,
    UNKNOWN
};

// Redis命令解析结果
struct RedisCommand {
    RedisCommandType         type;
    std::vector<std::string> args;

    RedisCommand(RedisCommandType t) : type(t) {}
};

// Redis协议处理器
class RedisProtocolHandler {
  private:
    IdleKVEngine* engine_;

    // 协议解析
    std::unique_ptr<RedisCommand> parse_command(const std::string& request);
    RedisCommandType              get_command_type(const std::string& cmd);

    // 命令执行
    std::string execute_get(const std::vector<std::string>& args);
    std::string execute_set(const std::vector<std::string>& args);
    std::string execute_del(const std::vector<std::string>& args);
    std::string execute_exists(const std::vector<std::string>& args);
    std::string execute_mget(const std::vector<std::string>& args);
    std::string execute_mset(const std::vector<std::string>& args);
    std::string execute_scan(const std::vector<std::string>& args);
    std::string execute_ping(const std::vector<std::string>& args);
    std::string execute_info(const std::vector<std::string>& args);
    std::string execute_flushall(const std::vector<std::string>& args);

    // 响应格式化
    std::string format_simple_string(const std::string& str);
    std::string format_bulk_string(const std::string& str);
    std::string format_array(const std::vector<std::string>& arr);
    std::string format_integer(int64_t num);
    std::string format_error(const std::string& error);
    std::string format_null();

  public:
    RedisProtocolHandler(IdleKVEngine* engine);
    ~RedisProtocolHandler();

    // 主要接口
    std::string process_request(const std::string& request);

    // 协议工具
    static std::vector<std::string> parse_resp_array(const std::string& data);
    static std::string              encode_resp_array(const std::vector<std::string>& arr);
    static bool                     is_complete_request(const std::string& data);
};

} // namespace idlekv