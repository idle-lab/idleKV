#pragma once

#include <server/handler.h>
#include <server/server.h>
#include <common/config.h>
#include <redis/connection.h>
#include <vector>
#include <memory>
#include <cstdlib>
#include <asiochan/asiochan.hpp>

namespace idlekv {

class RedisHandler : public Handler {
public:
    RedisHandler(const Config& cfg, std::shared_ptr<Server> srv) 
    : Handler(cfg.ip_, std::atoi(cfg.port_.c_str())), 
      srv_(srv)
    { }

    asio::awaitable<void> handle(Connection& conn);

    asio::awaitable<void> parse_and_execute(asiochan::channel<std::pair<std::string, bool>> in,
                                            asiochan::channel<std::pair<std::string, bool>> out);

    virtual asio::awaitable<void> listen() override;

    virtual void stop() override {}

    virtual std::string name() override { return "Redis"; }

private:
    std::vector<Connection> conns_;
    std::shared_ptr<Server> srv_;
};

} // namespace idlekv
