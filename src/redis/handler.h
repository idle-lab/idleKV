#pragma once

#include <asiochan/asiochan.hpp>
#include <common/config.h>
#include <cstdlib>
#include <memory>
#include <redis/connection.h>
#include <redis/type/base.h>
#include <server/handler.h>
#include <server/server.h>
#include <string>
#include <vector>

namespace idlekv {

class RedisHandler : public Handler {
public:
    RedisHandler(const Config& cfg, std::shared_ptr<Server> srv)
        : Handler(cfg.ip_, std::atoi(cfg.port_.c_str())), srv_(srv) {}

    asio::awaitable<void> handle(asio::ip::tcp::socket socket);

    asio::awaitable<void> parse_and_execute(std::shared_ptr<Connection> conn,
                                            asiochan::channel<Payload>  in,
                                            asiochan::channel<Payload>  out,
                                            asiochan::channel<void, 3>  doneCh);

    virtual asio::awaitable<void> start() override;

    virtual void stop() override {}

    virtual std::string name() override { return "Redis"; }

private:
    std::vector<Connection> conns_;
    std::shared_ptr<Server> srv_;
};

} // namespace idlekv
