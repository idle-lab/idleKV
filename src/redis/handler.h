#pragma once

#include "common/config.h"
#include "redis/connection.h"
#include "redis/protocol/parser.h"
#include "server/handler.h"
#include "server/server.h"

#include <asiochan/asiochan.hpp>
#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

namespace idlekv {

class RedisHandler : public Handler {
public:
    RedisHandler(const Config& cfg, std::shared_ptr<Server> srv)
        : Handler(cfg.ip_, std::atoi(cfg.port_.c_str())), srv_(srv) {}

    virtual asio::awaitable<void> handle(asio::ip::tcp::socket socket) override;

    // asio::awaitable<void> parse_and_execute(asiochan::channel<Payload> in,
    //                                         asiochan::channel<Payload> out,
    //                                         asiochan::channel<void, 3> doneCh);

    virtual void stop() override {}

    virtual std::string name() override { return "Redis"; }

    virtual ~RedisHandler() = default;

private:
    std::vector<Connection> conns_;
    std::shared_ptr<Server> srv_;
};

} // namespace idlekv
