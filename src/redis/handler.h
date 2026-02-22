#pragma once

#include "common/config.h"
#include "db/db.h"
#include "db/engine.h"
#include "redis/connection.h"
#include "server/handler.h"
#include "server/server.h"

#include <asiochan/asiochan.hpp>
#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

namespace idlekv {

// RESP(Redis serialization protocol) Handler.
// Currently, only RESP2 is supported.
class RESPHandler : public Handler {
public:
    RESPHandler(const Config& cfg, std::shared_ptr<Server> srv)
        : Handler(cfg.ip_, std::atoi(cfg.port_.c_str())), srv_(srv) {}

    virtual auto handle(asio::ip::tcp::socket socket) -> asio::awaitable<void> override;

    virtual void stop() override {}

    virtual std::string name() override { return "Redis"; }

    virtual ~RESPHandler() override = default;

private:
    std::vector<Connection> conns_;
    std::shared_ptr<Server> srv_;

    std::shared_ptr<IdleEngine> engine_;
    std::shared_ptr<DB> db_ = std::make_shared<DB>();
};

} // namespace idlekv
