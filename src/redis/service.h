#pragma once

#include "common/config.h"
#include "db/engine.h"
#include "redis/connection.h"
#include "redis/service_interface.h"
#include "server/el_pool.h"
#include "server/handler.h"
#include "server/server.h"
#include "utils/pool/pool.h"

#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

namespace idlekv {

// RESP(Redis serialization protocol) Handler.
// Currently, only RESP2 is supported.
class RedisService : public Handler, public ServiceInterface {
public:
    using ConnectionPtr = std::unique_ptr<Connection>;

    RedisService(const Config& cfg, const std::shared_ptr<IdleEngine>& engine, EventLoopPool* elp)
        : Handler(cfg.ip_, std::atoi(cfg.port_.c_str())), engine_(engine) {
        elp->await_foreach([this](size_t, EventLoop*) {
            conn_pool_.set_pool_size(64);
            conn_pool_.set_new([this]() -> ConnectionPtr {
                return std::make_unique<Connection>(this);
            });
        });
    }

    virtual auto handle(asio::ip::tcp::socket socket) -> asio::awaitable<void> override;

    virtual auto exec(Connection*, const std::vector<std::string>& args) noexcept -> std::string override;

    virtual void stop() override {}

    virtual std::string name() override { return "Redis"; }

    virtual ~RedisService() override = default;

private:
    std::vector<std::unique_ptr<Connection>> conns_;

    std::shared_ptr<IdleEngine> engine_;

    static thread_local utils::Pool<ConnectionPtr> conn_pool_;
};

} // namespace idlekv
