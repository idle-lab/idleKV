#pragma once

#include "common/config.h"
#include "redis/connection.h"
#include "server/el_pool.h"
#include "server/handler.h"
#include "utils/pool/pool.h"

#include <asio/awaitable.hpp>
#include <asio/buffer_registration.hpp>
#include <asio/steady_timer.hpp>
#include <asio/this_coro.hpp>
#include <atomic>
#include <cstdlib>
#include <list>
#include <memory>
#include <string>
#include <vector>

namespace idlekv {

// RESP(Redis serialization protocol) Handler.
// Currently, only RESP2 is supported.
class RedisService : public Handler {
public:
    using ConnectionPtr = std::unique_ptr<Connection>;
    class ServiceTLState {
    public:
        static constexpr auto kConnPoolSize = 64;

        auto conn_pool() -> utils::Pool<ConnectionPtr>& { return conn_pool_; }
        auto conn_list() -> std::list<Connection*>& { return conn_list_; }

    private:
        utils::Pool<ConnectionPtr>                            conn_pool_;
        std::list<Connection*>                                conn_list_;
    };

    RedisService(const Config& cfg) : Handler(cfg.ip_, std::atoi(cfg.port_.c_str())) {}

    virtual auto init(EventLoop* el) -> void override;
    virtual auto handle(asio::ip::tcp::socket socket) -> asio::awaitable<void> override;

    static auto tlocal() -> ServiceTLState* { return tl_; }

    auto stopped() -> bool { return stop_.load(std::memory_order_acquire); }

    virtual void stop() override { stop_.store(true, std::memory_order_release); }

    virtual std::string name() override { return "Redis"; }

    virtual ~RedisService() override = default;

private:
    std::vector<std::unique_ptr<Connection>> conns_;

    std::atomic<bool> stop_{false};

    static thread_local ServiceTLState* tl_;
};

} // namespace idlekv
