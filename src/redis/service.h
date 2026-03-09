#pragma once

#include "common/config.h"
#include "db/engine.h"
#include "redis/connection.h"
#include "redis/service_interface.h"
#include "server/el_pool.h"
#include "server/handler.h"
#include "server/server.h"
#include "utils/pool/pool.h"

#include <asio/awaitable.hpp>
#include <asio/steady_timer.hpp>
#include <asio/this_coro.hpp>
#include <atomic>
#include <cstdlib>
#include <list>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace idlekv {

// RESP(Redis serialization protocol) Handler.
// Currently, only RESP2 is supported.
class RedisService : public Handler, public ServiceInterface {
public:
    using ConnectionPtr = std::unique_ptr<Connection>;

    RedisService(const Config& cfg, const std::shared_ptr<IdleEngine>& engine, EventLoopPool* elp)
        : Handler(cfg.ip_, std::atoi(cfg.port_.c_str())), engine_(engine) {
        elp->await_foreach([this](size_t, EventLoop* el) {
            conn_pool_.set_pool_size(64);
            conn_pool_.set_new([this]() -> ConnectionPtr {
                return std::make_unique<Connection>(this);
            });

            el->dispatch([this]()-> asio::awaitable<void> {
                auto exector = co_await asio::this_coro::executor;
                asio::steady_timer timer(exector);
                while (!stop_.load(std::memory_order_acquire)) {
                    timer.expires_after(kMaxReplyFlushInterval);

                    co_await timer.async_wait();

                    for (auto& conn : conn_list_) {
                        if (stop_.load(std::memory_order_acquire)) {
                            co_return;
                        }
                        if (conn == nullptr || conn->closed()) {
                            continue;
                        }
                        co_await conn->flush();
                    }
                }
            }());
        });

    }

    virtual auto handle(asio::ip::tcp::socket socket) -> asio::awaitable<void> override;

    virtual auto exec(Connection*, const std::vector<std::string>& args) noexcept -> std::string override;

    virtual void stop() override {
        stop_.store(true, std::memory_order_release);
    }

    virtual std::string name() override { return "Redis"; }

    virtual ~RedisService() override = default;

private:
    std::vector<std::unique_ptr<Connection>> conns_;

    std::shared_ptr<IdleEngine> engine_;

    std::atomic<bool> stop_{false};

    static thread_local utils::Pool<ConnectionPtr> conn_pool_;
    static thread_local std::list<ConnectionPtr> conn_list_;
};

} // namespace idlekv
