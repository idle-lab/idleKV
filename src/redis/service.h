#pragma once

#include "common/config.h"
#include "redis/connection.h"
#include "redis/parser.h"
#include "server/el_pool.h"
#include "server/handler.h"
#include "utils/pool/pool.h"

#include <array>
#include <asio/any_io_executor.hpp>
#include <asio/awaitable.hpp>
#include <asio/buffer.hpp>
#include <asio/buffer_registration.hpp>
#include <asio/io_context.hpp>
#include <asio/registered_buffer.hpp>
#include <asio/steady_timer.hpp>
#include <asio/this_coro.hpp>
#include <atomic>
#include <cstddef>
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
        struct Slot {
            asio::mutable_registered_buffer buffer;
            size_t index;
        };
    public:
        static constexpr auto kConnPoolSize = 64;

        auto init(asio::any_io_executor exector, size_t buf_size_per_conn) -> void;

        auto get_register_buffer() -> std::optional<Slot>;
        auto free_buffer(size_t) -> void;
        auto conn_pool() -> utils::Pool<ConnectionPtr>& { return conn_pool_; }
        auto conn_list() -> std::list<Connection*>& { return conn_list_; }

    private:
        utils::Pool<ConnectionPtr>                            conn_pool_;
        std::list<Connection*>                                conn_list_;
        std::unique_ptr<byte[]> buf_space_;
        std::vector<size_t> free_list_;
        std::optional<asio::buffer_registration<std::vector<asio::mutable_buffer>>> buffer_registration_;
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
    std::atomic<bool> stop_{false};

    static thread_local ServiceTLState* tl_;
};

} // namespace idlekv
