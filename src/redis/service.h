#pragma once

#include "common/config.h"
#include "db/command.h"
#include "redis/connection.h"
#include "server/el_pool.h"
#include "server/handler.h"
#include "utils/pool/pool.h"

#include <atomic>
#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/buffer_registration.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/registered_buffer.hpp>
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
    class ServiceTLData {
        struct Slot {
            asio::mutable_registered_buffer buffer;
            size_t                          index;
        };

    public:
        static constexpr auto kConnPoolSize = 64;

        auto Init(asio::any_io_executor exector, size_t buf_size_per_conn) -> void;

        auto GetRegisterBuffer() -> std::optional<Slot>;
        auto FreeBuffer(size_t) -> void;
        auto ConnPool() -> utils::Pool<ConnectionPtr>& { return conn_pool_; }
        auto ConnList() -> std::list<Connection*>& { return conn_list_; }
        auto GetCmdArgsOrCreate() -> CmdArgsPtr;
        auto RecycleCmdArgs(CmdArgsPtr ptr) -> void;

    private:
        utils::Pool<ConnectionPtr> conn_pool_;
        std::list<Connection*>     conn_list_;

        // pipeline cmd args pool
        utils::Pool<CmdArgsPtr> args_pool_;

        // register buffer
        std::unique_ptr<char[]> buf_space_;
        std::vector<size_t>     free_list_;
        std::optional<asio::buffer_registration<std::vector<asio::mutable_buffer>>>
            buffer_registration_;
    };

    RedisService(const Config& cfg) : Handler(cfg.ip_, std::atoi(cfg.port_.c_str())) {}

    virtual auto Init(EventLoop* el) -> void override;
    virtual auto Handle(asio::ip::tcp::socket socket) -> void override;

    static auto Tlocal() -> ServiceTLData* { return tl_; }

    auto Stopped() -> bool { return stop_.load(std::memory_order_acquire); }

    virtual void Stop() override { stop_.store(true, std::memory_order_release); }

    virtual std::string Name() override { return "Redis"; }

    virtual ~RedisService() override = default;

private:
    std::atomic<bool> stop_{false};

    static thread_local ServiceTLData* tl_;
};

} // namespace idlekv
