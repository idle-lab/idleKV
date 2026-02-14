#include "server/server.h"

#include "common/logger.h"
#include "utils/timer/timer.h"

#include <asio/as_tuple.hpp>
#include <asio/awaitable.hpp>
#include <asio/co_spawn.hpp>
#include <asio/detached.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>
#include <cstdint>
#include <ranges>
#include <spdlog/spdlog.h>
#include <thread>

namespace idlekv {

Server::Server(const Config& cfg)
    : workers(cfg.worker_threads_ == 0 ? std::thread::hardware_concurrency()
                                       : cfg.worker_threads_) {
    // 1. 初始化
    cfg_ = ServerConfig::build(cfg);

    // 2. 检查/创建数据文件夹

    // 3. 恢复数据
}

void Server::accept() {
    asio::io_context io;
    for (auto& h : handlers_) {
        asio::co_spawn(
            io,
            [h, &io]() -> asio::awaitable<void> {
                asio::ip::tcp::acceptor acceptor(io, h->endpoint());
                for (;;) {
                    auto [ec, socket] = co_await acceptor.async_accept(asio::as_tuple(asio::use_awaitable));

                    if (ec)
                    {
                        // 被主动关闭
                        if (ec == asio::error::operation_aborted ||
                            ec == asio::error::bad_descriptor)
                        {
                            co_return; // 退出协程
                        }

                        // fd 耗尽
                        if (ec == asio::error::no_descriptors)
                        {
                            LOG(warn, "FD limit reached");
                            co_await set_timeout(std::chrono::seconds(1)).read();
                            continue;
                        }

                        // 临时错误（握手中断等）
                        LOG(warn, "accept error:" + ec.message());

                        continue;
                    }


                    asio::co_spawn(io,
                                   h->handle(std::move(socket)),
                                   asio::detached);
                }
            }(),
            asio::detached);
    }
}

void Server::listen_and_server() {
    LOG(info, "start server");

    for (auto i [[maybe_unused]] : std::views::iota(uint16_t(0), cfg_->io_threads)) {
        io_threads_.emplace_back(&Server::accept, this);
    }

    asio::io_context signal_handler;
    asio::executor_work_guard wg = asio::make_work_guard(signal_handler);
    asio::signal_set signals(signal_handler, SIGINT, SIGTERM, SIGABRT);

    signals.async_wait([this](const asio::error_code&, int) {
        spdlog::info("signal received, stopping server...");
        stop();
    });

    signal_handler.run();
}

void Server::register_handler(std::shared_ptr<Handler> handler) {
    handlers_.push_back(handler);
    LOG(info, "register handler: {}, {}:{}", handler->name(),
        handler->endpoint().address().to_string(), handler->endpoint().port());
}

void Server::stop() {
    workers.join();

    for (auto& handler : handlers_) {
        handler->stop();
    }

    LOG(info, "server stopped");
}

} // namespace idlekv
