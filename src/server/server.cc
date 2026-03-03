#include "server/server.h"

#include "common/logger.h"
#include "server/handler.h"
#include "server/thread_state.h"
#include "utils/timer/timer.h"

#include <asio/as_tuple.hpp>
#include <asio/awaitable.hpp>
#include <asio/co_spawn.hpp>
#include <asio/detached.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <ranges>
#include <spdlog/spdlog.h>
#include <thread>

namespace idlekv {

Server::Server(const Config& cfg) {
    // : workers(cfg.worker_threads_ == 0 ? std::thread::hardware_concurrency()
    //                                    : cfg.worker_threads_) {
    // 1. 初始化
    cfg_ = ServerConfig::build(cfg);

    threads_.resize(std::thread::hardware_concurrency());
    for (auto& t : threads_) {
        t = std::make_unique<ThreadState>();
    }

    // 2. 检查/创建数据文件夹

    // 3. 恢复数据
}

auto Server::do_accept(std::shared_ptr<Handler> h, asio::io_context& io) -> asio::awaitable<void> {
    asio::ip::tcp::acceptor acceptor(io);

    // open the acceptor with the option to reuse the address
    acceptor.open(h->endpoint().protocol());
    acceptor.set_option(asio::ip::tcp::acceptor::reuse_address(true));
    acceptor.bind(h->endpoint());
    acceptor.listen();

    LOG(info, "start handler {}, listen on {}:{}", h->name(),
        h->endpoint().address().to_string(), h->endpoint().port());

    for (;;) {
        auto [ec, socket] =
            co_await acceptor.async_accept(asio::as_tuple(asio::use_awaitable));

        if (ec) {
            // 被主动关闭
            if (ec == asio::error::operation_aborted ||
                ec == asio::error::bad_descriptor) {
                co_return; // 退出协程
            }

            // fd 耗尽
            if (ec == asio::error::no_descriptors) {
                LOG(warn, "FD limit reached");
                co_await set_timeout(std::chrono::seconds(1)).read();
                continue;
            }

            // 临时错误（握手中断等）
            LOG(warn, "accept error:" + ec.message());

            continue;
        }

        uint32_t mi = UINT_MAX;
        ThreadState* mi_ts = nullptr;
        for (auto& t : threads_) {
            if (t->co_num() < mi) {
                mi_ts = t.get();
                mi = t->co_num();
            }
        }

        mi_ts->assign(h->handle(std::move(socket)));
    }
}

void Server::listen_and_server() {
    LOG(info, "start server");


    for (size_t i = 0; i < handlers_.size(); i++) {
        auto& ts = threads_[i % threads_.size()];

        ts->assign(do_accept(handlers_[i], ts->io_context()));
    }

    asio::io_context          signal_handler;
    asio::executor_work_guard wg = asio::make_work_guard(signal_handler);
    asio::signal_set          signals(signal_handler, SIGINT, SIGTERM, SIGABRT);

    signals.async_wait([this, &wg](const asio::error_code&, int) {
        LOG(info, "signal received, stopping server...");
        stop();
        wg.reset();
    });

    signal_handler.run();
}

void Server::register_handler(std::shared_ptr<Handler> handler) {
    handlers_.push_back(handler);
    LOG(info, "register handler: {}", handler->name());
}

void Server::stop() {
    // workers.join();

    for (auto& handler : handlers_) {
        handler->stop();
    }

    LOG(info, "server stopped");
}

} // namespace idlekv
