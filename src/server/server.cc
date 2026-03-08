#include "server/server.h"

#include "common/logger.h"
#include "server/el_pool.h"
#include "server/handler.h"
#include "server/thread_state.h"
#include "utils/timer/timer.h"

#include <asio/as_tuple.hpp>
#include <asio/awaitable.hpp>
#include <asio/co_spawn.hpp>
#include <asio/detached.hpp>
#include <asio/executor_work_guard.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/this_coro.hpp>
#include <asio/use_future.hpp>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <spdlog/spdlog.h>
#include <sys/socket.h>

namespace idlekv {

Server::Server(const Config& cfg) {
    // 1. 初始化
    cfg_ = &cfg;
    elp_ = std::make_unique<EventLoopPool>();
    elp_->run();
    elp_->await_foreach(
        [](size_t i, EventLoop* el) { ThreadState::init(i, el, el->thread_id()); });
    // 2. 检查/创建数据文件夹

    // 3. 恢复数据
}

auto Server::do_accept(Handler* h) -> asio::awaitable<void> {
    auto exec = co_await asio::this_coro::executor;
    asio::ip::tcp::acceptor acceptor(exec);

    // open the acceptor with the option to reuse the address
    acceptor.open(h->endpoint().protocol());
    acceptor.set_option(asio::ip::tcp::acceptor::reuse_address(true));
    acceptor.bind(h->endpoint());
    acceptor.listen();

    LOG(info, "start handler {}, listen on {}:{}", h->name(), h->endpoint().address().to_string(),
        h->endpoint().port());

    for (;;) {
        auto [ec, socket] = co_await acceptor.async_accept(asio::as_tuple(asio::use_awaitable));

        if (ec) {
            // 被主动关闭
            if (ec == asio::error::operation_aborted || ec == asio::error::bad_descriptor) {
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

        pick_up_conn_el(socket)->dispatch(h->handle(std::move(socket)));
    }
}

auto Server::pick_up_conn_el(asio::ip::tcp::socket& sock) -> EventLoop* {
    uint32_t res_id = UINT32_MAX;
    // int fd = sock.native_handle();

    // int cpu;
    // socklen_t len = sizeof(cpu);

    // if (0 == getsockopt(fd, SOL_SOCKET, SO_INCOMING_CPU, &cpu, &len)) {
    //     LOG(info, "CPU for connection {} is {}", fd, cpu);

    //     const std::vector<unsigned>& ids = elp_->map_cpu_to_threads(cpu);
    //     if (!ids.empty()) {
    //       res_id = ids[0];
    //     }
    // }

    return res_id == UINT_MAX ? elp_->pick_up_el() : elp_->at(res_id);
}

void Server::listen_and_server() {
    LOG(info, "start server");

    for (size_t i = 0; i < handlers_.size(); i++) {
        elp_->dispatch(do_accept(handlers_[i].get()));
    }
    asio::io_context          signals_handler;
    asio::executor_work_guard wg = asio::make_work_guard(signals_handler);
    asio::signal_set          signals(signals_handler, SIGINT, SIGTERM, SIGABRT);
    signals.async_wait([this, &wg](const asio::error_code&, int) {
        LOG(info, "signal received, stopping server...");
        stop();
        wg.reset();
    });

    signals_handler.run();
}

void Server::register_handler(std::unique_ptr<Handler> handler) {
    LOG(info, "register handler: {}", handler->name());
    handlers_.push_back(std::move(handler));
}

void Server::stop() {
    for (auto& handler : handlers_) {
        handler->stop();
    }

    elp_->stop();
    LOG(info, "server stopped");
}

} // namespace idlekv
