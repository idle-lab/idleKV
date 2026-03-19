#include "server/server.h"

#include "common/config.h"
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
#include <unistd.h>

namespace idlekv {

namespace {

auto LogIoBackendStatus() -> void {
#if defined(ASIO_HAS_IO_URING_AS_DEFAULT)
    LOG(info, "I/O backend: io_uring enabled");
#elif defined(ASIO_HAS_IO_URING)
    LOG(info, "I/O backend: io_uring available but not the default backend");
#else
    LOG(info, "I/O backend: io_uring disabled, using reactor backend");
#endif
}

} // namespace

Server::Server(const Config& cfg) {
    // initialize the event loop pool and thread-local state for each worker.
    cfg_ = &cfg;
    LogIoBackendStatus();
    elp_ = std::make_unique<EventLoopPool>();
    elp_->Run();
    elp_->AwaitForeach([](size_t i, EventLoop* el) { ThreadState::Init(i, el, el->ThreadId()); });
    // check or create the data directory.

    // recover persisted data.
}

auto Server::DoAccept(Handler* h) -> asio::awaitable<void> {
    auto                    exec = co_await asio::this_coro::executor;
    asio::ip::tcp::acceptor acceptor(exec);
    std::error_code         ec;

    // open and prepare the listening socket for this handler.
    DISCARD_RESULT(acceptor.open(h->Endpoint().protocol(), ec));
    if (ec) {
        LOG(error, "acceptor open failed: {}", ec.message());
        co_return;
    }

    DISCARD_RESULT(acceptor.set_option(asio::ip::tcp::acceptor::reuse_address(true), ec));
    if (ec) {
        LOG(error, "acceptor set_option failed: {}", ec.message());
        co_return;
    }

    DISCARD_RESULT(acceptor.bind(h->Endpoint(), ec));
    if (ec) {
        LOG(error, "acceptor bind failed: {}", ec.message());
        co_return;
    }

    DISCARD_RESULT(acceptor.listen(asio::socket_base::max_listen_connections, ec));
    if (ec) {
        LOG(error, "acceptor listen failed: {}", ec.message());
        co_return;
    }

    LOG(info, "start handler {}, listen on {}:{}", h->Name(), h->Endpoint().address().to_string(),
        h->Endpoint().port());

    for (;;) {
        // accept connections continuously and hand them off to worker loops.
        auto [accept_ec, socket] =
            co_await acceptor.async_accept(asio::as_tuple(asio::use_awaitable));

        if (accept_ec) {
            // exit when the acceptor is intentionally stopped.
            if (accept_ec == asio::error::operation_aborted ||
                accept_ec == asio::error::bad_descriptor) {
                co_return; // 退出协程
            }

            // back off briefly when file descriptors are exhausted.
            if (accept_ec == asio::error::no_descriptors) {
                LOG(warn, "FD limit reached");

                co_await SetTimeout(std::chrono::seconds(1)).read();
                continue;
            }

            // keep serving on transient accept errors.
            LOG(warn, "accept error:" + accept_ec.message());

            continue;
        }

        DISCARD_RESULT(socket.set_option(asio::ip::tcp::no_delay(true), ec));
        if (ec) {
            LOG(warn, "set TCP_NODELAY failed: {}", ec.message());
        }

        auto* target_el = PickUpConnEl(socket);
        if (&socket.get_executor().context() != &target_el->IoContext()) {
            auto native_socket = socket.release(ec);
            if (ec) {
                LOG(warn, "release accepted socket failed: {}", ec.message());
                continue;
            }

            asio::ip::tcp::socket rebound_socket(target_el->IoContext());
            DISCARD_RESULT(rebound_socket.assign(h->Endpoint().protocol(), native_socket, ec));
            if (ec) {
                LOG(warn, "rebind accepted socket failed: {}", ec.message());
                ::close(native_socket);
                continue;
            }

            target_el->Dispatch(h->Handle(std::move(rebound_socket)));
            continue;
        }

        target_el->Dispatch(h->Handle(std::move(socket)));
    }
}

auto Server::PickUpConnEl(asio::ip::tcp::socket& sock) -> EventLoop* {
    uint32_t res_id = UINT32_MAX;
    // int fd = sock.native_handle();

    // int cpu;
    // socklen_t len = sizeof(cpu);

    // if (0 == getsockopt(fd, SOL_SOCKET, SO_INCOMING_CPU, &cpu, &len)) {
    //     LOG(info, "CPU for connection {} is {}", fd, cpu);

    //     const std::vector<unsigned>& ids = elp_->MapCpuToThreads(cpu);
    //     if (!ids.empty()) {
    //       res_id = ids[0];
    //     }
    // }

    // fall back to round-robin selection when no cpu hint is available.
    return res_id == UINT_MAX ? elp_->PickUpEl() : elp_->At(res_id);
}

void Server::ListenAndServe() {
    LOG(info, "start server");

    // initialize every handler on each event loop before accepting traffic.
    elp_->AwaitForeach([this]([[maybe_unused]] size_t i, EventLoop* el) {
        for (auto& handler : handlers_) {
            handler->Init(el);
        }
    });

    for (size_t i = 0; i < handlers_.size(); i++) {
        elp_->Dispatch(DoAccept(handlers_[i].get()));
    }

    // keep a small signal loop on the main thread for graceful shutdown.
    asio::io_context          signals_handler;
    asio::executor_work_guard wg = asio::make_work_guard(signals_handler);
    asio::signal_set          signals(signals_handler, SIGINT, SIGTERM, SIGABRT);
    signals.async_wait([this, &wg](const asio::error_code&, int) {
        LOG(info, "signal received, stopping server...");
        Stop();
        wg.reset();
    });

    signals_handler.run();
}

void Server::RegisterHandler(std::unique_ptr<Handler> handler) {
    LOG(info, "register handler: {}", handler->Name());
    handlers_.push_back(std::move(handler));
}

void Server::Stop() {
    // stop handlers first so they stop producing new work.
    for (auto& handler : handlers_) {
        handler->Stop();
    }

    // then stop all worker loops.
    elp_->Stop();
    LOG(info, "server stopped");
}

} // namespace idlekv
