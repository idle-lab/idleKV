#include "server/server.h"

#include "common/config.h"
#include "common/logger.h"
#include "server/el_pool.h"
#include "server/fiber_runtime.h"
#include "server/handler.h"
#include "server/thread_state.h"
#include "utils/timer/timer.h"

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/system/detail/error_code.hpp>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <spdlog/spdlog.h>
#include <sys/socket.h>
#include <system_error>
#include <unistd.h>

namespace idlekv {

namespace {

template <typename ErrorEnum>
auto ToStdErrorCode(ErrorEnum ec) -> std::error_code {
    return boost::system::error_code(ec);
}

auto LogIoBackendStatus() -> void {
#if defined(BOOST_ASIO_HAS_IO_URING_AS_DEFAULT)
    LOG(info, "I/O backend: io_uring enabled");
#elif defined(BOOST_ASIO_HAS_IO_URING)
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
    elp_->AwaitForeach([](size_t i, EventLoop* el) {
        ThreadState::Init(i, el, el->ThreadId());
    });
    // check or create the data directory.

    // recover persisted data.
}

auto Server::DoAccept(Handler* h) -> void {
    asio::ip::tcp::acceptor acceptor(CurrentIoContext());
    boost::system::error_code ec;

    // open and prepare the listening socket for this handler.
    DISCARD_RESULT(acceptor.open(h->Endpoint().protocol(), ec));
    if (ec) {
        LOG(error, "acceptor open failed: {}", ec.message());
        return;
    }

    DISCARD_RESULT(acceptor.set_option(asio::ip::tcp::acceptor::reuse_address(true), ec));
    if (ec) {
        LOG(error, "acceptor set_option failed: {}", ec.message());
        return;
    }

    DISCARD_RESULT(acceptor.bind(h->Endpoint(), ec));
    if (ec) {
        LOG(error, "acceptor bind failed: {}", ec.message());
        return;
    }

    DISCARD_RESULT(acceptor.listen(asio::socket_base::max_listen_connections, ec));
    if (ec) {
        LOG(error, "acceptor listen failed: {}", ec.message());
        return;
    }

    LOG(info, "start handler {}, listen on {}:{}", h->Name(), h->Endpoint().address().to_string(),
        h->Endpoint().port());

    for (;;) {

        boost::system::error_code accept_ec;
        asio::ip::tcp::socket socket(CurrentIoContext());
        acceptor.async_accept(socket, boost::fibers::asio::yield[accept_ec]);

        if (accept_ec) {
            if (accept_ec == ToStdErrorCode(asio::error::operation_aborted) ||
                accept_ec == ToStdErrorCode(asio::error::bad_descriptor)) {
                return;
            }

            if (accept_ec == ToStdErrorCode(asio::error::no_descriptors)) {
                LOG(warn, "FD limit reached");
                DISCARD_RESULT(SetTimeout(std::chrono::seconds(1)));
                continue;
            }

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

            target_el->Dispatch([h, socket = std::move(rebound_socket)]() mutable {
                h->Handle(std::move(socket));
            });
            continue;
        }

        target_el->Dispatch([h, socket = std::move(socket)]() mutable { h->Handle(std::move(socket)); });
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
        elp_->Dispatch([this, handler = handlers_[i].get()]() {
            DoAccept(handler); 
        });
    }

    // keep a small signal loop on the main thread for graceful shutdown.
    asio::io_context          signals_handler;
    asio::executor_work_guard wg = asio::make_work_guard(signals_handler);
    asio::signal_set          signals(signals_handler, SIGINT, SIGTERM, SIGABRT);
    signals.async_wait([this, &wg](const boost::system::error_code&, int) {
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
