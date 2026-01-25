#include <coroutine>
#include <cstdio>
#include <format>
#include <iostream>
#include <memory>
#include <signal.h>
#include <CLI11/CLI11.hpp>
#include <spdlog/spdlog.h>
#include <asio/asio.hpp>

using asio::awaitable;
using asio::co_spawn;
using asio::detached;
using asio::use_awaitable;
using asio::ip::tcp;
namespace this_coro = asio::this_coro;

#include <common/config.h>
#include <server/server.h>

std::string banner() {
    return R"(
    _     _ _      _  ___     __
   (_) __| | | ___| |/ \ \   / /
   | |/ _` | |/ _ \ ' /\ \ / / 
   | | (_| | |  __/ . \ \ V /  
   |_|\__,_|_|\___|_|\_\ \_/   
)";
}

awaitable<void> echo(tcp::socket socket) {
    try {
        char data[1024];
        for (;;) {
            std::size_t n =
                co_await socket.async_read_some(asio::buffer(data), use_awaitable);
            spdlog::info("recive: {}", std::string_view(data, n));
            co_await async_write(socket, asio::buffer(data, n), use_awaitable);
        }
    } catch (std::exception& e) {
        std::printf("echo Exception: %s\n", e.what());
    }
}

awaitable<void> listener() {
    auto          executor = co_await this_coro::executor;
    tcp::acceptor acceptor(executor, {tcp::v4(), 55555});
    for (;;) {
        tcp::socket socket = co_await acceptor.async_accept(use_awaitable);
        co_spawn(executor, echo(std::move(socket)), detached);
    }
}

class IntReader {
public:
    bool await_ready() const noexcept {
        return false;
    };

    void await_suspend(std::coroutine_handle<> h) noexcept {
        std::thread t([this, h] {
            sleep(1);
            value_ = 114514;
            h.resume();
        });

        t.detach();
    }

    int await_resume() {
        return value_;
    }

private:
    int value_{0};
};

class Coro {
public:
    struct promise_type {
        // 创建协程操作管理对象
        Coro get_return_object() {
            return Coro(std::coroutine_handle<promise_type>::from_promise(*this));
        }

        // 决定协程创建后是否马上执行
        // std::suspend_never：不挂起
        // std::suspend_always：挂起
        auto initial_suspend() noexcept {
            return std::suspend_always{};
        }

        // 协程执行完后是否马上销毁
        // std::suspend_never：不挂起，立即销毁资源
        // std::suspend_always：挂起，不立即销毁，等待外部 destroy 释放资源
        auto final_suspend() noexcept {
            return std::suspend_never{};
        }

        // 协程中有未处理的异常时如何处理
        void unhandled_exception() {
            std::cout << "未处理的异常" << std::endl;
        }

        int value_;
    };

    std::coroutine_handle<promise_type> handle;

    Coro(std::coroutine_handle<promise_type> handle) {
        this->handle = handle;
    }

    void resume() {
        if (!handle.done()) {
            handle.resume();
        }
    }
};



Coro MyCoro() {
    IntReader r;
    auto x = co_await r;

    std::cout << x << '\n';
}

int main(int argc, char** argv) {
    // try {
    //     asio::io_context io_context(1);

    //     asio::signal_set signals(io_context, SIGINT, SIGTERM);
    //     signals.async_wait([&](auto, auto) { io_context.stop(); });

    //     co_spawn(io_context, listener(), detached);

    //     io_context.run();
    // } catch (std::exception& e) {
    //     std::printf("Exception: %s\n", e.what());
    // }
    // auto co = MyCoro();
    // co.resume();
    // for (;;) {}
    try {
        idlekv::Config cfg;
        cfg.parse(argc, argv);
        idlekv::Server srv(idlekv::make_default_logger(), idlekv::ServerConfig::build(cfg));

        spdlog::info("start server");
        srv.listen_and_server();
    } catch (const std::exception& e) {
        spdlog::error(e.what());
    }
}
