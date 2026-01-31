#include <redis/handler.h>

#include <asiochan/select.hpp>
#include <asiochan/asiochan.hpp>
#include <utils/timer/timer.hpp>
#include <chrono>

namespace idlekv {

asio::awaitable<void> RedisHandler::handle(asio::ip::tcp::socket socket) {
    auto conn = std::make_shared<Connection>(std::move(socket));
    auto ep   = conn->socket().remote_endpoint();

    using msgChan = asiochan::channel<Payload>;
    using doneChan = asiochan::channel<void, 3>;
    auto in = msgChan(), out = msgChan();
    auto doneCh = doneChan();

    // 执行逻辑
    asio::co_spawn(
        srv_->get_worker_pool(), 
        parse_and_execute(conn, in, out, doneCh),
        asio::detached
    );

    // 写逻辑
    asio::co_spawn(
        srv_->get_io_context(),
        [conn](msgChan out, doneChan doneCh) -> asio::awaitable<void> {
            try {
                for (;;) {
                    auto [response, done] = co_await out.read();
                    if (done) {
                        break;
                    }

                    size_t n = co_await asio::async_write(conn->socket(), asio::buffer(response),
                                                          asio::use_awaitable);
                    LOG(debug, "send: {}", response);
                }
            } catch (std::exception& e) {
                LOG(error, "Connection write exception: {}", e.what());
            }

            co_await doneCh.write();
            LOG(debug, "Connection write task done");
        }(out, doneCh),
        asio::detached
    );
    
    // 读逻辑
    asio::co_spawn(
        srv_->get_io_context(), 
        [conn](msgChan in, doneChan doneCh) -> asio::awaitable<void> {
            char buff[1024];
            try {
                for (;;) {
                    auto n = co_await conn->socket().async_read_some(
                        asio::buffer(buff, sizeof(buff) - 1), asio::use_awaitable);
                    if (n == 0) {
                        LOG(info, "Connection closed by peer");
                        co_await in.write({"", true});
                        break;
                    }
                    co_await in.write({std::string(buff, n), false});
                }
            } catch (std::exception& e) {
                LOG(error, "Connection handle exception: {}", e.what());
            }

            co_await doneCh.write();
            LOG(debug, "Connection read task done");
        }(in, doneCh),
        asio::detached
    );

    co_await doneCh.read();
    conn->close();
    int cnt = 1;
    // 等待所有协程都退出
    for (;;) {
        auto timeout = set_timeout(std::chrono::milliseconds(200));

        auto res     = co_await asiochan::select(
            asiochan::ops::write(Payload{"", true}, in, out),
            asiochan::ops::read(timeout)
        );

        while(doneCh.try_read()) {
            cnt++;
        }

        if (cnt >= 3) {
            break;
        }
    }

    LOG(debug, "Connection from {} closed", ep.address().to_string());
}

asio::awaitable<void> RedisHandler::parse_and_execute(std::shared_ptr<Connection> conn,
                                                      asiochan::channel<Payload>  in,
                                                      asiochan::channel<Payload>  out,
                                                      asiochan::channel<void, 3>  doneCh) {
    try {
        for (;;) {
            auto [data, done] = co_await in.read();
            if (done) {
                break;
            }
            // 解析以及执行逻辑
            LOG(debug, "Parsed command: {}", data);
            std::string response = "you say: " + data + "\r\n";
            co_await out.write({std::move(response), done});
        }
    } catch (std::exception& e) {
        LOG(error, "Parse and execute exception: {}", e.what());
    }

    co_await doneCh.write();
    LOG(debug, "Parse and execute task done");
}

asio::awaitable<void> RedisHandler::listen() {
    auto exec = co_await asio::this_coro::executor;
    asio::ip::tcp::acceptor acceptor(exec, ep_);

    LOG(info, "Handler {} listening on {}:{}", name(), ep_.address().to_string(), ep_.port());

    for (;;) {
        auto [e, socket] = co_await acceptor.async_accept(asio::as_tuple);

        if (e) {
            LOG(error, "Accept error: {}", e.message());
            continue;
        }
        asio::co_spawn(exec, handle(std::move(socket)), asio::detached);
    }
}

} // namespace idlekv
