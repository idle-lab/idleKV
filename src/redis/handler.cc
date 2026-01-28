#include <redis/handler.h>

namespace idlekv {

asio::awaitable<void> RedisHandler::handle(Connection& conn) {
    LOG(debug, "New connection from {}", conn.socket().remote_endpoint().address().to_string());

    char buff[1024];
    
    using msgChan = asiochan::channel<std::pair<std::string, bool>>;
    using doneChan = asiochan::channel<bool>;
    auto in = msgChan(), out = msgChan();
    auto doneCh = doneChan();

    // Ö´ÐÐÂß¼­
    asio::co_spawn(srv_->get_worker_pool(), parse_and_execute(in, out), asio::detached);

    // Ð´Âß¼­
    asio::co_spawn(
        srv_->get_io_context(),
        [&conn](msgChan out, doneChan doneCh) -> asio::awaitable<void> {
            try {
                for (;;) {
                    auto [response, done] = co_await out.read();
                    size_t n = co_await asio::async_write(conn.socket(), asio::buffer(response),
                                                          asio::use_awaitable);
                    LOG(debug, "send: {}", response);
                    if (done) {
                        break;
                    }
                }
            } catch (std::exception& e) {
                LOG(error, "Connection write exception: {}", e.what());
            }

            co_await doneCh.write(true);
        }(out, doneCh),
        asio::detached);
    
    // ¶ÁÂß¼­
    bool has_exception = false;
    try {
        for (;;) {
            auto n = co_await conn.socket().async_read_some(asio::buffer(buff, sizeof(buff) - 1),
                                                              asio::use_awaitable);
            if (n == 0) {
                LOG(info, "Connection closed by peer");
                co_await in.write({"", true});
                break;
            }
            co_await in.write({std::string(buff, n), false});
        }
    } catch (std::exception& e) {
        LOG(error, "Connection handle exception: {}", e.what());
        has_exception = true;
    }

    if (has_exception) {
        co_await out.write({"", true});
    }

    co_await doneCh.read();
    
    LOG(debug, "Connection from {} closed", conn.socket().remote_endpoint().address().to_string());
}

asio::awaitable<void>
RedisHandler::parse_and_execute(asiochan::channel<std::pair<std::string, bool>> in,
                                asiochan::channel<std::pair<std::string, bool>> out) {
    try {
        for (;;) {
            auto [cmd, done] = co_await in.read();
            LOG(debug, "Parsed command: {}", cmd);
            std::string response = "+OK\r\n";
            co_await out.write({response, true});
        }
    } catch (std::exception& e) {
        LOG(error, "Parse and execute exception: {}", e.what());
    }
}

asio::awaitable<void> RedisHandler::listen() {
    auto exec = co_await asio::this_coro::executor;
    asio::ip::tcp::acceptor acceptor(exec, ep_);

    LOG(info, "Handler {} listening on {}:{}", name(), ep_.address().to_string(), ep_.port());

    for (;;) {
        asio::ip::tcp::socket socket = co_await acceptor.async_accept(asio::use_awaitable);

        conns_.emplace_back(std::move(socket));
        asio::co_spawn(exec, handle(conns_.back()), asio::detached);
    }
}

} // namespace idlekv
