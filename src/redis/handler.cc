#include <redis/handler.h>

namespace idlekv {

asio::awaitable<void> RedisHandler::handle(Connection& conn) {
    try {
        LOG(debug, "New connection from {}", conn.socket().remote_endpoint().address().to_string());

        char buff[1024];

        for (;;) {
            size_t n = co_await conn.socket().async_read_some(asio::buffer(buff, sizeof(buff) - 1),
                                                              asio::use_awaitable);
            buff[n]  = '\0';

            asio::post(srv_->get_worker_pool(),
                       [buff = std::string(buff), n]() { LOG(debug, "recive: {}", buff); });
        }

    } catch (std::exception& e) {
        LOG(error, "Connection handle exception: {}", e.what());
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
