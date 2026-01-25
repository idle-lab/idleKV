#include <server/server.h>
#include <asio/co_spawn.hpp>


namespace idlekv {

Server::Server(std::unique_ptr<Logger> lg, std::unique_ptr<ServerConfig> cfg) 
  : lg_(std::move(lg)), 
    workers(cfg->worker_threads) {
    // 1. 初始化

    // 2. 检查/创建数据文件夹

    // 3. 恢复数据

}



void Server::listen_and_server() {
    using namespace asio;

    co_spawn(this->io_context_, [this]() -> awaitable<void> {
      ip::tcp::endpoint ep{
            ip::make_address(this->cfg_->ip),
            this->cfg_->port
      };

      auto exec = co_await asio::this_coro::executor;
      ip::tcp::acceptor acceptor(exec, ep);


      for (;;) {
        ip::tcp::socket socket = co_await acceptor.async_accept(use_awaitable);

        co_await asio::post(this->workers, use_awaitable);

        auto exec = co_await asio::this_coro::executor;

      }

    }, detached);

    this->io_context_.run();
}

} // namespace idlekv
