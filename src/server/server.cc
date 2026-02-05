#include <server/server.h>
#include <asio/co_spawn.hpp>


namespace idlekv {

Server::Server(const Config& cfg) 
  : io_context_(cfg.io_threads_),
    workers(cfg.worker_threads_) {
    // 1. 初始化
    cfg_ = ServerConfig::build(cfg);
    // 2. 检查/创建数据文件夹

    // 3. 恢复数据

}


void Server::listen_and_server() {
    LOG(info, "start server");
    io_context_.run();
}

void Server::register_handler(std::shared_ptr<Handler> handler) {
    handlers_.push_back(handler);
    LOG(info, "register handler: {}, {}:{}", handler->name(), handler->endpoint().address().to_string(), handler->endpoint().port());
    asio::co_spawn(io_context_, handler->start(), asio::detached);
}

void Server::stop() { 
    io_context_.stop(); 
    workers.join();

    for (auto& handler : handlers_) {
        handler->stop();
    }

    LOG(info, "server stopped");
}

} // namespace idlekv
