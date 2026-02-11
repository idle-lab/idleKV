#include "common/config.h"
#include "common/logger.h"
#include "redis/handler.h"
#include "server/server.h"

#include <CLI11/CLI11.hpp>
#include <asio/asio.hpp>
#include <asiochan/asiochan.hpp>
#include <cstdio>
#include <memory>
#include <signal.h>
#include <spdlog/spdlog.h>

std::string banner() {
    return R"(
    _     _ _      _  ___     __
   (_) __| | | ___| |/ \ \   / /
   | |/ _` | |/ _ \ ' /\ \ / / 
   | | (_| | |  __/ . \ \ V /  
   |_|\__,_|_|\___|_|\_\ \_/   
)";
}

int main(int argc, char** argv) {
    try {
        idlekv::Config cfg;

        if (cfg.has_config_file()) {
            cfg.parse_from_file();
        } else {
            cfg.parse(argc, argv);
        }

        spdlog::set_default_logger(idlekv::make_default_logger());

        auto srv = std::make_shared<idlekv::Server>(cfg);

        srv->register_handler(std::make_shared<idlekv::RedisHandler>(cfg, srv));

        asio::signal_set signals(srv->get_io_context(), SIGINT, SIGTERM, SIGABRT);

        signals.async_wait([srv](const asio::error_code&, int) {
            spdlog::info("signal received, stopping server...");
            srv->stop();
        });

        srv->listen_and_server();
    } catch (const std::exception& e) {
        spdlog::error(e.what());
    }
}