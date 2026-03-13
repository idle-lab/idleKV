#include "common/config.h"
#include "common/logger.h"
#include "db/engine.h"
#include "redis/service.h"
#include "server/server.h"

#include <CLI11/CLI11.hpp>
#include <asio/asio.hpp>
#include <asiochan/asiochan.hpp>
#include <cstdio>
#include <memory>
#include <mimalloc.h>
#include <spdlog/spdlog.h>

std::string banner() {
    return R"(
    _     _ _      _  ___     __
   (_) __| | | ___| |/ \ \   / /
   | |/ _` | |/ _ \ ' / \ \ / / 
   | | (_| | |  __/ . \  \ V /  
   |_|\__,_|_|\___|_|\_\  \_/   
)";
}

using namespace idlekv;

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

        engine = std::make_unique<idlekv::IdleEngine>(cfg);
        engine->init(srv->event_loop_pool());

        srv->register_handler(std::make_unique<idlekv::RedisService>(cfg));
        // may be support rpc/http ?

        srv->listen_and_server();
    } catch (const std::exception& e) {
        spdlog::error(e.what());
    }
}