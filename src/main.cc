#include "common/config.h"
#include "common/logger.h"
#include "db/engine.h"
#include "redis/service.h"
#include "server/server.h"

#include <CLI11/CLI11.hpp>
#include <cstdio>
#include <memory>
#include <mimalloc.h>
#include <spdlog/spdlog.h>

std::string Banner() {
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

        if (cfg.HasConfigFile()) {
            cfg.ParseFromFile();
        } else {
            cfg.Parse(argc, argv);
        }

        spdlog::set_default_logger(idlekv::MakeDefaultLogger());

        auto srv = std::make_shared<idlekv::Server>(cfg);

        engine = std::make_unique<idlekv::IdleEngine>(cfg);
        engine->Init(srv->GetEventLoopPool());

        srv->RegisterHandler(std::make_unique<idlekv::RedisService>(cfg));
        // may be support rpc/http ?

        srv->ListenAndServe();
    } catch (const std::exception& e) {
        spdlog::error(e.what());
    }
}
