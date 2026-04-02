#include "banner.h"
#include "common/config.h"
#include "db/engine.h"
#include "redis/service.h"
#include "server/metrics_service.h"
#include "server/server.h"

#include <CLI11/CLI11.hpp>
#include <cstdio>
#include <memory>
#include <mimalloc.h>
#include <spdlog/spdlog.h>

std::string Banner() {
    return R"(
    ___      _ _         _  ____     __
   |_ _|  __| | |  ___  | |/ /\ \   / /
    | |  / _` | | / _ \ | ' /  \ \ / / 
    | | | (_| | |/  __/ | . \   \ V /  
   |___| \__,_|_|\___|  |_|\_\   \_/   
                                       
    >> High Performance Key-Value Store <<
)";
}

using namespace idlekv;

int main(int argc, char** argv) {
    Console::PrintGradient(Banner());

    try {
        idlekv::Config cfg;

        if (cfg.HasConfigFile()) {
            cfg.ParseFromFile();
        } else {
            cfg.Parse(argc, argv);
        }

        spdlog::set_default_logger(idlekv::MakeDefaultLogger());

#ifdef NDEBUG
        LOG(info, "you are running in release mode");
#elif
        LOG(warn, "you are running in debug mode, performance may be significantly degraded");
#endif

        auto srv = std::make_shared<idlekv::Server>(cfg);

        engine = std::make_unique<idlekv::IdleEngine>(cfg);
        engine->Init(srv->GetEventLoopPool());

        srv->RegisterHandler(std::make_unique<idlekv::RedisService>(cfg));
        if (cfg.metrics_port_ != 0) {
            srv->RegisterHandler(std::make_unique<idlekv::MetricsService>(cfg));
        }
        // may be support rpc/http ?

        srv->ListenAndServe();
    } catch (const std::exception& e) {
        spdlog::error(e.what());
    }
}
