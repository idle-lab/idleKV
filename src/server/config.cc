#include <server/config.h>

namespace idlekv {

std::unique_ptr<ServerConfig> ServerConfig::build(const Config& cfg) {
    auto scfg = std::make_unique<ServerConfig>();

    scfg->ip = cfg.ip_;
    scfg->port = std::atoi(cfg.port_.c_str());

    return scfg;
}

    
} // namespace idlekv
