#include "server/config.h"

#include <algorithm>

namespace idlekv {

std::unique_ptr<ServerConfig> ServerConfig::build(const Config& cfg) {
    auto scfg = std::make_unique<ServerConfig>();

    scfg->ip   = cfg.ip_;
    scfg->port = std::atoi(cfg.port_.c_str());

    scfg->io_threads     = std::max(uint16_t(1), cfg.io_threads_);
    scfg->worker_threads = std::max(uint16_t(1), cfg.worker_threads_);

    return scfg;
}

} // namespace idlekv
