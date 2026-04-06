#pragma once

#include "common/config.h"
#include "server/el_pool.h"
#include "server/handler.h"

#include <atomic>
#include <string>

namespace idlekv {

class MetricsService : public Handler {
public:
    explicit MetricsService(const Config& cfg) : Handler(cfg.metrics_ip_, cfg.metrics_port_) {}

    auto Init(EventLoop* el) -> void override;
    auto Handle(asio::ip::tcp::socket socket) -> void override;

    auto Stop() -> void override { stop_.store(true, std::memory_order_release); }
    auto Name() -> std::string override { return "Metrics"; }

private:
    auto Stopped() const -> bool { return stop_.load(std::memory_order_acquire); }

    std::atomic<bool> stop_{false};
};

} // namespace idlekv
