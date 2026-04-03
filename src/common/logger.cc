#include <common/logger.h>
#include <spdlog/common.h>

namespace idlekv {

std::shared_ptr<spdlog::logger> MakeDefaultLogger() {
    auto lg = std::make_shared<spdlog::logger>(
        "default", std::make_shared<spdlog::sinks::stdout_color_sink_mt>());
#ifdef NDEBUG
    lg->set_level(spdlog::level::info);
#else
    lg->set_level(spdlog::level::debug);
#endif
    return lg;
}

} // namespace idlekv
