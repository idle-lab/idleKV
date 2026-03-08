#include <common/logger.h>

namespace idlekv {

std::shared_ptr<spdlog::logger> make_default_logger() {
    auto lg = std::make_shared<spdlog::logger>(
        "default", std::make_shared<spdlog::sinks::stdout_color_sink_mt>());
#ifdef NDEBUG
    lg->set_level(spdlog::level::debug);
#else
    lg->set_level(spdlog::level::debug);
#endif
    return lg;
}

} // namespace idlekv
