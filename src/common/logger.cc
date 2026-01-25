#include <common/logger.h>

namespace idlekv {

std::unique_ptr<Logger> make_default_logger() {
    auto sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();

    return std::make_unique<Logger>("default", sink);
}

} // namespace idlekv 
