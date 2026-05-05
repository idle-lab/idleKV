#include "common/logger.h"

#include "common/config.h"

#include <algorithm>
#include <cctype>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <spdlog/common.h>
#include <spdlog/sinks/basic_file_sink.h>

namespace idlekv {

namespace {

auto DefaultLogLevel() -> spdlog::level::level_enum {
#ifdef NDEBUG
    return spdlog::level::info;
#else
    return spdlog::level::debug;
#endif
}

auto ParseLogLevel(std::string_view level_name) -> spdlog::level::level_enum {
    if (level_name.empty()) {
        return DefaultLogLevel();
    }

    std::string normalized(level_name);
    std::transform(normalized.begin(), normalized.end(), normalized.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });

    if (normalized == "warning") {
        normalized = "warn";
    }

    const auto level = spdlog::level::from_str(normalized);
    if (level == spdlog::level::off && normalized != "off") {
        throw std::invalid_argument("invalid --log-level value: " + std::string(level_name));
    }

    return level;
}

} // namespace

std::shared_ptr<spdlog::logger> MakeDefaultLogger(const Config& cfg) {
    std::vector<spdlog::sink_ptr> sinks;
    sinks.push_back(std::make_shared<spdlog::sinks::stdout_color_sink_mt>());

    if (!cfg.log_file_.empty()) {
        sinks.push_back(std::make_shared<spdlog::sinks::basic_file_sink_mt>(cfg.log_file_, true));
    }

    auto lg = std::make_shared<spdlog::logger>("default", sinks.begin(), sinks.end());
    lg->set_level(ParseLogLevel(cfg.log_level_));
    lg->flush_on(spdlog::level::debug);
    return lg;
}

} // namespace idlekv
