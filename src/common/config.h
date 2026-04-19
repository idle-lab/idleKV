#pragma once

#include <CLI11/CLI11.hpp>
#include <cstdint>
#include <spdlog/spdlog.h>
#include <string>

const std::string SERVER_NAME  = "idlekv";
const std::string DEFAULT_PORT = "4396";

constexpr uint64_t B  = 1;
constexpr uint64_t KB = 1024 * B;
constexpr uint64_t MB = 1024 * KB;
constexpr uint64_t GB = 1024 * MB;

#define DISCARD_RESULT(expr) void(expr);

#if defined(__GNUC__) || defined(__clang__)
#define UNREACHABLE() __builtin_unreachable()
#elif defined(_MSC_VER)
#define UNREACHABLE() __assume(0)
#else
#define UNREACHABLE() ((void)0)
#endif

#if defined(__GNUC__) || defined(__clang__)
#define LIKELY(x) (__builtin_expect(!!(x), 1))
#define UNLIKELY(x) (__builtin_expect(!!(x), 0))
#else
#define LIKELY(x) (!!(x))
#define UNLIKELY(x) (!!(x))
#endif

#if defined(__GNUC__) || defined(__clang__)
#define PREFETCH(p) __builtin_prefetch(p)
#define PREFETCH_R(p, locality) __builtin_prefetch(p, 0, locality)
#define PREFETCH_W(p, locality) __builtin_prefetch(p, 1, locality)
#else
#define PREFETCH(p)
#define PREFETCH_R(p)
#define PREFETCH_W(p)
#endif

#define DISABLE_COPY(type)                                                                         \
    type(const type&)            = delete;                                                         \
    type& operator=(const type&) = delete;

#define DISABLE_MOVE(type)                                                                         \
    type(type&&)            = delete;                                                              \
    type& operator=(type&&) = delete;

#define DISABLE_COPY_MOVE(type)                                                                    \
    DISABLE_COPY(type)                                                                             \
    DISABLE_MOVE(type)

namespace idlekv {

class Config {
public:
    Config() : opts_(SERVER_NAME) {
        opts_.add_option("--ip", ip_, "Listen IP")->default_val("0.0.0.0");
        opts_.add_option("--port", port_, "Listen port")->default_val(DEFAULT_PORT);
        opts_.add_option("--metrics-ip", metrics_ip_, "Prometheus listen IP")
            ->default_val("0.0.0.0");
        opts_.add_option("--metrics-port", metrics_port_, "Prometheus metrics port")
            ->default_val(9108);
        opts_.add_option("--log-file", log_file_, "Write logs to this file");
        opts_.add_option("--log-level", log_level_,
                         "Log level: trace, debug, info, warn, error, critical, off");

        opts_.add_option("-c,--config", config_file_path, "Config file path");
        opts_.add_option("--DbNum", db_num_, "number of DB");
    }

    DISABLE_COPY_MOVE(Config);

    void Parse(int argc, char** argv) {
        try {
            opts_.parse(argc, argv);
        } catch (const CLI::CallForHelp&) {
            std::cout << opts_.help() << '\n';
            std::exit(0);
        } catch (const CLI::CallForAllHelp&) {
            std::cout << opts_.help("", CLI::AppFormatMode::All) << '\n';
            std::exit(0);
        }
    }

    void ParseFromFile() { throw std::runtime_error("Not yet implemented"); }

    bool HasConfigFile() const { return !config_file_path.empty(); }

    std::string ip_, port_;
    std::string metrics_ip_;
    std::string log_file_;
    std::string log_level_;
    uint16_t    io_threads_     = 1;
    uint16_t    worker_threads_ = 0;
    uint16_t    metrics_port_   = 0;

    std::string config_file_path;

    uint8_t db_num_{16};

    uint8_t shard_num_{6};

private:
    CLI::App opts_;
};

} // namespace idlekv
