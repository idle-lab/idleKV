#pragma once

#include <asio/io_context.hpp>
#include <cstdint>
#include <thread>
#include <vector>
namespace idlekv {

class IOPool {
public:

    auto run() -> void;



private:
    auto setup_threads() -> void;

    std::vector<asio::io_context> io_;

    std::vector<std::jthread> threads_;
    std::vector<uint32_t> cpus_;
};

} // namespace idlekv
