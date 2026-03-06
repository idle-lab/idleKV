
#include "server/el_pool.h"

#include "common/logger.h"
#include "utils/cpu/basic.h"

#include <asio/co_spawn.hpp>
#include <asio/use_future.hpp>
#include <atomic>
#include <cassert>
#include <iostream>
#include <memory>
#include <pthread.h>
#include <sched.h>
#include <spdlog/spdlog.h>
#include <thread>

namespace idlekv {

auto EventLoop::run() -> void {
    th_ = std::jthread([this]() mutable { io_.run(); });
}

auto EventLoop::stop() -> void {
    io_.stop();
    wg_.reset();
}

auto EventLoopPool::run() -> void {
    setup_els();

    is_running_.store(true, std::memory_order_release);
    LOG(info, "Running {} io threads", pool_size_);
}

auto EventLoopPool::stop() -> void {
    is_running_.store(false, std::memory_order_release);

    for (auto& el : els_) {
        el->stop();
    }
}

auto EventLoopPool::pick_up_el() -> EventLoop* {
    auto idx = next_el_.load(std::memory_order_relaxed);

    auto el = els_[idx++].get();

    if (idx >= pool_size_) {
        idx = 0;
    }

    next_el_.store(idx, std::memory_order_relaxed);
    return el;
}

auto EventLoopPool::setup_els() -> void {
    auto     online_cpus     = utils::get_online_cpus();
    unsigned num_online_cpus = CPU_COUNT(&online_cpus);

    std::vector<unsigned> rel_to_abs_cpu(num_online_cpus, 0);
    unsigned              rel_cpu_index = 0, abs_cpu_index = 0;

    for (; abs_cpu_index < CPU_SETSIZE; abs_cpu_index++) {
        if (CPU_ISSET(abs_cpu_index, &online_cpus)) {
            rel_to_abs_cpu[rel_cpu_index] = abs_cpu_index;
            rel_cpu_index++;

            if (rel_cpu_index == num_online_cpus)
                break;
        }
    }

    cpu_set_t cps;
    CPU_ZERO(&cps);
    cpu_threads_.resize(abs_cpu_index + 1);
    els_.resize(pool_size_);

    for (unsigned i = 0; i < pool_size_; ++i) {
        int      rel_indx = i % num_online_cpus;
        unsigned abs_cpu  = rel_to_abs_cpu[rel_indx];
        els_[i]           = std::make_unique<EventLoop>(abs_cpu);

        els_[i]->run();

        pthread_t tid = els_[i]->thread_id();

        CPU_SET(abs_cpu, &cps);

        int rc = pthread_setaffinity_np(tid, sizeof(cpu_set_t), &cps);
        if (rc == 0) {
            LOG(debug, "Setting affinity of thread {} on cpu {}", i, abs_cpu);
            cpu_threads_[abs_cpu].push_back(i);
        } else {
            LOG(warn, "Error calling pthread_setaffinity_np: {}", strerror(rc));
        }

        CPU_CLR(abs_cpu, &cps);
    }
}

} // namespace idlekv
