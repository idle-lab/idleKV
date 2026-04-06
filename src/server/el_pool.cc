
#include "server/el_pool.h"

#include "common/logger.h"
#include "server/fiber_runtime.h"
#include "utils/cpu/basic.h"

#include <atomic>
#include <boost/fiber/fiber.hpp>
#include <boost/fiber/operations.hpp>
#include <boost/fiber/scheduler.hpp>
#include <cassert>
#include <pthread.h>
#include <sched.h>
#include <spdlog/spdlog.h>

namespace idlekv {

auto EventLoop::Run() -> void {
    th_ = std::jthread([this]() mutable {
        io_.restart();
        boost::fibers::use_scheduling_algorithm<idlekv::Priority>(io_);

        auto& prop = boost::this_fiber::properties<FiberProps>();
        prop.SetName("EventLoopFiber");
        prop.SetPriority(FiberPriority::BACKGROUND);

        // main fiber loop: run the io_context and yield to ready fibers when possible.
        while (!io_.stopped()) {
            if (boost::fibers::has_ready_fibers()) {
                while (io_.poll())
                    ;

                // yield this fiber to processe pending (ready) fibers.
                boost::this_fiber::yield();
            } else {
                if (!io_.run_one()) {
                    break;
                }
            }
        }
    });
}

auto EventLoop::Stop() -> void {
    io_.stop();
    // if (th_.joinable()) {
    //     th_.join();
    // }
}

auto EventLoopPool::Run() -> void {
    // create and start all worker event loops before marking the pool ready.
    SetupEls();

    is_running_.store(true, std::memory_order_release);
    LOG(info, "Running {} io threads", pool_size_);
}

auto EventLoopPool::Stop() -> void {
    is_running_.store(false, std::memory_order_release);

    for (auto& el : els_) {
        el->Stop();
    }
}

auto EventLoopPool::PickUpEl() -> EventLoop* {
    // use a simple round-robin cursor to spread connections across loops.
    auto idx = next_el_.load(std::memory_order_relaxed);

    auto el = els_[idx++].get();

    if (idx >= pool_size_) {
        idx = 0;
    }

    next_el_.store(idx, std::memory_order_relaxed);
    return el;
}

auto EventLoopPool::SetupEls() -> void {
    // build a relative-to-absolute cpu map from the current online cpu set.
    auto     online_cpus     = utils::GetOnlineCpus();
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
        // pin each event loop thread to a cpu to keep scheduling predictable.
        int      rel_indx = i % num_online_cpus;
        unsigned abs_cpu  = rel_to_abs_cpu[rel_indx];
        els_[i]           = std::make_unique<EventLoop>(abs_cpu, i);

        els_[i]->Run();

        pthread_t tid = els_[i]->ThreadId();

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
