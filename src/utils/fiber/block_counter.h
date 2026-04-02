#pragma once

#include "common/logger.h"
#include <atomic>
#include <boost/fiber/context.hpp>
#include <boost/fiber/mutex.hpp>
#include <boost/fiber/operations.hpp>
#include <mutex>

namespace idlekv::utils {

// A simple block counter that allows at most one waiter fiber to wait for the counter to reach zero.
class SingleWaiterBlockCounter {
public:
    auto Start(size_t count) -> void {
        CHECK(count > 0);
        CHECK(count_.load(std::memory_order_acquire) == 0);
        count_.store(count, std::memory_order_release);
    }

    auto Done() -> void {
        size_t prev = count_.fetch_sub(1, std::memory_order_acq_rel);
        CHECK(prev > 0);
        if (prev == 1) {
            waiting_ctx_splk_.lock();
            auto* ctx = waiting_ctx_;
            waiting_ctx_ = nullptr;
            waiting_ctx_splk_.unlock();

            if (ctx != nullptr) {
                boost::fibers::context::active()->schedule(ctx);
            }
        }
    }

    auto Wait() -> void {
        if (count_.load(std::memory_order_acquire) == 0) {
            return;
        }

        waiting_ctx_splk_.lock();
        std::unique_lock<boost::fibers::detail::spinlock> lk(waiting_ctx_splk_);

        if (count_.load(std::memory_order_acquire) == 0) {
            waiting_ctx_splk_.unlock();
            return;
        }

        CHECK(waiting_ctx_ == nullptr);
        waiting_ctx_ = boost::fibers::context::active();
        waiting_ctx_->suspend(lk);
    }

private:
    std::atomic<size_t> count_{0};

    boost::fibers::context* waiting_ctx_{nullptr};
    boost::fibers::detail::spinlock waiting_ctx_splk_;
};

} // namespace idlekv::utils