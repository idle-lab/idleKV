#pragma once

#include "common/logger.h"

#include <boost/fiber/condition_variable.hpp>
#include <boost/fiber/detail/spinlock.hpp>
#include <memory>

namespace idlekv::utils {

// A simple block counter that allows at most one waiter fiber to wait for the counter to reach zero.
// Internal smart pointer for easier lifetime management. Pass by value.
class SingleWaiterBlockCounter {
public:
    SingleWaiterBlockCounter() : detail_(std::make_shared<detail>()) {}

    auto Start(size_t count) -> void {
        CHECK(count > 0);
        std::unique_lock<boost::fibers::detail::spinlock> lk(detail_->state_splk_);

        CHECK_EQ(detail_->count_, size_t{0});
        detail_->count_ = count;
        ++detail_->generation_;
    }

    auto Done() -> void {
        bool notify = false;

        {
            std::unique_lock<boost::fibers::detail::spinlock> lk(detail_->state_splk_);

            CHECK_GT(detail_->count_, size_t{0});
            --detail_->count_;
            notify = detail_->count_ == 0;
        }

        if (notify) {
            detail_->cv_.notify_one();
        }
    }

    auto Wait() -> void {
        std::unique_lock<boost::fibers::detail::spinlock> lk(detail_->state_splk_);
        if (detail_->count_ == 0) {
            return;
        }

        CHECK(!detail_->waiter_active_);
        detail_->waiter_active_ = true;

        const size_t wait_generation = detail_->generation_;
        detail_->cv_.wait(lk, [&]() {
            return detail_->count_ == 0 || detail_->generation_ != wait_generation;
        });

        CHECK_EQ(wait_generation, detail_->generation_);
        detail_->waiter_active_ = false;
    }

private:
    struct detail {
        size_t count_{0};
        size_t generation_{0};
        bool   waiter_active_{false};

        boost::fibers::detail::spinlock    state_splk_;
        boost::fibers::condition_variable_any cv_;
    };

    std::shared_ptr<detail> detail_;
};

} // namespace idlekv::utils
