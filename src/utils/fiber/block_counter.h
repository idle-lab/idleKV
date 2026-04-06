#pragma once

#include "common/logger.h"

#include <boost/fiber/condition_variable.hpp>
#include <boost/fiber/detail/spinlock.hpp>

namespace idlekv::utils {

// A simple block counter that allows at most one waiter fiber to wait for the counter to reach zero.
class SingleWaiterBlockCounter {
public:
    auto Start(size_t count) -> void {
        CHECK(count > 0);
        std::unique_lock<boost::fibers::detail::spinlock> lk(state_splk_);

        CHECK_EQ(count_, size_t{0});
        count_ = count;
        ++generation_;
    }

    auto Done() -> void {
        bool notify = false;

        {
            std::unique_lock<boost::fibers::detail::spinlock> lk(state_splk_);

            CHECK_GT(count_, size_t{0});
            --count_;
            notify = count_ == 0;
        }

        if (notify) {
            cv_.notify_one();
        }
    }

    auto Wait() -> void {
        std::unique_lock<boost::fibers::detail::spinlock> lk(state_splk_);
        if (count_ == 0) {
            return;
        }

        CHECK(!waiter_active_);
        waiter_active_ = true;

        const size_t wait_generation = generation_;
        cv_.wait(lk, [&]() { return count_ == 0 || generation_ != wait_generation; });

        waiter_active_ = false;
    }

private:
    size_t count_{0};
    size_t generation_{0};
    bool   waiter_active_{false};

    boost::fibers::detail::spinlock    state_splk_;
    boost::fibers::condition_variable_any cv_;
};

} // namespace idlekv::utils
