#pragma once

#include <boost/assert.hpp>
#include <boost/fiber/condition_variable.hpp>
#include <boost/fiber/detail/spinlock.hpp>
#include <cstddef>
#include <memory>
#include <mutex>

namespace idlekv::utils {

// A simple block counter that allows at most one waiter fiber to wait for the counter to reach
// zero. Internal smart pointer for easier lifetime management. Pass by value.
class SingleWaiterBlockCounter {
public:
    SingleWaiterBlockCounter() : detail_(std::make_shared<detail>()) {}

    auto Start(size_t count) -> void {
        BOOST_ASSERT_MSG(count > 0, "SingleWaiterBlockCounter::Start requires count > 0");
        std::unique_lock<boost::fibers::detail::spinlock> lk(detail_->state_splk_);

        BOOST_ASSERT_MSG(detail_->count_ == size_t{0},
                         "SingleWaiterBlockCounter::Start requires an idle counter");
        detail_->count_ = count;
        ++detail_->generation_;
    }

    auto Done() -> void {
        bool notify = false;

        {
            std::unique_lock<boost::fibers::detail::spinlock> lk(detail_->state_splk_);

            BOOST_ASSERT_MSG(detail_->count_ > size_t{0},
                             "SingleWaiterBlockCounter::Done requires a positive counter");
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

        BOOST_ASSERT_MSG(!detail_->waiter_active_,
                         "SingleWaiterBlockCounter only supports one active waiter");
        detail_->waiter_active_ = true;

        const size_t wait_generation = detail_->generation_;
        detail_->cv_.wait(
            lk, [&]() { return detail_->count_ == 0 || detail_->generation_ != wait_generation; });

        BOOST_ASSERT_MSG(wait_generation == detail_->generation_,
                         "SingleWaiterBlockCounter generation changed while waiting");
        detail_->waiter_active_ = false;
    }

private:
    struct detail {
        size_t count_{0};
        size_t generation_{0};
        bool   waiter_active_{false};

        boost::fibers::detail::spinlock       state_splk_;
        boost::fibers::condition_variable_any cv_;
    };

    std::shared_ptr<detail> detail_;
};

} // namespace idlekv::utils
