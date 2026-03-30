#pragma once

#include <atomic>
#include <boost/fiber/condition_variable.hpp>
#include <boost/fiber/context.hpp>
#include <boost/fiber/detail/spinlock.hpp>
#include <boost/fiber/waker.hpp>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <utility>

namespace idlekv::utils {

class EventCount {
public:
    using cv_status = boost::fibers::cv_status;

    EventCount() noexcept : val_(0) {}

    class Key {
        friend class EventCount;

    public:
        Key(const Key&)                    = delete;
        auto operator=(const Key&) -> Key& = delete;

        Key(Key&& other) noexcept
            : me_(std::exchange(other.me_, nullptr)), epoch_(other.epoch_) {}

        auto operator=(Key&& other) noexcept -> Key& {
            if (this != &other) {
                Release();
                me_    = std::exchange(other.me_, nullptr);
                epoch_ = other.epoch_;
            }
            return *this;
        }

        ~Key() { Release(); }

        auto epoch() const -> uint32_t { return epoch_; }

    private:
        explicit Key(EventCount* me, uint32_t epoch) noexcept : me_(me), epoch_(epoch) {}

        auto Release() noexcept -> void {
            if (me_ == nullptr) {
                return;
            }
            me_->val_.fetch_sub(kAddWaiter, std::memory_order_relaxed);
            me_ = nullptr;
        }

        EventCount* me_{nullptr};
        uint32_t    epoch_{0};
    };

    auto notify() noexcept -> bool {
        uint64_t prev = val_.fetch_add(kAddEpoch, std::memory_order_acq_rel);
        if ((prev & kWaiterMask) == 0) {
            return false;
        }

        boost::fibers::detail::spinlock_lock lk{wait_queue_splk_};
        wait_queue_.notify_one();
        return true;
    }

    auto notifyAll() noexcept -> bool {
        uint64_t prev = val_.fetch_add(kAddEpoch, std::memory_order_acq_rel);
        if ((prev & kWaiterMask) == 0) {
            return false;
        }

        boost::fibers::detail::spinlock_lock lk{wait_queue_splk_};
        wait_queue_.notify_all();
        return true;
    }

    template <class Condition>
    auto await(Condition condition) -> bool {
        bool suspended = false;

        while (!condition()) {
            auto key = prepareWait();
            if (condition()) {
                return suspended;
            }
            suspended = wait(key.epoch()) || suspended;
        }

        return suspended;
    }

    template <class Condition>
    auto await_until(Condition condition, const std::chrono::steady_clock::time_point& tp)
        -> cv_status {
        while (!condition()) {
            auto key = prepareWait();
            if (condition()) {
                return cv_status::no_timeout;
            }

            if (wait_until(key.epoch(), tp) == cv_status::timeout) {
                return condition() ? cv_status::no_timeout : cv_status::timeout;
            }
        }

        return cv_status::no_timeout;
    }

    auto prepareWait() noexcept -> Key {
        uint64_t prev = val_.fetch_add(kAddWaiter, std::memory_order_acq_rel);
        return Key(this, static_cast<uint32_t>(prev >> kEpochShift));
    }

    auto wait(uint32_t epoch) noexcept -> bool {
        if (CurrentEpoch() != epoch) {
            return false;
        }

        auto* active_ctx = boost::fibers::context::active();

        boost::fibers::detail::spinlock_lock lk{wait_queue_splk_};
        if (CurrentEpoch() != epoch) {
            return false;
        }

        wait_queue_.suspend_and_wait(lk, active_ctx);
        return true;
    }

    auto wait_until(uint32_t epoch, const std::chrono::steady_clock::time_point& tp) noexcept
        -> cv_status {
        if (CurrentEpoch() != epoch) {
            return cv_status::no_timeout;
        }

        auto* active_ctx = boost::fibers::context::active();

        boost::fibers::detail::spinlock_lock lk{wait_queue_splk_};
        if (CurrentEpoch() != epoch) {
            return cv_status::no_timeout;
        }

        return wait_queue_.suspend_and_wait_until(lk, active_ctx, tp) ? cv_status::no_timeout
                                                                       : cv_status::timeout;
    }

private:
    auto CurrentEpoch() const noexcept -> uint32_t {
        return static_cast<uint32_t>(val_.load(std::memory_order_acquire) >> kEpochShift);
    }

    static constexpr uint64_t kAddWaiter  = 1ULL;
    static constexpr size_t   kEpochShift = 32;
    static constexpr uint64_t kAddEpoch   = 1ULL << kEpochShift;
    static constexpr uint64_t kWaiterMask = kAddEpoch - 1;

    std::atomic_uint64_t            val_;
    boost::fibers::detail::spinlock wait_queue_splk_{};
    boost::fibers::wait_queue       wait_queue_{};
};

} // namespace idlekv::utils
