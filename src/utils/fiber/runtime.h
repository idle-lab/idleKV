//          Copyright 2003-2013 Christopher M. Kohlhoff
//          Copyright Oliver Kowalke, Nat Goodspeed 2015.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)
// modified for boost.asio >= 1.70
#pragma once

#include "absl/base/internal/cycleclock.h"

#include <atomic>
#include <boost/asio/execution_context.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/assert.hpp>
#include <boost/fiber/algo/algorithm.hpp>
#include <boost/fiber/condition_variable.hpp>
#include <boost/fiber/context.hpp>
#include <boost/fiber/fiber.hpp>
#include <boost/fiber/future.hpp>
#include <boost/fiber/mutex.hpp>
#include <boost/fiber/operations.hpp>
#include <boost/fiber/scheduler.hpp>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

namespace idlekv {

enum class FiberPriority : uint8_t {
    NORMAL     = 0, // default priority
    BACKGROUND = 1, // background priority, runs when no NORMAL fibers are ready.
    HIGH       = 2, // high priority, scheduled before other worker fibers.
};

class FiberProps : public boost::fibers::fiber_properties {
public:
    explicit FiberProps(boost::fibers::context* ctx,
                        FiberPriority           priority = FiberPriority::NORMAL) noexcept
        : boost::fibers::fiber_properties(ctx), priority_(priority) {}

    FiberProps(std::string name, FiberPriority priority = FiberPriority::NORMAL) noexcept
        : boost::fibers::fiber_properties(nullptr), priority_(priority), name_(std::move(name)) {}

    auto Priority() const noexcept -> FiberPriority { return priority_; }

    auto SetPriority(FiberPriority priority) noexcept -> void {
        if (priority_ == priority) {
            return;
        }

        priority_ = priority;
        notify();
    }

    auto Name() const noexcept -> const std::string& { return name_; }
    auto SetName(std::string name) noexcept -> void { name_ = std::move(name); }

    uint64_t last_resume_cycle{0};
    uint64_t switch_count{0};

private:
    FiberPriority priority_{FiberPriority::NORMAL};
    std::string   name_;
};

class FiberCycleClock {
public:
    // Returns the current value of the cycle counter.
    static uint64_t Now() {
#if defined(__x86_64__)
        unsigned long low, high;
        __asm__ volatile("rdtsc" : "=a"(low), "=d"(high));
        return (static_cast<uint64_t>(high) << 32) + low;
#elif defined(__aarch64__)
        int64_t tv;
        asm volatile("mrs %0, cntvct_el0" : "=r"(tv));
        return tv;
#else
        return absl::base_internal::CycleClock::Now();
#endif
    }

    static uint64_t Frequency() {
        uint64_t frequency_;
#ifdef __aarch64__
        // On aarch64, we can read the frequency from the cntfrq_el0 register.
        uint64_t res;
        __asm__ volatile("mrs %0, cntfrq_el0" : "=r"(res));
        frequency_ = res;
#else
        frequency_ = absl::base_internal::CycleClock::Frequency();
#endif
        return frequency_;
    }
};

class Priority : public boost::fibers::algo::algorithm_with_properties<idlekv::FiberProps> {
private:
    using ready_queue_type = boost::fibers::scheduler::ready_queue_type;

    class service : public boost::asio::io_context::service {
    public:
        static boost::asio::io_context::id id;

        explicit service(boost::asio::io_context& io_ctx)
            : boost::asio::io_context::service(io_ctx),
              work_(std::make_unique<boost::asio::io_context::work>(io_ctx)) {}

    private:
        void shutdown_service() override { work_.reset(); }

        std::unique_ptr<boost::asio::io_context::work> work_;
    };

public:
    explicit Priority(boost::asio::io_context& io_ctx) : suspend_timer_(io_ctx) {
        boost::asio::add_service(io_ctx, new service(io_ctx));
    }

    void property_change(boost::fibers::context* ctx, idlekv::FiberProps& props) noexcept override {
        BOOST_ASSERT(nullptr != ctx);
        if (ctx->is_context(boost::fibers::type::dispatcher_context) || !ctx->ready_is_linked()) {
            return;
        }

        ctx->ready_unlink();
        ctx->ready_link(ReadyQueueFor(ctx, &props));
    }

    void awakened(boost::fibers::context* ctx, idlekv::FiberProps& prop) noexcept override {
        BOOST_ASSERT(nullptr != ctx);
        BOOST_ASSERT(!ctx->ready_is_linked());
        ctx->ready_link(ReadyQueueFor(ctx, &prop));
        if (!ctx->is_context(boost::fibers::type::dispatcher_context)) {
            ++counter_;
        }
    }

    auto pick_next() noexcept -> boost::fibers::context* override {
        AccountRunningFiber();

        if (auto* ctx = PickNextFrom(high_queue_); ctx != nullptr) {
            return ctx;
        }

        if (auto* ctx = PickNextFrom(normal_queue_); ctx != nullptr) {
            return ctx;
        }

        if (auto* ctx = PickNextFrom(background_queue_); ctx != nullptr) {
            return ctx;
        }

        return nullptr;
    }

    auto has_ready_fibers() const noexcept -> bool override { return counter_ > 0; }

    void suspend_until(std::chrono::steady_clock::time_point const& abs_time) noexcept override {
        if (abs_time != std::chrono::steady_clock::time_point::max()) {
            suspend_timer_.expires_at(abs_time);
            suspend_timer_.async_wait([](boost::system::error_code const& ec) {
                if (!ec) {
                    boost::this_fiber::yield();
                }
            });
        }
    }

    void notify() noexcept override {
        suspend_timer_.async_wait(
            [](boost::system::error_code const&) { boost::this_fiber::yield(); });
        suspend_timer_.expires_at(std::chrono::steady_clock::now());
    }

private:
    auto ReadyQueueFor(boost::fibers::context* ctx, idlekv::FiberProps const* props) noexcept
        -> ready_queue_type& {
        if (ctx->is_context(boost::fibers::type::dispatcher_context)) {
            return background_queue_;
        }

        if (props == nullptr) {
            return normal_queue_;
        }

        switch (props->Priority()) {
        case idlekv::FiberPriority::HIGH:
            return high_queue_;
        case idlekv::FiberPriority::BACKGROUND:
            return background_queue_;
        case idlekv::FiberPriority::NORMAL:
        default:
            return normal_queue_;
        }
    }

    auto PickNextFrom(ready_queue_type& queue) noexcept -> boost::fibers::context* {
        if (queue.empty()) {
            return nullptr;
        }

        auto* ctx = &queue.front();
        queue.pop_front();

        BOOST_ASSERT(nullptr != ctx);
        BOOST_ASSERT(boost::fibers::context::active() != ctx);

        if (!ctx->is_context(boost::fibers::type::dispatcher_context)) {
            --counter_;
            MarkScheduledIn(ctx);
        }

        return ctx;
    }

    auto PropsOf(boost::fibers::context* ctx) noexcept -> idlekv::FiberProps* {
        if (ctx == nullptr || !ctx->is_context(boost::fibers::type::worker_context)) {
            return nullptr;
        }

        auto* raw = ctx->get_properties();
        return raw == nullptr ? nullptr : static_cast<idlekv::FiberProps*>(raw);
    }

    void AccountRunningFiber() noexcept {
        auto* props = PropsOf(running_ctx_);
        if (props == nullptr || running_since_cycle_ == 0) {
            running_ctx_         = nullptr;
            running_since_cycle_ = 0;
            return;
        }

        ++props->switch_count;
        props->last_resume_cycle = 0;
        running_ctx_             = nullptr;
        running_since_cycle_     = 0;
    }

    void MarkScheduledIn(boost::fibers::context* ctx) noexcept {
        auto* props = PropsOf(ctx);
        if (props == nullptr) {
            return;
        }

        const uint64_t now       = idlekv::FiberCycleClock::Now();
        props->last_resume_cycle = now;

        running_ctx_         = ctx;
        running_since_cycle_ = now;
    }

    ready_queue_type          high_queue_{};
    ready_queue_type          normal_queue_{};
    ready_queue_type          background_queue_{};
    boost::asio::steady_timer suspend_timer_;
    boost::fibers::mutex      mtx_{};
    std::size_t               counter_{0};

    boost::fibers::context* running_ctx_{nullptr};
    uint64_t                running_since_cycle_{0};
};

} // namespace idlekv

namespace boost::fibers {

struct no_op_lock {
    void lock() {}
    void unlock() {}

    bool try_lock() { return true; }
};

} // namespace boost::fibers

namespace boost::fibers::asio {

//[fibers_asio_yield_t
class yield_t {
public:
    yield_t() = default;

    /**
     * @code
     * static yield_t yield;
     * boost::system::error_code myec;
     * func(yield[myec]);
     * @endcode
     * @c yield[myec] returns an instance of @c yield_t whose @c ec_ points
     * to @c myec. The expression @c yield[myec] "binds" @c myec to that
     * (anonymous) @c yield_t instance, instructing @c func() to store any
     * @c error_code it might produce into @c myec rather than throwing @c
     * boost::system::system_error.
     */
    yield_t operator[](boost::system::error_code& ec) const {
        yield_t tmp;
        tmp.ec_ = &ec;
        return tmp;
    }

    // private:
    //  ptr to bound error_code instance if any
    boost::system::error_code* ec_{nullptr};
};
//]

//[fibers_asio_yield
// canonical instance
inline thread_local yield_t yield{};
//]

namespace detail {

//[fibers_asio_yield_completion
// Bundle a completion bool flag with a spinlock to protect it.
struct yield_completion {
    enum state_t { init, waiting, complete };

    typedef fibers::detail::spinlock               mutex_t;
    typedef std::unique_lock<mutex_t>              lock_t;
    typedef boost::intrusive_ptr<yield_completion> ptr_t;

    std::atomic<std::size_t> use_count_{0};
    mutex_t                  mtx_{};
    state_t                  state_{init};

    void wait() {
        // yield_handler_base::operator()() will set state_ `complete` and
        // attempt to wake a suspended fiber. It would be Bad if that call
        // happened between our detecting (complete != state_) and suspending.
        lock_t lk{mtx_};
        // If state_ is already set, we're done here: don't suspend.
        if (complete != state_) {
            state_ = waiting;
            // suspend(unique_lock<spinlock>) unlocks the lock in the act of
            // resuming another fiber
            fibers::context::active()->suspend(lk);
        }
    }

    friend void intrusive_ptr_add_ref(yield_completion* yc) noexcept {
        BOOST_ASSERT(nullptr != yc);
        yc->use_count_.fetch_add(1, std::memory_order_relaxed);
    }

    friend void intrusive_ptr_release(yield_completion* yc) noexcept {
        BOOST_ASSERT(nullptr != yc);
        if (1 == yc->use_count_.fetch_sub(1, std::memory_order_release)) {
            std::atomic_thread_fence(std::memory_order_acquire);
            delete yc;
        }
    }
};
//]

//[fibers_asio_yield_handler_base
// This class encapsulates common elements between yield_handler<T> (capturing
// a value to return from asio async function) and yield_handler<void> (no
// such value). See yield_handler<T> and its <void> specialization below. Both
// yield_handler<T> and yield_handler<void> are passed by value through
// various layers of asio functions. In other words, they're potentially
// copied multiple times. So key data such as the yield_completion instance
// must be stored in our async_result<yield_handler<>> specialization, which
// should be instantiated only once.
class yield_handler_base {
public:
    yield_handler_base(yield_t const& y)
        : // capture the context* associated with the running fiber
          ctx_{boost::fibers::context::active()},
          // capture the passed yield_t
          yt_(y) {}

    // completion callback passing only (error_code)
    void operator()(boost::system::error_code const& ec) {
        BOOST_ASSERT_MSG(ycomp_, "Must inject yield_completion* "
                                 "before calling yield_handler_base::operator()()");
        BOOST_ASSERT_MSG(yt_.ec_, "Must inject boost::system::error_code* "
                                  "before calling yield_handler_base::operator()()");
        // If originating fiber is busy testing state_ flag, wait until it
        // has observed (completed != state_).
        yield_completion::lock_t  lk{ycomp_->mtx_};
        yield_completion::state_t state = ycomp_->state_;
        // Notify a subsequent yield_completion::wait() call that it need not
        // suspend.
        ycomp_->state_ = yield_completion::complete;
        // set the error_code bound by yield_t
        *yt_.ec_ = ec;
        // unlock the lock that protects state_
        lk.unlock();
        // If ctx_ is still active, e.g. because the async operation
        // immediately called its callback (this method!) before the asio
        // async function called async_result_base::get(), we must not set it
        // ready.
        if (yield_completion::waiting == state) {
            // wake the fiber
            fibers::context::active()->schedule(ctx_);
        }
    }

    // private:
    boost::fibers::context* ctx_;
    yield_t                 yt_;
    // We depend on this pointer to yield_completion, which will be injected
    // by async_result.
    yield_completion::ptr_t ycomp_{};
};
//]

//[fibers_asio_yield_handler_T
// asio uses handler_type<completion token type, signature>::type to decide
// what to instantiate as the actual handler. Below, we specialize
// handler_type< yield_t, ... > to indicate yield_handler<>. So when you pass
// an instance of yield_t as an asio completion token, asio selects
// yield_handler<> as the actual handler class.
template <typename T>
class yield_handler : public yield_handler_base {
public:
    // asio passes the completion token to the handler constructor
    explicit yield_handler(yield_t const& y) : yield_handler_base{y} {}

    // completion callback passing only value (T)
    void operator()(T t) {
        // just like callback passing success error_code
        (*this)(boost::system::error_code(), std::move(t));
    }

    // completion callback passing (error_code, T)
    void operator()(boost::system::error_code const& ec, T t) {
        BOOST_ASSERT_MSG(value_, "Must inject value ptr "
                                 "before caling yield_handler<T>::operator()()");
        // move the value to async_result<> instance BEFORE waking up a
        // suspended fiber
        *value_ = std::move(t);
        // forward the call to base-class completion handler
        yield_handler_base::operator()(ec);
    }

    // private:
    //  pointer to destination for eventual value
    //  this must be injected by async_result before operator()() is called
    T* value_{nullptr};
};
//]

//[fibers_asio_yield_handler_void
// yield_handler<void> is like yield_handler<T> without value_. In fact it's
// just like yield_handler_base.
template <>
class yield_handler<void> : public yield_handler_base {
public:
    explicit yield_handler(yield_t const& y) : yield_handler_base{y} {}

    // nullary completion callback
    void operator()() { (*this)(boost::system::error_code()); }

    // inherit operator()(error_code) overload from base class
    using yield_handler_base::operator();
};
//]

// Specialize asio_handler_invoke hook to ensure that any exceptions thrown
// from the handler are propagated back to the caller
template <typename Fn, typename T>
void asio_handler_invoke(Fn&& fn, yield_handler<T>*) {
    fn();
}

//[fibers_asio_async_result_base
// Factor out commonality between async_result<yield_handler<T>> and
// async_result<yield_handler<void>>
class async_result_base {
public:
    explicit async_result_base(yield_handler_base& h) : ycomp_{new yield_completion{}} {
        // Inject ptr to our yield_completion instance into this
        // yield_handler<>.
        h.ycomp_ = this->ycomp_;
        // if yield_t didn't bind an error_code, make yield_handler_base's
        // error_code* point to an error_code local to this object so
        // yield_handler_base::operator() can unconditionally store through
        // its error_code*
        if (!h.yt_.ec_) {
            h.yt_.ec_ = &ec_;
        }
    }

    void get() {
        // Unless yield_handler_base::operator() has already been called,
        // suspend the calling fiber until that call.
        ycomp_->wait();
        // The only way our own ec_ member could have a non-default value is
        // if our yield_handler did not have a bound error_code AND the
        // completion callback passed a non-default error_code.
        if (ec_) {
            throw_exception(boost::system::system_error{ec_});
        }
    }

private:
    // If yield_t does not bind an error_code instance, store into here.
    boost::system::error_code ec_{};
    yield_completion::ptr_t   ycomp_;
};
//]

} // namespace detail

} // namespace boost::fibers::asio

namespace boost::asio {

//[fibers_asio_async_result_T
// asio constructs an async_result<> instance from the yield_handler specified
// by handler_type<>::type. A particular asio async method constructs the
// yield_handler, constructs this async_result specialization from it, then
// returns the result of calling its get() method.
template <typename ReturnType, typename T>
class async_result<boost::fibers::asio::yield_t, ReturnType(boost::system::error_code, T)>
    : public boost::fibers::asio::detail::async_result_base {
public:
    // type returned by get()
    using return_type             = T;
    using completion_handler_type = fibers::asio::detail::yield_handler<T>;

    explicit async_result(boost::fibers::asio::detail::yield_handler<T>& h)
        : boost::fibers::asio::detail::async_result_base{h} {
        // Inject ptr to our value_ member into yield_handler<>: result will
        // be stored here.
        h.value_ = &value_;
    }

    // asio async method returns result of calling get()
    return_type get() {
        boost::fibers::asio::detail::async_result_base::get();
        return std::move(value_);
    }

private:
    return_type value_{};
};
//]

//[fibers_asio_async_result_void
// Without the need to handle a passed value, our yield_handler<void>
// specialization is just like async_result_base.
template <>
class async_result<boost::fibers::asio::yield_t, void(boost::system::error_code)>
    : public boost::fibers::asio::detail::async_result_base {
public:
    using return_type             = void;
    using completion_handler_type = fibers::asio::detail::yield_handler<void>;

    explicit async_result(boost::fibers::asio::detail::yield_handler<void>& h)
        : boost::fibers::asio::detail::async_result_base{h} {}
};
//]

} // namespace boost::asio
