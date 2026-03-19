#pragma once

#include "common/asio_no_exceptions.h"

#include <algorithm>
#include <asio/any_io_executor.hpp>
#include <asio/associated_executor.hpp>
#include <asio/async_result.hpp>
#include <asio/awaitable.hpp>
#include <asio/dispatch.hpp>
#include <asio/use_awaitable.hpp>
#include <concepts>
#include <cstddef>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>

namespace idlekv::utils {

// A coroutine-friendly blocking queue:
// - thread-safe across different event loop threads
// - consumers can await until an item is available
// - producers can optionally await when a bounded queue is full
template <class T>
class BlockingQueue {
public:
    explicit BlockingQueue(size_t capacity = 0) : capacity_(capacity) {}

    BlockingQueue(const BlockingQueue&)                    = delete;
    auto operator=(const BlockingQueue&) -> BlockingQueue& = delete;

    BlockingQueue(BlockingQueue&&)                    = delete;
    auto operator=(BlockingQueue&&) -> BlockingQueue& = delete;

    ~BlockingQueue() { Close(); }

    template <class U>
        requires std::constructible_from<T, U&&>
    auto TryPush(U&& value) -> bool {
        std::shared_ptr<PopWaiterBase> pop_waiter;
        std::optional<T>               ready_value;

        {
            std::lock_guard<std::mutex> lk(mu_);
            if (closed_) {
                return false;
            }

            if (!pop_waiters_.empty()) {
                pop_waiter = pop_waiters_.front();
                pop_waiters_.pop_front();
                pop_waiter->queued = false;
                ready_value.emplace(std::forward<U>(value));
            } else {
                if (IsFullUnlocked()) {
                    return false;
                }

                items_.emplace_back(std::forward<U>(value));
                return true;
            }
        }

        CompletePop(pop_waiter, std::move(ready_value));
        return true;
    }

    template <class... Args>
        requires std::constructible_from<T, Args&&...>
    auto TryEmplace(Args&&... args) -> bool {
        return TryPush(T(std::forward<Args>(args)...));
    }

    template <class U, class CompletionToken = asio::use_awaitable_t<>>
        requires std::constructible_from<T, U&&>
    auto asyncPush(U&& value, CompletionToken token = CompletionToken{}) {
        return asio::async_initiate<CompletionToken, void(bool)>(
            [this, item = T(std::forward<U>(value))](auto handler) mutable {
                using Handler = decltype(handler);

                auto waiter = std::shared_ptr<PushWaiterBase>(std::make_shared<PushWaiter<Handler>>(
                    asio::get_associated_executor(handler, asio::system_executor()),
                    std::move(handler), std::move(item)));

                std::shared_ptr<PopWaiterBase> pop_waiter;
                std::optional<T>               ready_value;
                bool                           complete_now = false;
                bool                           accepted     = false;

                {
                    std::lock_guard<std::mutex> lk(mu_);

                    if (closed_) {
                        complete_now = true;
                    } else if (!pop_waiters_.empty()) {
                        pop_waiter = pop_waiters_.front();
                        pop_waiters_.pop_front();
                        pop_waiter->queued = false;
                        ready_value        = std::move(waiter->value);
                        waiter->value.reset();
                        complete_now = true;
                        accepted     = true;
                    } else if (!IsFullUnlocked()) {
                        items_.emplace_back(std::move(*waiter->value));
                        waiter->value.reset();
                        complete_now = true;
                        accepted     = true;
                    } else {
                        waiter->queued = true;
                        push_waiters_.push_back(waiter);
                    }
                }

                if (complete_now) {
                    CompletePush(waiter, accepted);
                    if (pop_waiter) {
                        CompletePop(pop_waiter, std::move(ready_value));
                    }
                }
            },
            token);
    }

    auto TryPop() -> std::optional<T> {
        std::shared_ptr<PushWaiterBase> push_waiter;
        std::optional<T>                value;

        {
            std::lock_guard<std::mutex> lk(mu_);
            if (items_.empty()) {
                return std::nullopt;
            }

            value.emplace(std::move(items_.front()));
            items_.pop_front();
            push_waiter = AdmitOneWaitingPushUnlocked();
        }

        if (push_waiter) {
            CompletePush(push_waiter, true);
        }
        return value;
    }

    template <class CompletionToken = asio::use_awaitable_t<>>
    auto asyncPop(CompletionToken token = CompletionToken{}) {
        return asio::async_initiate<CompletionToken, void(std::optional<T>)>(
            [this](auto handler) mutable {
                using Handler = decltype(handler);

                auto waiter = std::shared_ptr<PopWaiterBase>(std::make_shared<PopWaiter<Handler>>(
                    asio::get_associated_executor(handler, asio::system_executor()),
                    std::move(handler)));

                std::shared_ptr<PushWaiterBase> push_waiter;
                std::optional<T>                value;
                bool                            complete_now = false;

                {
                    std::lock_guard<std::mutex> lk(mu_);

                    if (!items_.empty()) {
                        value.emplace(std::move(items_.front()));
                        items_.pop_front();
                        push_waiter  = AdmitOneWaitingPushUnlocked();
                        complete_now = true;
                    } else if (closed_) {
                        complete_now = true;
                    } else {
                        waiter->queued = true;
                        pop_waiters_.push_back(waiter);
                    }
                }

                if (complete_now) {
                    CompletePop(waiter, std::move(value));
                    if (push_waiter) {
                        CompletePush(push_waiter, true);
                    }
                }
            },
            token);
    }

    auto Close() noexcept -> void {
        std::deque<std::shared_ptr<PopWaiterBase>>  pop_waiters;
        std::deque<std::shared_ptr<PushWaiterBase>> push_waiters;

        {
            std::lock_guard<std::mutex> lk(mu_);
            if (closed_) {
                return;
            }

            closed_ = true;

            for (auto& waiter : pop_waiters_) {
                waiter->queued = false;
            }
            for (auto& waiter : push_waiters_) {
                waiter->queued = false;
            }

            pop_waiters.swap(pop_waiters_);
            push_waiters.swap(push_waiters_);
        }

        for (const auto& waiter : pop_waiters) {
            CompletePop(waiter, std::nullopt);
        }
        for (const auto& waiter : push_waiters) {
            CompletePush(waiter, false);
        }
    }

    auto Closed() const -> bool {
        std::lock_guard<std::mutex> lk(mu_);
        return closed_;
    }

    auto Empty() const -> bool {
        std::lock_guard<std::mutex> lk(mu_);
        return items_.empty();
    }

    auto Size() const -> size_t {
        std::lock_guard<std::mutex> lk(mu_);
        return items_.size();
    }

    auto Capacity() const -> size_t { return capacity_; }

private:
    struct PopWaiterBase {
        explicit PopWaiterBase(asio::any_io_executor executor) : executor(std::move(executor)) {}
        virtual ~PopWaiterBase() = default;

        virtual void Complete(std::optional<T> value) = 0;

        asio::any_io_executor executor;
        bool                  queued = false;
    };

    template <class Handler>
    struct PopWaiter final : PopWaiterBase {
        PopWaiter(asio::any_io_executor executor, Handler&& handler)
            : PopWaiterBase(std::move(executor)), handler(std::move(handler)) {}

        void Complete(std::optional<T> value) override {
            auto h = std::move(handler);
            std::move(h)(std::move(value));
        }

        Handler handler;
    };

    struct PushWaiterBase {
        PushWaiterBase(asio::any_io_executor executor, T value)
            : executor(std::move(executor)), value(std::move(value)) {}
        virtual ~PushWaiterBase() = default;

        virtual void Complete(bool accepted) = 0;

        asio::any_io_executor executor;
        std::optional<T>      value;
        bool                  queued = false;
    };

    template <class Handler>
    struct PushWaiter final : PushWaiterBase {
        PushWaiter(asio::any_io_executor executor, Handler&& handler, T value)
            : PushWaiterBase(std::move(executor), std::move(value)), handler(std::move(handler)) {}

        void Complete(bool accepted) override {
            auto h = std::move(handler);
            std::move(h)(accepted);
        }

        Handler handler;
    };

    auto IsFullUnlocked() const noexcept -> bool {
        return capacity_ != 0 && items_.size() >= capacity_;
    }

    auto AdmitOneWaitingPushUnlocked() -> std::shared_ptr<PushWaiterBase> {
        if (closed_ || push_waiters_.empty() || IsFullUnlocked()) {
            return nullptr;
        }

        auto waiter = push_waiters_.front();
        push_waiters_.pop_front();
        waiter->queued = false;

        items_.emplace_back(std::move(*waiter->value));
        waiter->value.reset();
        return waiter;
    }

    static auto CompletePop(const std::shared_ptr<PopWaiterBase>& waiter,
                            std::optional<T>                      value) noexcept -> void {
        if (!waiter) {
            return;
        }

        asio::dispatch(waiter->executor, [waiter, value = std::move(value)]() mutable {
            waiter->Complete(std::move(value));
        });
    }

    static auto CompletePush(const std::shared_ptr<PushWaiterBase>& waiter, bool accepted) noexcept
        -> void {
        if (!waiter) {
            return;
        }

        asio::dispatch(waiter->executor,
                       [waiter, accepted]() mutable { waiter->Complete(accepted); });
    }

private:
    const size_t capacity_;

    mutable std::mutex mu_;
    std::deque<T>      items_;

    bool closed_ = false;

    std::deque<std::shared_ptr<PopWaiterBase>>  pop_waiters_;
    std::deque<std::shared_ptr<PushWaiterBase>> push_waiters_;
};

} // namespace idlekv::utils
