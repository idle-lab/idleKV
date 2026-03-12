#pragma once

#include "common/asio_no_exceptions.h"

#include <asio/awaitable.hpp>
#include <asio/dispatch.hpp>
#include <asio/io_context.hpp>
#include <asio/steady_timer.hpp>
#include <asio/use_awaitable.hpp>
namespace idlekv {

namespace utils {

class ConditionVariable {
private:
    struct waiter {
        waiter*                 prev = nullptr;
        waiter*                 next = nullptr;
        std::coroutine_handle<> handle;
        bool                    queued = false;
    };

public:
    explicit ConditionVariable(asio::any_io_executor ex) : executor_(ex) {}

    ConditionVariable(const ConditionVariable&)            = delete;
    ConditionVariable& operator=(const ConditionVariable&) = delete;

    class awaiter {
    public:
        awaiter(ConditionVariable& cv) : cv_(cv) {}

        bool await_ready() noexcept { return false; }

        bool await_suspend(std::coroutine_handle<> h) noexcept {
            node_.handle = h;
            cv_.enqueue(&node_);
            return true;
        }

        void await_resume() noexcept {}

        ~awaiter() {
            if (node_.queued) {
                cv_.remove(&node_);
            }
        }

    private:
        ConditionVariable& cv_;
        waiter             node_;
    };

    awaiter async_wait() noexcept { return awaiter(*this); }

    void notify_one() {
        waiter* w = dequeue();
        if (!w)
            return;

        auto h = w->handle;

        asio::dispatch(executor_, [h]() mutable { h.resume(); });
    }

    void notify_all() {
        waiter* w = head_;
        head_ = tail_ = nullptr;

        while (w) {
            waiter* next = w->next;
            w->queued    = false;

            auto h = w->handle;

            asio::dispatch(executor_, [h]() mutable { h.resume(); });

            w = next;
        }
    }

private:
    void enqueue(waiter* w) noexcept {
        w->queued = true;
        w->next   = nullptr;
        w->prev   = tail_;

        if (tail_)
            tail_->next = w;
        else
            head_ = w;

        tail_ = w;
    }

    waiter* dequeue() noexcept {
        waiter* w = head_;
        if (!w)
            return nullptr;

        head_ = w->next;

        if (head_)
            head_->prev = nullptr;
        else
            tail_ = nullptr;

        w->queued = false;
        return w;
    }

    void remove(waiter* w) noexcept {
        if (!w->queued)
            return;

        if (w->prev)
            w->prev->next = w->next;
        else
            head_ = w->next;

        if (w->next)
            w->next->prev = w->prev;
        else
            tail_ = w->prev;

        w->queued = false;
    }

private:
    asio::any_io_executor executor_;
    waiter*               head_ = nullptr;
    waiter*               tail_ = nullptr;
};

} // namespace utils

} // namespace idlekv
