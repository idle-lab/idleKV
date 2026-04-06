#pragma once
#include <coroutine>
#include <exception>
#include <utility>

namespace idlekv::utils {

template <typename T>
struct Generator {
    struct promise_type {
        T current_value;

        Generator get_return_object() {
            return Generator{std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_always initial_suspend() { return {}; } // 初始挂起，等待迭代
        std::suspend_always final_suspend() noexcept { return {}; }
        void                unhandled_exception() { std::terminate(); }
        void                return_void() {}

        std::suspend_always yield_value(T value) {
            current_value = std::move(value);
            return {};
        }
    };

    struct iterator {
        std::coroutine_handle<promise_type> handle;

        iterator& operator++() {
            handle.resume();
            return *this;
        }
        T&   operator*() { return handle.promise().current_value; }
        bool operator!=(const iterator&) const { return !handle.done(); }
    };

    iterator begin() {
        if (handle)
            handle.resume();
        return {handle};
    }
    iterator end() { return {nullptr}; }

    explicit Generator(std::coroutine_handle<promise_type> h) : handle(h) {}
    ~Generator() {
        if (handle)
            handle.destroy();
    }

    Generator(const Generator&)            = delete;
    Generator& operator=(const Generator&) = delete;

    Generator(Generator&& other) noexcept : handle(other.handle) { other.handle = nullptr; }
    Generator& operator=(Generator&& other) noexcept {
        if (this != &other) {
            if (handle) {
                handle.destroy();
            }
            handle       = other.handle;
            other.handle = nullptr;
        }
        return *this;
    }

private:
    std::coroutine_handle<promise_type> handle;
};

} // namespace idlekv::utils