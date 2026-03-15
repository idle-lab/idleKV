#pragma once

#include <concepts>
#include <functional>
#include <type_traits>
#include <utility>

namespace idlekv {

namespace utils {

// just like defer keyword of golang
class Defer {
public:
    Defer() = default;

    template <typename F>
        requires(!std::same_as<std::remove_cvref_t<F>, Defer>)
    explicit Defer(F&& fn) : fn_(std::forward<F>(fn)), active_(true) {}

    Defer(const Defer&)                    = delete;
    auto operator=(const Defer&) -> Defer& = delete;

    Defer(Defer&& other) noexcept
        : fn_(std::move(other.fn_)), active_(std::exchange(other.active_, false)) {}

    auto operator=(Defer&& other) noexcept -> Defer& {
        if (this == &other) {
            return *this;
        }

        run_if_active();
        fn_     = std::move(other.fn_);
        active_ = std::exchange(other.active_, false);
        return *this;
    }

    ~Defer() { run_if_active(); }

    auto dismiss() noexcept -> void { active_ = false; }
    auto active() const noexcept -> bool { return active_; }

private:
    auto run_if_active() -> void {
        if (!active_) {
            return;
        }

        active_ = false;
        fn_();
    }

    std::function<void()> fn_;
    bool                  active_{false};
};

} // namespace utils

} // namespace idlekv
