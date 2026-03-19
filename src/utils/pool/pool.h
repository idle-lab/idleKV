#pragma once

#include <cstddef>
#include <functional>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <vector>

namespace idlekv {
namespace utils {

template <class T>
class Pool {
public:
    using NewFunc = std::function<T()>;

    explicit Pool(size_t PoolSize = 0, NewFunc new_func = {})
        : pool_size_(PoolSize), new_func_(std::move(new_func)) {}

    Pool(const Pool&)                    = delete;
    auto operator=(const Pool&) -> Pool& = delete;

    Pool(Pool&&)                    = delete;
    auto operator=(Pool&&) -> Pool& = delete;

    auto Get() -> T {
        if (!free_list_.empty()) {
            T obj = std::move(free_list_.back());
            free_list_.pop_back();
            return obj;
        }

        if (new_func_) {
            return new_func_();
        }

        if constexpr (std::is_default_constructible_v<T>) {
            return T{};
        } else {
            throw std::logic_error(
                "Pool::Get() needs a new function for non-default-constructible types");
        }
    }

    template <class U>
        requires std::is_constructible_v<T, U&&>
    auto Put(U&& obj) -> void {
        if (pool_size_ != 0 && free_list_.size() >= pool_size_) {
            // Same spirit as Go sync.Pool: cached objects are best-effort and can be dropped.
            return;
        }
        free_list_.emplace_back(std::forward<U>(obj));
    }

    auto Clear() -> void { free_list_.clear(); }

    auto Size() const -> size_t { return free_list_.size(); }

    auto SetNew(NewFunc new_func) -> void { new_func_ = std::move(new_func); }

    auto SetPoolSize(size_t PoolSize) -> void {
        pool_size_ = PoolSize;
        if (pool_size_ == 0) {
            return;
        }
        if (free_list_.size() > pool_size_) {
            free_list_.erase(free_list_.begin(),
                             free_list_.end() - static_cast<std::ptrdiff_t>(pool_size_));
        }
    }

private:
    std::vector<T> free_list_;

    size_t  pool_size_;
    NewFunc new_func_;
};

} // namespace utils
} // namespace idlekv
