#pragma once

#include <cstddef>
#include <list>
#include <vector>
namespace idlekv {
namespace utils {

template <class T>
class Pool {
public:
    auto get() -> T {
        if (free_list_.empty()) {
        }
    }

private:
    std::vector<T> free_list_;

    size_t pool_size_;
};

} // namespace utils
} // namespace idlekv
