#pragma once

#include <cstddef>
#include <memory_resource>
#include <mimalloc.h>

namespace idlekv {

// wrapper of mimalloc to count memory usage.
class XAllocator : public std::pmr::memory_resource {
public:
    explicit XAllocator(mi_heap_t* heap) : heap_(heap) {}

    auto Heap() -> mi_heap_t* { return heap_; }

    auto Used() const -> size_t { return used_; }

private:
    void* do_allocate(std::size_t size, std::size_t align) final;

    void do_deallocate(void* ptr, std::size_t size, std::size_t align) final;

    bool do_is_equal(const std::pmr::memory_resource& o) const noexcept { return this == &o; }

    mi_heap_t* heap_;
    size_t     used_ = 0;
};

} // namespace idlekv
