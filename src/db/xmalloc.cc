#include "db/xmalloc.h"

namespace idlekv {

void* XAlloctor::do_allocate(size_t size, size_t align) {
  void* res = mi_heap_malloc_aligned(heap_, size, align);

  if (!res)
    throw std::bad_alloc{};

  size_t delta = mi_usable_size(res);

  used_ += delta;

  return res;
}


void XAlloctor::do_deallocate(void* ptr, size_t size, size_t align) {
  size_t usable = mi_usable_size(ptr);

  used_ -= usable;
  mi_free_size_aligned(ptr, size, align);
}
} // namespace idlekv
