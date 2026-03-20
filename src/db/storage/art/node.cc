#include "db/storage/art/node.h"
#include "common/logger.h"
#include <cstddef>

#if defined(__i386__) || defined(__amd64__)
#include <emmintrin.h>
#endif


namespace idlekv {

auto Node::CheckPerfix(const byte* data, size_t size) -> size_t {
    if (prefix_) {
        size_t p = 0;
        size_t mi = std::min<size_t>(size, prefix_len_);
        while (p < mi && data[p] == prefix_[p]) p++;
        return p;
    }
    return 0;
}

auto Node4::FindNext(byte key) -> Node** {
    for (int i = 0;i < size_;i++) {
        if (key == keys_[i]) {
            return &next_[i];
        }
    }
    return nullptr;
}

auto Node4::SetNext(byte key, Node* next) -> void {
    keys_[size_] = key;
    next_[size_] = next;
    size_++;
}

auto Node4::DelNext(byte key) -> Node* {
  Node* child_to_delete = nullptr;
  for (int i = 0; i < size_; ++i) {
    if (child_to_delete == nullptr && key == keys_[i]) {
      child_to_delete = next_[i];
    }
    if (child_to_delete != nullptr) {
      /* move existing sibling to the left */
      keys_[i] = i < size_ - 1 ? keys_[i + 1] : 0;
      next_[i] = i < size_ - 1 ? next_[i + 1] : nullptr;
    }
  }
  if (child_to_delete != nullptr) {
    --size_;
  }
  return child_to_delete;
}

auto Node16::FindNext(byte key) -> Node** {
#if defined(__i386__) || defined(__amd64__)
  int bitfield =
      _mm_movemask_epi8(_mm_cmpeq_epi8(_mm_set1_epi8(key),
                                       _mm_loadu_si128((__m128i *)keys_))) &
      ((1 << size_) - 1);
    return (bool)bitfield ? &next_[__builtin_ctz(bitfield)] : nullptr;
#else
    for (int i = 0;i < size_;i++) {
        if (key == keys_[i]) {
            return &next_[i];
        }
    }
    return nullptr;
#endif
}

auto Node16::SetNext(byte key, Node* next) -> void {
    keys_[size_] = key;
    next_[size_] = next;
    size_++;
}

auto Node16::DelNext(byte key) -> Node* {
    Node* child_to_delete = nullptr;
    for (int i = 0; i < size_; ++i) {
        if (child_to_delete == nullptr && key == keys_[i]) {
            child_to_delete = next_[i];
        }
        if (child_to_delete != nullptr) {
            keys_[i] = i < size_ - 1 ? keys_[i + 1] : 0;
            next_[i] = i < size_ - 1 ? next_[i + 1] : nullptr;
        }
    }

    if (child_to_delete != nullptr) {
        --size_;
    }
    return child_to_delete;
}

auto Node48::FindNext(byte key) -> Node** {
    const auto slot = keys_[key];
    return slot == Nothing ? nullptr : &next_[slot - 1];
}

auto Node48::SetNext(byte key, Node* next) -> void {
    for (int i = 0; i < 48; ++i) {
        if (next_[i] == nullptr) {
            keys_[key] = static_cast<byte>(i + 1);
            next_[i] = next;
            ++size_;
            return;
        }
    }
}

auto Node48::DelNext(byte key) -> Node* {
    const auto slot = keys_[key];
    if (slot == Nothing) {
        return nullptr;
    }

    keys_[key] = Nothing;
    Node* child_to_delete = next_[slot - 1];
    next_[slot - 1] = nullptr;
    --size_;
    return child_to_delete;
}

auto Node256::FindNext(byte key) -> Node** {
    return next_[key] == nullptr ? nullptr : &next_[key];
}

auto Node256::SetNext(byte key, Node* next) -> void {
    next_[key] = next;
    size_++;
}

auto Node256::DelNext(byte key) -> Node* {
    Node* child_to_delete = next_[key];
    if (child_to_delete != nullptr) {
        next_[key] = nullptr;
        --size_;
    }
    return child_to_delete;
}




} // namespace idlekv
