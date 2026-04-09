#include "db/storage/art/node.h"

#include "common/logger.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iostream>

namespace idlekv {

namespace {

// debug func
[[maybe_unused]] auto PrintBitmap(uint64_t bitmap) {
    for (int i = 47; i >= 0; i--) {
        std::cout << ((bitmap >> i) & 1);
    }
    std::cout << '\n';
}

} // namespace

auto Node::CheckPerfix(const byte* data) -> size_t {
    return std::mismatch(prefix_.data_, prefix_.data_ + prefix_.len_, data).second - data;
}

auto Node4::FindNext(byte key) -> Node** {
    for (int i = 0; i < size_; i++) {
        if (key == keys_[i]) {
            return &next_[i];
        }
    }
    return nullptr;
}

auto Node4::SetNext(byte key, Node* next) -> void {
    int c_i;
    for (c_i = 0; c_i < size_ && key >= keys_[c_i]; ++c_i) {
    }

    std::memmove(keys_ + c_i + 1, keys_ + c_i, size_ - c_i);
    std::memmove(next_ + c_i + 1, next_ + c_i, (size_ - c_i) * sizeof(void*));

    keys_[c_i] = key;
    next_[c_i] = next;
    ++size_;
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
        _mm_movemask_epi8(_mm_cmpeq_epi8(_mm_set1_epi8(key), _mm_loadu_si128((__m128i*)keys_))) &
        ((1 << size_) - 1);
    return (bool)bitfield ? &next_[__builtin_ctz(bitfield)] : nullptr;
#else
    for (int i = 0; i < size_; i++) {
        if (key == keys_[i]) {
            return &next_[i];
        }
    }
    return nullptr;
#endif
}

auto Node16::SetNext(byte key, Node* next) -> void {
    int child_i;
    for (int i = size_ - 1;; --i) {
        if (i >= 0 && key < keys_[i]) {
            /* move existing sibling to the right */
            keys_[i + 1] = keys_[i];
            next_[i + 1] = next_[i];
        } else {
            child_i = i + 1;
            break;
        }
    }

    keys_[child_i] = key;
    next_[child_i] = next;
    ++size_;
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
    int idx = std::countr_zero(bitmap_);
    CHECK_EQ(next_[idx], nullptr);
    keys_[key] = idx + 1;
    next_[idx] = next;
    bitmap_ ^= 1ULL << idx;
    size_++;
}

auto Node48::DelNext(byte key) -> Node* {
    const auto slot = keys_[key];
    if (slot == Nothing) {
        return nullptr;
    }

    keys_[key]            = Nothing;
    Node* child_to_delete = next_[slot - 1];
    next_[slot - 1]       = nullptr;
    bitmap_ ^= 1ULL << (slot - 1);
    --size_;
    return child_to_delete;
}

auto Node256::FindNext(byte key) -> Node** { return next_[key] == nullptr ? nullptr : &next_[key]; }

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
