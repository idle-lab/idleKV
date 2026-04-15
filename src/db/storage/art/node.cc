#include "db/storage/art/node.h"

#include "common/logger.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>

namespace idlekv {

namespace {

template <class T>
auto NewNode(std::pmr::memory_resource* mr) -> Node* {
    CHECK(mr != nullptr);
    void* ptr = mr->allocate(sizeof(T), alignof(T));
    return new (ptr) T();
}

template <class T>
auto FreeNode(Node* node, std::pmr::memory_resource* mr) -> void {
    CHECK(node != nullptr);
    CHECK(mr != nullptr);
    auto* typed = static_cast<T*>(node);
    typed->~T();
    mr->deallocate(typed, sizeof(T), alignof(T));
}

// debug func
[[maybe_unused]] auto PrintBitmap(uint64_t bitmap) {
    for (int i = 47; i >= 0; i--) {
        std::cout << ((bitmap >> i) & 1);
    }
    std::cout << '\n';
}

} // namespace

auto Node::PrefixData() -> byte* {
    return reinterpret_cast<byte*>(UsesHeapPrefix() ? prefix_.heap_ : prefix_.inline_);
}

auto Node::PrefixData() const -> const byte* {
    return reinterpret_cast<const byte*>(UsesHeapPrefix() ? prefix_.heap_ : prefix_.inline_);
}

auto Node::SetPrefix(const byte* data, size_t len, std::pmr::memory_resource* mr) -> void {
    // @data can alias the current prefix storage, so stage the replacement first.
    // We then copy the union object as a whole into prefix_, which keeps the final
    // transfer as a single word-sized move on typical 64-bit builds.
    Prefix prefix{};

    if (len != 0) {
        if (len <= kInlinePrefixBytes) {
            std::memmove(prefix.inline_, data, len);
        } else {
            CHECK(mr != nullptr);
            prefix.heap_ = reinterpret_cast<byte*>(mr->allocate(len, alignof(byte)));
            std::memcpy(prefix.heap_, data, len);
        }
    }

    // Release the old prefix only after the replacement bytes are safe in temporary storage.
    SetNewPrefix(prefix, len, mr);
}

auto Node::SetNewPrefix(Prefix prefix, size_t len, std::pmr::memory_resource* mr) -> void {
    ClearPrefix(mr);
    prefix_len_ = static_cast<uint32_t>(len);
    prefix_     = prefix;
}

auto Node::ClearPrefix(std::pmr::memory_resource* mr) -> void {
    if (UsesHeapPrefix()) {
        CHECK(mr != nullptr);
        mr->deallocate(prefix_.heap_, prefix_len_, alignof(byte));
        prefix_.heap_ = nullptr;
    }
    prefix_len_ = 0;
}

auto Node::MovePrefixFrom(Node& other, std::pmr::memory_resource* mr) -> void {
    if (this == &other) {
        return;
    }

    ClearPrefix(mr);
    prefix_len_ = other.prefix_len_;
    if (other.UsesHeapPrefix()) {
        prefix_.heap_       = other.prefix_.heap_;
        other.prefix_.heap_ = nullptr;
        other.prefix_len_   = 0;
        return;
    }

    if (prefix_len_ != 0) {
        std::memcpy(prefix_.inline_, other.prefix_.inline_, prefix_len_);
    }
    other.prefix_len_ = 0;
}

auto Node::CheckPerfix(const byte* data, size_t) -> size_t {
    const byte* const start = PrefixData();
    return std::mismatch(start, start + prefix_len_, data).second - data;
}

Node* Node::New(NodeType type, std::pmr::memory_resource* mr) {
    switch (type) {
    case NodeType::Node4:
        return NewNode<Node4>(mr);
    case NodeType::Node16:
        return NewNode<Node16>(mr);
    case NodeType::Node48:
        return NewNode<Node48>(mr);
    case NodeType::Node256:
        return NewNode<Node256>(mr);
    case NodeType::Leaf:
        CHECK(false) << "Node::New does not support leaf nodes";
        return nullptr;
    default:
        CHECK(false) << "mismatch node type";
        return nullptr;
    }
}

void Node::Free(NodeType type, Node* node, std::pmr::memory_resource* mr) {
    switch (type) {
    case NodeType::Node4:
        return FreeNode<Node4>(node, mr);
    case NodeType::Node16:
        return FreeNode<Node16>(node, mr);
    case NodeType::Node48:
        return FreeNode<Node48>(node, mr);
    case NodeType::Node256:
        return FreeNode<Node256>(node, mr);
    case NodeType::Leaf:
        CHECK(false) << "Node::Free does not support leaf nodes";
        return;
    default:
        CHECK(false) << "mismatch node type";
        return;
    }
}

auto Node4::FindNext(byte key) -> Node** {
    for (int i = 0; i < size_; i++) {
        if (key == keys_[i]) {
            return &next_[i];
        }
    }
    return nullptr;
}

auto Node4::FindChildGte(byte key) -> std::pair<Node**, int> {
    // Range scans and lower_bound need the first child whose edge is >= key, not
    // just exact-match lookup. Node4 keeps edges sorted, so a tiny linear scan is enough.
    for (int i = 0; i < size_; ++i) {
        if (keys_[i] >= key) {
            return {&next_[i], i};
        }
    }
    return {nullptr, -1};
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

auto Node4::Grow(std::pmr::memory_resource* mr) -> InnerNode* {
    auto* bigger = static_cast<Node16*>(Node::New(NodeType::Node16, mr));
    // Node4 and Node16 share the same sorted-dense layout, so growth is just a
    // bulk copy of keys/children plus the compressed prefix header.
    std::memmove(bigger->keys_, keys_, size_);
    std::memmove(bigger->next_, next_, static_cast<size_t>(size_) * sizeof(Node*));
    MoveHeaderTo(*bigger, mr);
    Node::Free(NodeType::Node4, this, mr);
    return bigger;
}

auto Node4::Shrink(std::pmr::memory_resource*) -> InnerNode* {
    CHECK(false) << "node4 has no smaller inner node type";
    return nullptr;
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

auto Node16::FindChildGte(byte key) -> std::pair<Node**, int> {
    // Exact-match lookup uses SIMD on x86, but lower_bound still needs the first
    // edge >= key, so the ordered array is scanned directly here.
    for (int i = 0; i < size_; ++i) {
        if (keys_[i] >= key) {
            return {&next_[i], i};
        }
    }
    return {nullptr, -1};
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

auto Node16::Grow(std::pmr::memory_resource* mr) -> InnerNode* {
    auto* bigger = static_cast<Node48*>(Node::New(NodeType::Node48, mr));
    // Node48 replaces the dense edge array with a 256-entry edge->slot table. The
    // child array stays dense; keys_ stores slot+1 so 0 can mean "missing child".
    std::memmove(bigger->next_, next_, static_cast<size_t>(size_) * sizeof(Node*));
    for (int i = 0; i < size_; ++i) {
        bigger->keys_[keys_[i]] = i + 1;
    }
    // The first @size_ slots are already occupied by moved children.
    bigger->bitmap_ = (~((1ULL << size_) - 1)) & Node48::VALID_MASK;
    MoveHeaderTo(*bigger, mr);
    Node::Free(NodeType::Node16, this, mr);
    return bigger;
}

auto Node16::Shrink(std::pmr::memory_resource* mr) -> InnerNode* {
    auto* smaller = static_cast<Node4*>(Node::New(NodeType::Node4, mr));
    std::memmove(smaller->keys_, keys_, size_);
    std::memmove(smaller->next_, next_, static_cast<size_t>(size_) * sizeof(Node*));
    MoveHeaderTo(*smaller, mr);
    Node::Free(NodeType::Node16, this, mr);
    return smaller;
}

auto Node48::FindNext(byte key) -> Node** {
    const auto slot = keys_[key];
    return slot == Nothing ? nullptr : &next_[slot - 1];
}

auto Node48::FindChildGte(byte key) -> std::pair<Node**, int> {
    // Node48 keeps lexicographic order by scanning the 256-way edge map, while the
    // actual child pointers live in a dense 48-entry array.
    for (int i = key; i <= 0xFF; ++i) {
        const auto slot = keys_[i];
        if (slot != Nothing) {
            return {&next_[slot - 1], i};
        }
    }
    return {nullptr, -1};
}

auto Node48::SetNext(byte key, Node* next) -> void {
    // bitmap_ tracks free slots in next_[]. countr_zero finds the first free dense
    // slot so insertion keeps next_ compact without disturbing edge ordering in keys_.
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

auto Node48::Grow(std::pmr::memory_resource* mr) -> InnerNode* {
    auto* bigger = static_cast<Node256*>(Node::New(NodeType::Node256, mr));
    // Node256 drops the indirection table completely and materializes a direct
    // 256-way child array for the final growth stage.
    for (int i = 0; i < 256; ++i) {
        if (keys_[i] != Nothing) {
            bigger->next_[i] = next_[keys_[i] - 1];
        }
    }
    MoveHeaderTo(*bigger, mr);
    Node::Free(NodeType::Node48, this, mr);
    return bigger;
}

auto Node48::Shrink(std::pmr::memory_resource* mr) -> InnerNode* {
    auto* smaller = static_cast<Node16*>(Node::New(NodeType::Node16, mr));
    // Shrinking rebuilds the ordered dense representation expected by Node16 from
    // the sparse Node48 edge map.
    for (int i = 0; i < 256; ++i) {
        if (keys_[i] != Nothing) {
            smaller->keys_[smaller->size_] = i;
            smaller->next_[smaller->size_] = next_[keys_[i] - 1];
            ++smaller->size_;
        }
    }
    MoveHeaderTo(*smaller, mr);
    Node::Free(NodeType::Node48, this, mr);
    return smaller;
}

auto Node256::FindNext(byte key) -> Node** { return next_[key] == nullptr ? nullptr : &next_[key]; }

auto Node256::FindChildGte(byte key) -> std::pair<Node**, int> {
    // Node256 has O(1) exact lookup, but lower_bound still needs to scan forward
    // for the first populated slot >= key.
    for (int i = key; i <= 0xFF; ++i) {
        if (next_[i] != nullptr) {
            return {&next_[i], i};
        }
    }
    return {nullptr, -1};
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

auto Node256::Grow(std::pmr::memory_resource*) -> InnerNode* {
    CHECK(false) << "node256 has no larger inner node type";
    return nullptr;
}

auto Node256::Shrink(std::pmr::memory_resource* mr) -> InnerNode* {
    auto* smaller = static_cast<Node48*>(Node::New(NodeType::Node48, mr));
    // Rebuild the Node48 indirection table densely so future inserts can keep
    // using the compact 48-slot child array.
    for (int i = 0; i < 256; ++i) {
        if (next_[i] != nullptr) {
            smaller->keys_[i]              = smaller->size_ + 1;
            smaller->next_[smaller->size_] = next_[i];
            ++smaller->size_;
        }
    }
    // The first @size_ slots are already occupied by moved children.
    smaller->bitmap_ = (~((1ULL << smaller->size_) - 1)) & Node48::VALID_MASK;
    MoveHeaderTo(*smaller, mr);
    Node::Free(NodeType::Node256, this, mr);
    return smaller;
}

} // namespace idlekv
