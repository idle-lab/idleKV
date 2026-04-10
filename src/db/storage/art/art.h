#pragma once

#include "common/config.h"
#include "common/logger.h"
#include "db/storage/art/alloctor.h"
#include "db/storage/art/art_key.h"
#include "db/storage/art/node.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <memory_resource>
#include <span>
#include <type_traits>
#include <utility>
#include <vector>

namespace idlekv {

enum struct InsertMode : uint8_t {
    InsertOnly,
    Upsert,
    IfExistGetValue,
    OOM, // TODO(cyb): support max memory usage.
};

template <class ValueType, size_t MaxPrefixLen = 16>
class Art {
public:
    using DataNode = NodeLeaf<ValueType>;

    using Node4Alloctor    = NodeAlloctorT<Node4>;
    using Node16Alloctor   = NodeAlloctorT<Node16>;
    using Node48Alloctor   = NodeAlloctorT<Node48>;
    using Node256Alloctor  = NodeAlloctorT<Node256>;
    using DataNodeAlloctor = NodeAlloctorT<DataNode>;

    using NodeTypes = TypeList<Node4, Node16, Node48, Node256, DataNode>;

    template <typename T>
    using AllocatorOf = std::conditional_t<
        std::is_same_v<T, Node4>, Node4Alloctor,
        std::conditional_t<
            std::is_same_v<T, Node16>, Node16Alloctor,
            std::conditional_t<std::is_same_v<T, Node48>, Node48Alloctor,
                               std::conditional_t<std::is_same_v<T, Node256>, Node256Alloctor,
                                                  std::conditional_t<std::is_same_v<T, DataNode>,
                                                                     DataNodeAlloctor, void>>>>>;

    explicit Art(std::pmr::memory_resource* mr)
        : mr_(mr) {
        alloctor_[TypeIndex<Node4, NodeTypes>::value] =
            std::make_unique<Node4Alloctor>(mr);
        alloctor_[TypeIndex<Node16, NodeTypes>::value] =
            std::make_unique<Node16Alloctor>(mr);
        alloctor_[TypeIndex<Node48, NodeTypes>::value] =
            std::make_unique<Node48Alloctor>(mr);
        alloctor_[TypeIndex<Node256, NodeTypes>::value] =
            std::make_unique<Node256Alloctor>(mr);
        alloctor_[TypeIndex<DataNode, NodeTypes>::value] =
            std::make_unique<DataNodeAlloctor>(mr);
    }

    struct InsertResutl {
        enum Status {
            OK,
            UpsertValue,
            DuplicateKey,
        };
        InsertResutl(Status status) : s(status) {}
        InsertResutl(ValueType* data) : value(data), s(Status::OK) {}

        auto operator==(Status other) const -> bool { return s == other; }

        ValueType* value{nullptr};
        Status     s;
    };

    struct RangeEntry {
        std::vector<byte> key;
        ValueType*        value{nullptr};
    };

    template <class V>
    auto Insert(ArtKey key, V&& value, InsertMode mode = InsertMode::InsertOnly) noexcept
        -> InsertResutl {
        if (UNLIKELY(!root_)) [[unlikely]] {
            root_ = CreateLeafChain(key.Data(), key.Len(), std::forward<V>(value));
            size_++;
            return InsertResutl::OK;
        }
        PREFETCH_W(root_, 3);
        auto res = InsertInternal(root_, &root_, key, std::forward<V>(value), mode);
        if (res == InsertResutl::OK) {
            size_++;
        }
        return res;
    }

    // return a copy of value, so you should consider the cost of value copying.
    auto Lookup(ArtKey key) noexcept -> ValueType* {
        if (UNLIKELY(!root_)) [[unlikely]] {
            return nullptr;
        }
        PREFETCH_R(root_, 3);
        return LookupInternal(root_, key);
    }

    template <class Fn>
    auto Iterate(Fn&& fn) -> void {
        if (root_ == nullptr) {
            return;
        }

        std::vector<byte> key;
        key.reserve(64);
        TraverseInOrder(root_, key, fn);
    }

    auto Range(ArtKey min, ArtKey max) -> std::vector<RangeEntry> {
        std::vector<RangeEntry> matches;
        if (root_ == nullptr) {
            return matches;
        }

        if (CompareKeyBytes(KeyView(min), KeyView(max)) > 0) {
            return matches;
        }

        Iterate([&](std::span<const byte> key, ValueType& value) -> bool {
            const int min_cmp = CompareKeyBytes(key, KeyView(min));
            if (min_cmp < 0) {
                return true;
            }

            if (CompareKeyBytes(key, KeyView(max)) > 0) {
                return false;
            }

            matches.push_back(RangeEntry{std::vector<byte>(key.begin(), key.end()), &value});
            return true;
        });

        return matches;
    }

    auto Min() noexcept -> ValueType* { return EdgeValue(/*find_min=*/true); }

    auto Max() noexcept -> ValueType* { return EdgeValue(/*find_min=*/false); }

    auto Size() const noexcept -> size_t { return size_; }

    auto Empty() const noexcept -> bool { return size_ == 0; }

private:
    static auto KeyView(const ArtKey& key) -> std::span<const byte> {
        return {key.Data(), key.Len()};
    }

    static auto CompareKeyBytes(std::span<const byte> lhs, std::span<const byte> rhs) -> int {
        const size_t cmp_len = std::min(lhs.size(), rhs.size());
        if (cmp_len != 0) {
            const int cmp = std::memcmp(lhs.data(), rhs.data(), cmp_len);
            if (cmp != 0) {
                return cmp < 0 ? -1 : 1;
            }
        }

        if (lhs.size() == rhs.size()) {
            return 0;
        }
        return lhs.size() < rhs.size() ? -1 : 1;
    }

    template <class V>
    auto CreateLeafChain(const byte* data, size_t len, V&& value) -> Node* {
        if (len <= kMaxPrefixBytes) {
            DataNode* leaf = NewNodeWithPerfix<DataNode>(data, len);
            leaf->value_   = std::forward<V>(value);
            return leaf;
        }

        Node4* node = NewNodeWithPerfix<Node4>(data, kMaxPrefixBytes);
        node->SetNext(data[kMaxPrefixBytes],
                      CreateLeafChain(data + kMaxPrefixBytes + 1, len - kMaxPrefixBytes - 1,
                                      std::forward<V>(value)));
        return node;
    }

    auto PrependPrefixChain(const byte* data, size_t len, Node* node) -> Node* {
        if (len == 0) {
            return node;
        }

        if (len + node->prefix_.len_ <= kMaxPrefixBytes) {
            if (node->prefix_.len_ > 0) {
                std::memmove(node->prefix_.data_ + len, node->prefix_.data_, node->prefix_.len_);
            }
            std::memcpy(node->prefix_.data_, data, len);
            node->prefix_.len_ += len;
            return node;
        }

        CHECK_GE(len, size_t{1});
        const size_t chunk_len = std::min<size_t>(kMaxPrefixBytes, len - 1);
        Node4*       root      = NewNodeWithPerfix<Node4>(data, chunk_len);
        root->SetNext(data[chunk_len],
                      PrependPrefixChain(data + chunk_len + 1, len - chunk_len - 1, node));
        return root;
    }

    template <class Fn>
    auto TraverseInOrder(Node* node, std::vector<byte>& key, Fn& fn) -> bool {
        const size_t original_size = key.size();
        key.insert(key.end(), node->prefix_.data_, node->prefix_.data_ + node->prefix_.len_);

        if (node->type_ == NodeType::Leaf) {
            bool keep_going = fn(std::span<const byte>(key.data(), key.size()),
                                 static_cast<DataNode*>(node)->value_);
            key.resize(original_size);
            return keep_going;
        }

        auto visit_child = [&](byte partial_key, Node* child) -> bool {
            key.push_back(partial_key);
            const bool keep_going = TraverseInOrder(child, key, fn);
            key.pop_back();
            return keep_going;
        };

        switch (node->type_) {
        case NodeType::Node4: {
            auto* n4 = static_cast<Node4*>(node);
            for (int i = 0; i < n4->size_; ++i) {
                if (!visit_child(n4->keys_[i], n4->next_[i])) {
                    key.resize(original_size);
                    return false;
                }
            }
            break;
        }
        case NodeType::Node16: {
            auto* n16 = static_cast<Node16*>(node);
            for (int i = 0; i < n16->size_; ++i) {
                if (!visit_child(n16->keys_[i], n16->next_[i])) {
                    key.resize(original_size);
                    return false;
                }
            }
            break;
        }
        case NodeType::Node48: {
            auto* n48 = static_cast<Node48*>(node);
            for (int i = 0; i <= 0xFF; ++i) {
                const auto slot = n48->keys_[i];
                if (slot != Node48::Nothing && !visit_child(static_cast<byte>(i), n48->next_[slot - 1])) {
                    key.resize(original_size);
                    return false;
                }
            }
            break;
        }
        case NodeType::Node256: {
            auto* n256 = static_cast<Node256*>(node);
            for (int i = 0; i <= 0xFF; ++i) {
                if (n256->next_[i] != nullptr &&
                    !visit_child(static_cast<byte>(i), n256->next_[i])) {
                    key.resize(original_size);
                    return false;
                }
            }
            break;
        }
        case NodeType::Leaf:
            UNREACHABLE();
        default:
            CHECK(false) << "mismatch node type";
            UNREACHABLE();
        }

        key.resize(original_size);
        return true;
    }

    auto EdgeValue(bool find_min) noexcept -> ValueType* {
        Node* cur = root_;
        while (cur != nullptr && cur->type_ != NodeType::Leaf) {
            cur = find_min ? FirstChild(cur) : LastChild(cur);
        }

        return cur == nullptr ? nullptr : &static_cast<DataNode*>(cur)->value_;
    }

    auto FirstChild(Node* node) noexcept -> Node* {
        switch (node->type_) {
        case NodeType::Node4:
            return static_cast<Node4*>(node)->size_ == 0 ? nullptr : static_cast<Node4*>(node)->next_[0];
        case NodeType::Node16:
            return static_cast<Node16*>(node)->size_ == 0 ? nullptr : static_cast<Node16*>(node)->next_[0];
        case NodeType::Node48: {
            auto* n48 = static_cast<Node48*>(node);
            for (int i = 0; i <= 0xFF; ++i) {
                if (n48->keys_[i] != Node48::Nothing) {
                    return n48->next_[n48->keys_[i] - 1];
                }
            }
            return nullptr;
        }
        case NodeType::Node256: {
            auto* n256 = static_cast<Node256*>(node);
            for (auto* child : n256->next_) {
                if (child != nullptr) {
                    return child;
                }
            }
            return nullptr;
        }
        case NodeType::Leaf:
            return node;
        default:
            CHECK(false) << "mismatch node type";
            UNREACHABLE();
        }
    }

    auto LastChild(Node* node) noexcept -> Node* {
        switch (node->type_) {
        case NodeType::Node4: {
            auto* n4 = static_cast<Node4*>(node);
            return n4->size_ == 0 ? nullptr : n4->next_[n4->size_ - 1];
        }
        case NodeType::Node16: {
            auto* n16 = static_cast<Node16*>(node);
            return n16->size_ == 0 ? nullptr : n16->next_[n16->size_ - 1];
        }
        case NodeType::Node48: {
            auto* n48 = static_cast<Node48*>(node);
            for (int i = 0xFF; i >= 0; --i) {
                if (n48->keys_[i] != Node48::Nothing) {
                    return n48->next_[n48->keys_[i] - 1];
                }
            }
            return nullptr;
        }
        case NodeType::Node256: {
            auto* n256 = static_cast<Node256*>(node);
            for (int i = 0xFF; i >= 0; --i) {
                if (n256->next_[i] != nullptr) {
                    return n256->next_[i];
                }
            }
            return nullptr;
        }
        case NodeType::Leaf:
            return node;
        default:
            CHECK(false) << "mismatch node type";
            UNREACHABLE();
        }
    }

public:
    auto Erase(ArtKey key) noexcept -> size_t {
        if (UNLIKELY(!root_)) [[unlikely]] {
            return 0;
        }

        PREFETCH_R(root_, 3);
        size_t erased = EraseInternal(root_, &root_, key);
        if (erased != 0) {
            size_ -= erased;
        }
        return erased;
    }

private:
    template <class V>
    auto InsertInternal(Node* cur, Node** cur_ref, ArtKey& key, V&& value, InsertMode mode) noexcept
        -> InsertResutl {
        size_t len;
        while (true) {
            len = cur->CheckPerfix(key.Data());

            if (len != cur->prefix_.len_) {
                // prefix mismatch
                Node4* node = NewNodeWithPerfix<Node4>(cur->prefix_.data_, len);
                node->SetNext(cur->prefix_.data_[len], cur);
                node->SetNext(key.Data()[len],
                              CreateLeafChain(key.Data() + len + 1, key.Len() - len - 1,
                                              std::forward<V>(value)));

                if (cur->prefix_.len_ > len + 1) {
                    std::memmove(cur->prefix_.data_, cur->prefix_.data_ + len + 1,
                                 cur->prefix_.len_ - len - 1);
                    cur->prefix_.len_ = cur->prefix_.len_ - len - 1;
                } else {
                    cur->prefix_.len_ = 0;
                }

                *cur_ref = node;
                return InsertResutl::OK;
            }

            if (key.Len() == cur->prefix_.len_) {
                // exact match
                // cur must be a leaf
                CHECK_EQ(cur->type_, NodeType::Leaf);
                switch (mode) {
                case InsertMode::InsertOnly:
                    return InsertResutl::DuplicateKey;
                case InsertMode::IfExistGetValue:
                    PREFETCH_W(&static_cast<DataNode*>(cur)->value_, 1);
                    return &static_cast<DataNode*>(cur)->value_;
                case InsertMode::Upsert: {
                    DataNode* node = static_cast<DataNode*>(cur);
                    node->value_   = std::move(value);

                    return InsertResutl::UpsertValue;
                }
                default:
                    UNREACHABLE();
                }
            }

            key.Cut(len);
            Node** next = FindNext(cur, *key.Data());
            if (next == nullptr) {
                Node* maybe_new =
                    SetNext(cur, *key.Data(),
                            CreateLeafChain(key.Data() + 1, key.Len() - 1, std::forward<V>(value)));
                if (maybe_new != cur) {
                    *cur_ref = maybe_new;
                }

                return InsertResutl::OK;
            }

            PREFETCH_W(*next, 2);
            key.Cut(1);
            cur_ref = next;
            cur     = *next;
        }
        UNREACHABLE();
    }

    auto LookupInternal(Node* cur, ArtKey& key) noexcept -> ValueType* {
        while (true) {
            if (cur->CheckPerfix(key.Data()) != cur->prefix_.len_) {
                // prefix mismatch => key doesn't exist
                return nullptr;
            }

            if (key.Len() == cur->prefix_.len_) {
                // exact match
                CHECK_EQ(cur->type_, NodeType::Leaf);
                PREFETCH_R(&static_cast<DataNode*>(cur)->value_, 1);
                return &static_cast<DataNode*>(cur)->value_;
            }

            Node** next = FindNext(cur, key.Data()[cur->prefix_.len_]);
            if (next == nullptr) {
                return nullptr;
            }
            PREFETCH_R(*next, 2);
            key.Cut(cur->prefix_.len_ + 1);
            cur = *next;
        }
        UNREACHABLE();
    }

    auto EraseInternal(Node* cur, Node** cur_ref, ArtKey& key) noexcept -> size_t {
        Node**     parent_ref       = nullptr;
        InnerNode* parent           = nullptr;
        byte       last_partial_key = 0;
        while (true) {
            size_t len = cur->CheckPerfix(key.Data());
            if (len != cur->prefix_.len_) {
                // prefix mismatch => key doesn't exist
                return 0;
            }

            if (key.Len() == cur->prefix_.len_) {
                // exact match
                // cur must be a leaf node, if not key doesn't exist
                if (cur->type_ != NodeType::Leaf) {
                    return 0;
                }

                if (UNLIKELY(parent == nullptr)) [[unlikely]] {
                    // match results at the root node
                    // this branch only happen when only one record in tree
                    CHECK_EQ(size_, size_t{1});
                    DestoryArtNode(cur);
                    *cur_ref = nullptr;
                    return 1;
                }

                Node* maybe_new = DelNext(parent, last_partial_key);
                if (maybe_new != parent) {
                    *parent_ref = maybe_new;
                    parent      = static_cast<InnerNode*>(maybe_new);
                }

                if (parent->size_ == 1) {
                    // path compression
                    // parent must be a node4 type
                    CHECK_EQ(parent->type_, NodeType::Node4);
                    Node4* n4   = static_cast<Node4*>(parent);
                    Node*  next = n4->next_[0];
                    std::array<byte, kMaxPrefixBytes + 1> compressed_prefix{};
                    std::memcpy(compressed_prefix.data(), n4->prefix_.data_, n4->prefix_.len_);
                    compressed_prefix[n4->prefix_.len_] = n4->keys_[0];

                    DestoryArtNode(parent);
                    *parent_ref = PrependPrefixChain(compressed_prefix.data(), n4->prefix_.len_ + 1,
                                                     next);
                }

                return 1;
            }

            Node** next = FindNext(cur, key.Data()[len]);
            if (next == nullptr) {
                return 0;
            }
            PREFETCH_W(*next, 2);
            last_partial_key = key.Data()[len];
            key.Cut(len + 1);
            parent_ref = cur_ref;
            parent     = static_cast<InnerNode*>(cur);
            cur_ref    = next;
            cur        = *next;
        }
        UNREACHABLE();
    }

    auto FindNext(Node* node, byte key) -> Node** {
        switch (node->type_) {
        case NodeType::Node4:
            return static_cast<Node4*>(node)->FindNext(key);
        case NodeType::Node16:
            return static_cast<Node16*>(node)->FindNext(key);
        case NodeType::Node48:
            return static_cast<Node48*>(node)->FindNext(key);
        case NodeType::Node256:
            return static_cast<Node256*>(node)->FindNext(key);
        case NodeType::Leaf:
            CHECK(false) << "find next in leaf node";
            UNREACHABLE();
        default:
            CHECK(false) << "mismatch node type";
            UNREACHABLE();
        }
    }

    auto SetNext(Node* node, byte key, Node* next) -> Node* {
        switch (node->type_) {
        case NodeType::Node4:
            return SetNextOrGrow<Node4, Node16>(static_cast<Node4*>(node), key, next);
        case NodeType::Node16:
            return SetNextOrGrow<Node16, Node48>(static_cast<Node16*>(node), key, next);
        case NodeType::Node48:
            return SetNextOrGrow<Node48, Node256>(static_cast<Node48*>(node), key, next);
        case NodeType::Node256: {
            Node256* n256 = static_cast<Node256*>(node);
            n256->SetNext(key, next);
            return node;
        }
        case NodeType::Leaf:
            CHECK(false) << "find next in leaf node";
            UNREACHABLE();
        default:
            CHECK(false) << "mismatch node type";
            UNREACHABLE();
        }
    }

    auto DelNext(Node* node, byte key) -> Node* {
        switch (node->type_) {
        case NodeType::Node4:
            DestoryArtNode(static_cast<Node4*>(node)->DelNext(key));
            return node;
        case NodeType::Node16:
            return DelNextOrShrink<Node16, Node4>(static_cast<Node16*>(node), key);
        case NodeType::Node48:
            return DelNextOrShrink<Node48, Node16>(static_cast<Node48*>(node), key);
        case NodeType::Node256:
            return DelNextOrShrink<Node256, Node48>(static_cast<Node256*>(node), key);
        case NodeType::Leaf:
            CHECK(false) << "delete next in leaf node";
            UNREACHABLE();
        default:
            CHECK(false) << "mismatch node type";
            UNREACHABLE();
        }
    }

    template <class CurNode, class BiggerNode>
    auto SetNextOrGrow(CurNode* node, byte key, Node* next) -> Node* {
        if (!node->IsFull()) {
            node->SetNext(key, next);
            return node;
        }

        BiggerNode* bigger = static_cast<BiggerNode*>(NodeGrow(node));
        bigger->SetNext(key, next);
        return bigger;
    }

    template <class CurNode, class SmallerNode>
    auto DelNextOrShrink(CurNode* node, byte key) -> Node* {
        Node* child_to_delete = node->DelNext(key);
        DestoryArtNode(child_to_delete);
        return node->UnderFull() ? NodeShrink(node) : node;
    }

    auto HeaderMove(InnerNode* dest, InnerNode* src) -> void {
        dest->prefix_ = src->prefix_;
        dest->size_   = src->size_;
    }

    auto NodeGrow(Node* node) -> Node* {
        switch (node->type_) {
        case NodeType::Node4: {
            Node4*  prv    = static_cast<Node4*>(node);
            Node16* bigger = GetAlloctor<Node16>()->New();
            std::memmove(bigger->keys_, prv->keys_, prv->size_);
            std::memmove(bigger->next_, prv->next_,
                         static_cast<size_t>(prv->size_) * sizeof(Node*));
            HeaderMove(bigger, prv);
            GetAlloctor<Node4>()->Free(prv);
            PREFETCH_W(bigger, 2);
            return bigger;
        }
        case NodeType::Node16: {
            Node16* prv    = static_cast<Node16*>(node);
            Node48* bigger = GetAlloctor<Node48>()->New();
            std::memmove(bigger->next_, prv->next_,
                         static_cast<size_t>(prv->size_) * sizeof(Node*));
            for (int i = 0; i < prv->size_; i++) {
                bigger->keys_[prv->keys_[i]] = i + 1;
            }
            // the positions of the first @prv->size_ elements are all occupied
            bigger->bitmap_ = (~((1ULL << prv->size_) - 1)) & Node48::VALID_MASK;
            HeaderMove(bigger, prv);
            GetAlloctor<Node16>()->Free(prv);
            PREFETCH_W(bigger, 2);
            return bigger;
        }
        case NodeType::Node48: {
            Node48*  prv    = static_cast<Node48*>(node);
            Node256* bigger = GetAlloctor<Node256>()->New();
            for (int i = 0; i < 256; i++) {
                if (prv->keys_[i]) {
                    bigger->next_[i] = prv->next_[prv->keys_[i] - 1];
                }
            }
            HeaderMove(bigger, prv);
            GetAlloctor<Node48>()->Free(prv);
            PREFETCH_W(bigger, 2);
            return bigger;
        }
        case NodeType::Node256:
            CHECK(false) << "no bigger node type.";
            UNREACHABLE();
        default:
            CHECK(false) << "mismatch node type.";
            UNREACHABLE();
        }
    }

    auto NodeShrink(Node* node) -> Node* {
        switch (node->type_) {
        case NodeType::Node4:
            CHECK(false) << "no smaller node type.";
            UNREACHABLE();
        case NodeType::Node16: {
            Node16* prv     = static_cast<Node16*>(node);
            Node4*  smaller = GetAlloctor<Node4>()->New();
            std::memmove(smaller->keys_, prv->keys_, prv->size_);
            std::memmove(smaller->next_, prv->next_,
                         static_cast<size_t>(prv->size_) * sizeof(Node*));
            HeaderMove(smaller, prv);
            GetAlloctor<Node16>()->Free(prv);
            PREFETCH_W(smaller, 2);
            return smaller;
        }
        case NodeType::Node48: {
            Node48* prv     = static_cast<Node48*>(node);
            Node16* smaller = GetAlloctor<Node16>()->New();
            for (int i = 0; i < 256; i++) {
                if (prv->keys_[i]) {
                    smaller->keys_[smaller->size_] = i;
                    smaller->next_[smaller->size_] = prv->next_[prv->keys_[i] - 1];
                    smaller->size_++;
                }
            }
            HeaderMove(smaller, prv);
            GetAlloctor<Node48>()->Free(prv);
            PREFETCH_W(smaller, 2);
            return smaller;
        }
        case NodeType::Node256: {
            Node256* prv     = static_cast<Node256*>(node);
            Node48*  smaller = GetAlloctor<Node48>()->New();
            for (int i = 0; i < 256; i++) {
                if (prv->next_[i]) {
                    smaller->keys_[i]              = smaller->size_ + 1;
                    smaller->next_[smaller->size_] = prv->next_[i];
                    smaller->size_++;
                }
            }
            // the positions of the first @prv->size_ elements are all occupied
            smaller->bitmap_ = (~((1ULL << smaller->size_) - 1)) & Node48::VALID_MASK;
            HeaderMove(smaller, prv);
            GetAlloctor<Node256>()->Free(prv);
            PREFETCH_W(smaller, 2);
            return smaller;
        }
        default:
            CHECK(false) << "mismatch node type.";
            UNREACHABLE();
        }
    }

    auto DestoryArtNode(Node* node) -> void {
        switch (node->type_) {
        case NodeType::Node4:
            return GetAlloctor<Node4>()->Free(node);
        case NodeType::Node16:
            return GetAlloctor<Node16>()->Free(node);
        case NodeType::Node48:
            return GetAlloctor<Node48>()->Free(node);
        case NodeType::Node256:
            return GetAlloctor<Node256>()->Free(node);
        case NodeType::Leaf:
            return GetAlloctor<DataNode>()->Free(node);
        default:
            CHECK(false) << "mismatch node type.";
            UNREACHABLE();
        }
    }

    template <class T>
    auto NewNodeWithPerfix(const byte* data, size_t len) -> T* {
        T* node = AllocateNode<T>();
        if (len > 0) {
            CHECK_LE(len, kMaxPrefixBytes);
            node->prefix_.len_ = len;
            std::memcpy(node->prefix_.data_, data, len);
        }
        return node;
    }

    template <class T, class... Args>
    auto AllocateNode(Args&&... args) -> T* {
        return GetAlloctor<T>()->New(std::forward<Args>(args)...);
    }

    template <class NodeType>
    auto GetAlloctor(NodeType* = nullptr) -> AllocatorOf<NodeType>* {
        static_assert(!std::is_void_v<AllocatorOf<NodeType>>, "unsupported node type");
        constexpr size_t idx = TypeIndex<NodeType, NodeTypes>::value;
        return static_cast<AllocatorOf<NodeType>*>(alloctor_[idx].get());
    }

    Node*                                                       root_{nullptr};
    size_t                                                      size_{0};
    std::array<std::unique_ptr<NodeAlloctor>, kNodeTypeCount> alloctor_;
    std::pmr::memory_resource* mr_{std::pmr::get_default_resource()};
};

} // namespace idlekv
