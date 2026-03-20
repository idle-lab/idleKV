#pragma once

#include "common/config.h"
#include "common/logger.h"
#include "db/storage/art/art_key.h"
#include "db/storage/art/node.h"
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory_resource>
#include <optional>
#include <utility>

namespace idlekv {

enum struct InsertMode : uint8_t {
    InsertOnly,
    Upsert,
    OOM, // TODO(cyb): support max memory usage.
};

enum struct InsertResutl : uint8_t {
    OK,
    UpsertValue,
    DuplicateKey,
};

template<class ValueType>
class Art {
public: 
    using DataNode = NodeLeaf<ValueType>;

    explicit Art(std::pmr::memory_resource* mr = std::pmr::get_default_resource()) : mr_(mr) {}

    template<class V>
    auto Insert(ArtKey& key, V&& value, InsertMode mode = InsertMode::InsertOnly) noexcept -> InsertResutl {
        if (UNLIKELY(!root_)) [[unlikely]] {
            DataNode* node = AllocateNode<DataNode>();
            node->prefix_ = static_cast<byte*>(mr_->allocate(key.Len()));
            node->prefix_len_ = key.Len();
            std::memcpy(node->prefix_, key.Data(), key.Len());
            node->value_ = std::move(value);
            root_ = node;
            size_++;
            return InsertResutl::OK;
        }
        auto res = InsertInternal(root_, &root_, key, std::forward<V>(value), mode);
        if (res == InsertResutl::OK) {
            size_++;
        }
        return res;
    }

    // return a copy of value, so you should consider the cost of value copying.
    auto Lookup(ArtKey& key) noexcept -> std::optional<ValueType> {
        if (UNLIKELY(!root_)) [[unlikely]] {
            return std::nullopt;
        }
        return LookupInternal(root_, key);
    }

    auto Erase(ArtKey& key) noexcept -> size_t {
        if (UNLIKELY(!root_)) [[unlikely]] {
            return 0;
        }

        size_t erased = EraseInternal(root_, &root_, key);
        if (erased != 0) {
            --size_;
        }
        return erased;
    }

    // TODO(cyb): Range, BulkInsert, RangeDelete
private:

    template<class V>
    auto InsertInternal(Node* cur, Node** cur_ref, ArtKey& key, V&& value, InsertMode mode) noexcept -> InsertResutl {
        size_t len;
        while (true) {
            len = cur->CheckPerfix(key.Data(), key.Len());

            if (len != cur->prefix_len_) {
                // prefix mismatch
                Node4* node = NewNodeWithPerfix<Node4>(cur->prefix_, len);
                DataNode* leaf = NewNodeWithPerfix<DataNode>(key.Data() + len + 1, key.Len() - len - 1);
                leaf->value_ = std::move(value);
                node->SetNext(cur->prefix_[len], cur);
                node->SetNext(key.Data()[len], leaf);
    
                if (cur->prefix_len_ > len + 1) {
                    size_t new_perfix_len = cur->prefix_len_ - len - 1;
                    byte* new_perfix = static_cast<byte*>(mr_->allocate(new_perfix_len));
                    std::memmove(new_perfix, cur->prefix_ + len + 1, new_perfix_len);
                    mr_->deallocate(cur->prefix_, cur->prefix_len_);
                    cur->prefix_ = new_perfix;
                    cur->prefix_len_ = new_perfix_len;
                } else {
                    mr_->deallocate(cur->prefix_, cur->prefix_len_);
                    cur->prefix_ = nullptr;
                    cur->prefix_len_ = 0;
                }

                *cur_ref = node;
                return InsertResutl::OK;
            }

            if (key.Len() == cur->prefix_len_) {
                // exact match
                // cur must be a leaf
                CHECK_EQ(cur->type_, NodeType::Leaf);
                if (mode == InsertMode::Upsert) {
                    DataNode* node = static_cast<DataNode*>(cur);
                    node->value_ = std::move(value);

                    return InsertResutl::UpsertValue;
                }

                return InsertResutl::DuplicateKey;
            }

            key.Cut(len);
            Node** next = FindNext(cur, *key.Data());
            if (next == nullptr) {
                DataNode* leaf = NewNodeWithPerfix<DataNode>(key.Data() + 1, key.Len() - 1);
                leaf->value_ = std::move(value);
                Node* maybe_new = SetNext(cur, *key.Data(), leaf);
                if (maybe_new != cur) {
                    *cur_ref = maybe_new;
                }

                return InsertResutl::OK;
            }

            key.Cut(1);
            cur_ref = next;
            cur = *next;
        }
        UNREACHABLE();
    }

    auto LookupInternal(Node* cur, ArtKey& key) noexcept -> std::optional<ValueType> {
        while (true) {
            size_t len = cur->CheckPerfix(key.Data(), key.Len());

            if (len != cur->prefix_len_) {
                // prefix mismatch => key doesn't exist
                return std::nullopt;
            }

            if (key.Len() == cur->prefix_len_) {
                // exact match
                CHECK_EQ(cur->type_, NodeType::Leaf);
                DataNode* node = static_cast<DataNode*>(cur);
                return node->value_;
            }

            Node** next = FindNext(cur, key.Data()[len]);
            if (next == nullptr) {
                return std::nullopt;
            }

            key.Cut(len + 1);
            cur = *next;
        }
        UNREACHABLE();
    }

    auto EraseInternal(Node* cur, Node** cur_ref, ArtKey& key) noexcept -> size_t {
        Node** parent_ref = nullptr;
        InnerNode* parent = nullptr;
        byte last_partial_key = 0;
        while (true) {
            size_t len = cur->CheckPerfix(key.Data(), key.Len());
            if (len != cur->prefix_len_) {
                // prefix mismatch => key doesn't exist
                return 0;
            }

            if (key.Len() == cur->prefix_len_) {
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
                    parent = static_cast<InnerNode*>(maybe_new);
                }

                if (parent->size_ == 1) {
                    // path compression
                    // parent must be a node4 type
                    CHECK_EQ(parent->type_, NodeType::Node4);
                    Node4* n4 = static_cast<Node4*>(parent);
                    Node* next = n4->next_[0];
                    const byte remaining_edge = n4->keys_[0];
                    // Compress parent prefix + remaining edge + child prefix into child.
                    size_t total = n4->prefix_len_ + next->prefix_len_ + 1;
                    byte* new_prefix = static_cast<byte*>(mr_->allocate(total));
                    if (n4->prefix_len_ > 0) {
                        std::memmove(new_prefix, n4->prefix_, n4->prefix_len_);
                    }
                    new_prefix[n4->prefix_len_] = remaining_edge;
                    if (next->prefix_len_ > 0) {
                        std::memmove(new_prefix + n4->prefix_len_ + 1, next->prefix_, next->prefix_len_);
                        mr_->deallocate(next->prefix_, next->prefix_len_);
                    }

                    next->prefix_ = new_prefix;
                    next->prefix_len_ = total;

                    DestoryArtNode(parent);
                    *parent_ref = next;
                }

                return 1;
            }

            Node** next = FindNext(cur, key.Data()[len]);
            if (next == nullptr) {
                return 0;
            }
            last_partial_key = key.Data()[len];
            key.Cut(len + 1);
            parent_ref = cur_ref;
            parent = static_cast<InnerNode*>(cur);
            cur_ref = next;
            cur = *next;
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

    template<class CurNode, class BiggerNode>
    auto SetNextOrGrow(CurNode* node, byte key, Node* next) -> Node* {
        if (!node->IsFull()) {
            node->SetNext(key, next);
            return node;
        }

        BiggerNode* bigger = static_cast<BiggerNode*>(NodeGrow(node));
        bigger->SetNext(key, next);
        return bigger;
    }

    template<class CurNode, class SmallerNode>
    auto DelNextOrShrink(CurNode* node, byte key) -> Node* {
        Node* child_to_delete = node->DelNext(key);
        DestoryArtNode(child_to_delete);
        return node->UnderFull() ? NodeShrink(node) : node;
    }

    auto HeaderMove(InnerNode* dest, InnerNode* src) -> void {
        dest->size_ = src->size_;
        dest->prefix_len_ = src->prefix_len_;
        dest->prefix_ = src->prefix_;
        src->size_ = 0;
        src->prefix_ = nullptr;
        src->prefix_len_ = 0;
    }

    auto NodeGrow(Node* node) -> Node* {
        switch (node->type_) {
        case NodeType::Node4: {
            Node4* prv = static_cast<Node4*>(node);
            Node16* bigger = AllocateNode<Node16>();
            std::memmove(bigger->keys_, prv->keys_, prv->size_);
            std::memmove(bigger->next_, prv->next_, static_cast<size_t>(prv->size_) * sizeof(Node*));
            HeaderMove(bigger, prv);
            DestroyNode(prv);
            return bigger;
        }
        case NodeType::Node16: {
            Node16* prv = static_cast<Node16*>(node);
            Node48* bigger = AllocateNode<Node48>();
            std::memmove(bigger->next_, prv->next_, static_cast<size_t>(prv->size_) * sizeof(Node*));
            for (int i = 0;i < prv->size_;i++) {
                bigger->keys_[prv->keys_[i]] = i + 1;
            }
            HeaderMove(bigger, prv);
            DestroyNode(prv);
            return bigger;
        }
        case NodeType::Node48: {
            Node48* prv = static_cast<Node48*>(node);
            Node256* bigger = AllocateNode<Node256>();
            for (int i = 0;i < 256;i++) {
                if (prv->keys_[i]) {
                    bigger->next_[i] = prv->next_[prv->keys_[i] - 1];
                }
            }
            HeaderMove(bigger, prv);
            DestroyNode(prv);
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
            Node16* prv = static_cast<Node16*>(node);
            Node4* smaller = AllocateNode<Node4>();
            std::memmove(smaller->keys_, prv->keys_, prv->size_);
            std::memmove(smaller->next_, prv->next_, static_cast<size_t>(prv->size_) * sizeof(Node*));
            HeaderMove(smaller, prv);
            DestroyNode(prv);
            return smaller;
        }
        case NodeType::Node48: {
            Node48* prv = static_cast<Node48*>(node);
            Node16* smaller = AllocateNode<Node16>();
            for (int i = 0; i < 256; i++) {
                if (prv->keys_[i]) {
                    smaller->keys_[smaller->size_] = i;
                    smaller->next_[smaller->size_] = prv->next_[prv->keys_[i] - 1];
                    smaller->size_++;
                }
            }
            HeaderMove(smaller, prv);
            DestroyNode(prv);
            return smaller;
        }
        case NodeType::Node256: {
            Node256* prv = static_cast<Node256*>(node);
            Node48* smaller = AllocateNode<Node48>();
            for (int i = 0; i < 256; i++) {
                if (prv->next_[i]) {
                    smaller->keys_[i] = smaller->size_ + 1;
                    smaller->next_[smaller->size_] = prv->next_[i];
                    smaller->size_++;
                }
            }
            HeaderMove(smaller, prv);
            DestroyNode(prv);
            return smaller;
        }
        default:
            CHECK(false) << "mismatch node type.";
            UNREACHABLE();
        }
    }

    template<class T>
    auto NewNodeWithPerfix(const byte* data, size_t len) -> T* {
        T* node = AllocateNode<T>();
        if (len > 0) {
            node->prefix_len_ = len;
            node->prefix_ = static_cast<byte*>(mr_->allocate(len));
            std::memcpy(node->prefix_, data, len);
        }
        return node;
    }

    // TODO(cyb): cache Art Node, avoid alloc memory from heap every time.
    template<typename T, typename... Args>
    auto AllocateNode(Args&&... args) -> T* {
        void* ptr = mr_->allocate(sizeof(T), alignof(T));
        return new (ptr) T(std::forward<Args>(args)...);
    }

    template<typename T>
    auto DestroyNode(T* node) -> void {
        if (node->prefix_ != nullptr) {
            mr_->deallocate(node->prefix_, node->prefix_len_);
            node->prefix_ = nullptr;
            node->prefix_len_ = 0;
        }
        static_cast<T*>(node)->~T();
        mr_->deallocate(node, sizeof(T), alignof(T));
    }

    auto DestoryArtNode(Node* node) -> void {
        if (node == nullptr) {
            return;
        }
        switch (node->type_) {
        case NodeType::Node4:
            return DestroyNode<Node4>(static_cast<Node4*>(node));
        case NodeType::Node16: 
            return DestroyNode<Node16>(static_cast<Node16*>(node));
        case NodeType::Node48:
            return DestroyNode<Node48>(static_cast<Node48*>(node));
        case NodeType::Node256:
            return DestroyNode<Node256>(static_cast<Node256*>(node));
        case NodeType::Leaf:
            return DestroyNode<DataNode>(static_cast<DataNode*>(node));
        default:
            CHECK(false) << "mismatch node type";
            UNREACHABLE();
        }
    }

    Node* root_{nullptr};
    size_t size_{0};
    std::pmr::memory_resource* mr_{std::pmr::get_default_resource()};
};

} // namespace idlekv
