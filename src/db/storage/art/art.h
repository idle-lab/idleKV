#pragma once

#include "common/config.h"
#include "common/logger.h"
#include "db/storage/alloctor.h"
#include "db/storage/art/art_key.h"
#include "db/storage/art/node.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <memory_resource>
#include <type_traits>
#include <utility>

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

    using Node4Alloctor    = FixMemoryAlloctor<Node4>;
    using Node16Alloctor   = FixMemoryAlloctor<Node16>;
    using Node48Alloctor   = FixMemoryAlloctor<Node48>;
    using Node256Alloctor  = FixMemoryAlloctor<Node256>;
    using DataNodeAlloctor = FixMemoryAlloctor<DataNode>;

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

    explicit Art(std::pmr::memory_resource* mr, EBRManager* ebr_mgr = nullptr)
        : ebr_mgr_(ebr_mgr), mr_(mr) {
        alloctor_[TypeIndex<Node4, NodeTypes>::value] =
            std::make_unique<Node4Alloctor>(mr, ebr_mgr);
        alloctor_[TypeIndex<Node16, NodeTypes>::value] =
            std::make_unique<Node16Alloctor>(mr, ebr_mgr);
        alloctor_[TypeIndex<Node48, NodeTypes>::value] =
            std::make_unique<Node48Alloctor>(mr, ebr_mgr);
        alloctor_[TypeIndex<Node256, NodeTypes>::value] =
            std::make_unique<Node256Alloctor>(mr, ebr_mgr);
        alloctor_[TypeIndex<DataNode, NodeTypes>::value] =
            std::make_unique<DataNodeAlloctor>(mr, ebr_mgr);
    }

    struct InsertResutl {
        enum Status {
            OK,
            UpsertValue,
            DuplicateKey,
        };
        InsertResutl(Status status) : s(status) {}
        InsertResutl(ValueType* data) : value(data), s(Status::OK) {}

        auto operator==(Status other) -> bool { return s == other; }

        ValueType* value{nullptr};
        Status     s;
    };

    template <class V>
    auto Insert(ArtKey key, V&& value, InsertMode mode = InsertMode::InsertOnly) noexcept
        -> InsertResutl {
        if (UNLIKELY(!root_)) [[unlikely]] {
            DataNode* node     = AllocateNode<DataNode>();
            node->prefix_.len_ = key.Len();
            std::memcpy(node->prefix_.data_, key.Data(), key.Len());
            node->value_ = std::move(value);
            root_        = node;
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

    // TODO(cyb): Range, BulkInsert, RangeDelete
private:
    template <class V>
    auto InsertInternal(Node* cur, Node** cur_ref, ArtKey& key, V&& value, InsertMode mode) noexcept
        -> InsertResutl {
        size_t len;
        while (true) {
            len = cur->CheckPerfix(key.Data());

            if (len != cur->prefix_.len_) {
                // prefix mismatch
                Node4*    node = NewNodeWithPerfix<Node4>(cur->prefix_.data_, len);
                DataNode* leaf =
                    NewNodeWithPerfix<DataNode>(key.Data() + len + 1, key.Len() - len - 1);
                leaf->value_ = std::move(value);
                node->SetNext(cur->prefix_.data_[len], cur);
                node->SetNext(key.Data()[len], leaf);

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
                DataNode* leaf  = NewNodeWithPerfix<DataNode>(key.Data() + 1, key.Len() - 1);
                leaf->value_    = std::move(value);
                Node* maybe_new = SetNext(cur, *key.Data(), leaf);
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
                    // Compress parent prefix + remaining edge + child prefix into child.
                    size_t total = n4->prefix_.len_ + next->prefix_.len_ + 1;
                    CHECK_LE(total, kMaxPrefixBytes);

                    if (next->prefix_.len_ > 0) {
                        std::memmove(next->prefix_.data_ + n4->prefix_.len_ + 1,
                                     next->prefix_.data_, next->prefix_.len_);
                    }
                    next->prefix_.data_[n4->prefix_.len_] = n4->keys_[0];
                    if (n4->prefix_.len_ > 0) {
                        std::memmove(next->prefix_.data_, n4->prefix_.data_, n4->prefix_.len_);
                    }
                    next->prefix_.len_ = total;

                    DestoryArtNode(parent);
                    *parent_ref = next;
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
    EBRManager*                                                 ebr_mgr_;
    std::array<std::unique_ptr<MemoryAlloctor>, kNodeTypeCount> alloctor_;
    std::pmr::memory_resource* mr_{std::pmr::get_default_resource()};
};

} // namespace idlekv
