#pragma once

#include "common/logger.h"
#include "db/storage/art/art_key.h"
#include "db/storage/art/node.h"
#include <cstddef>
#include <cstring>
#include <memory_resource>
#include <utility>

namespace idlekv {

template<class ValueType>
class Art {
public: 
    using DataNode = NodeLeaf<ValueType>;

    template<class V>
    auto insert(const ArtKey& key, V&& value) -> bool {
        if (!root_) [[unlikely]] {
            DataNode* node = allocate_node();
            node->prefix_ = mr_->allocate(key.len);
            std::memcpy(node->prefix_, key.data, key.len);
            node->value_ = std::move(value);
            root_ = node;
            return true;
        }

        return InsertInternal(root_, key, std::forward<V>(value));
    }

    template<class U>
    auto LookUp(U&& key) -> ValueType;

    template<class U>
    auto Delete(U&& key) -> size_t;

private:

    template<class V>
    auto InsertInternal(Node* root, const ArtKey& key, V&& value) -> bool {

    }

    auto header_move(InnerNode* dest, InnerNode* src) -> void {
        dest->size_ = src->size_;
        dest->prefix_len_ = src->prefix_len_;
        if (src->prefix_len_) {
            std::memmove(dest->prefix_, src->prefix_, src->prefix_len_);
        }
    }

    auto node_grow(Node* node) -> Node* {
        switch (node->type_) {
        case NodeType::Node4: {
            Node4* prv = static_cast<Node4*>(node);
            Node16* bigger = allocate_node();
            std::memmove(bigger->keys_, prv->keys_, prv->size_);
            std::memmove(bigger->next_, prv->next_, prv->size_);
            header_move(bigger, prv);
            destory_node(prv);
            return bigger;
        }
        case NodeType::Node16: {
            Node16* prv = static_cast<Node16*>(node);
            Node48* bigger = allocate_node();
            std::memmove(bigger->next_, prv->next_, prv->size_);
            for (int i = 0;i < prv->size_;i++) {
                bigger->keys_[prv->keys_[i]] = i + 1;
            }
            header_move(bigger, prv);
            destory_node(prv);
            return bigger;
        }
        case NodeType::Node48: {
            Node48* prv = static_cast<Node48*>(node);
            Node256* bigger = allocate_node();
            for (int i = 0;i < 256;i++) {
                if (prv->keys_[i]) {
                    bigger->next_[i] = prv->next_[prv->keys_[i] - 1];
                }
            }
            header_move(bigger, prv);
            destory_node(prv);
            return bigger;
        }
        case NodeType::Node256: 
            CHECK(false) << "no bigger node type.";
        default:
            CHECK(false) << "mismatch node type.";
        }
    }

    auto node_shrink(Node* node) -> Node* {
        switch (node->type_) {
        case NodeType::Node4:
            CHECK(false) << "no smaller node type.";
        case NodeType::Node16: {
            Node16* prv = static_cast<Node16*>(node);
            Node4* smaller = allocate_node();
            std::memmove(smaller->keys_, prv->keys_, prv->size_);
            std::memmove(smaller->next_, prv->next_, prv->size_);
            header_move(smaller, prv);
            destory_node(prv);
            return smaller;
        }
        case NodeType::Node48: {
            Node48* prv = static_cast<Node48*>(node);
            Node16* smaller = allocate_node();
            for (int i = 0; i < 256; i++) {
                if (prv->keys_[i]) {
                    smaller->keys_[smaller->size_] = i;
                    smaller->next_[smaller->size_] = prv->next_[prv->keys_[i] - 1];
                    smaller->size_++;
                }
            }
            header_move(smaller, prv);
            return smaller;
        }
        case NodeType::Node256: {
            Node256* prv = static_cast<Node256*>(node);
            Node48* smaller = allocate_node();
            for (int i = 0; i < 256; i++) {
                if (prv->next_[i]) {
                    smaller->keys_[i] = smaller->size_ + 1;
                    smaller->next_[smaller->size_] = prv->next_[i];
                    smaller->size_++;
                }
            }
            header_move(smaller, prv);
            return smaller;
        }
        default:
            CHECK(false) << "mismatch node type.";
        }
    }

    template<typename T, typename... Args>
    auto allocate_node(Args&&... args) -> T* {
        void* ptr = mr_->allocate(sizeof(T), alignof(T));
        return new (ptr) T(std::forward<Args>(args)...);
    }

    template<typename T>
    auto destory_node(T* node) -> void {
        static_cast<T*>(node)->~T();
        mr_->deallocate(node, sizeof(T), alignof(T));
    }

    Node* root_;
    std::pmr::memory_resource* mr_;
};

} // namespace idlekv