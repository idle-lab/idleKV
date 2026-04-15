#pragma once

#include "absl/container/inlined_vector.h"
#include "common/config.h"
#include "common/logger.h"
#include "db/storage/art/art_key.h"
#include "db/storage/art/node.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory_resource>
#include <optional>
#include <span>
#include <type_traits>
#include <utility>
#include <vector>

namespace idlekv {

template <class>
struct ArtTestAccess;

enum struct InsertMode : uint8_t {
    InsertOnly,
    Upsert,
    IfExistGetValue, // TODO(cyb): deprecate.
    OOM,             // TODO(cyb): support max memory usage.
};

template <class ValueType>
class Art {
public:
    // The tree indexes arbitrary binary keys by bytewise lexicographic order.
    // Path compression stores unmatched suffixes on each node instead of one byte
    // per level, which keeps lookup hot paths shallow without changing key order.
    using DataNode     = NodeLeaf<ValueType>;
    using AncestorPath = absl::InlinedVector<InnerNode*, 16>;
    class Cursor;

    explicit Art(std::pmr::memory_resource* mr) : mr_(mr) {}

    ~Art() { DestroySubtree(root_); }

    struct InsertResutl {
        enum Status {
            OK,
            UpsertValue,
            DuplicateKey,
        };
        InsertResutl(Status status) : status(status) {}
        InsertResutl(ValueType* data) : value(data), status(Status::OK) {}

        auto operator==(Status other) const -> bool { return status == other; }

        ValueType* value{nullptr};
        Status     status;
    };

    struct RangeEntry {
        std::vector<byte> key;
        ValueType*        value{nullptr};
    };

    class Cursor {
    public:
        Cursor() = default;

        auto Valid() const -> bool { return leaf_ != nullptr; }

        auto Key() const -> std::span<const byte> {
            CheckVersion();
            CHECK(leaf_ != nullptr);
            return {key_buf_.data(), key_buf_.size()};
        }

        auto Value() const -> ValueType& {
            CheckVersion();
            CHECK(leaf_ != nullptr);
            return static_cast<DataNode*>(leaf_)->value_;
        }

        auto ValuePtr() const -> ValueType* {
            CheckVersion();
            return leaf_ == nullptr ? nullptr : &static_cast<DataNode*>(leaf_)->value_;
        }

        auto Next() -> void {
            if (leaf_ == nullptr || owner_ == nullptr) {
                return;
            }
            CheckVersion();

            // ART nodes do not record parent pointers, so the cursor carries an
            // explicit root-to-leaf stack and climbs back up until it finds the
            // first ancestor with an unvisited greater sibling.
            for (size_t depth = stack_.size(); depth > 0; --depth) {
                const Frame frame = stack_[depth - 1];
                key_buf_.resize(frame.key_size_after_prefix);
                stack_.resize(depth - 1);

                auto sibling = owner_->FindChildGt(frame.node, frame.edge);
                if (!sibling.has_value()) {
                    continue;
                }

                stack_.push_back(Frame{frame.node, sibling->edge, frame.key_size_after_prefix});
                key_buf_.push_back(sibling->edge);
                owner_->DescendLeftmost(sibling->child, *this);
                return;
            }

            leaf_ = nullptr;
            stack_.clear();
            key_buf_.clear();
        }

    private:
        struct Frame {
            // key_size_after_prefix points at the position right after the ancestor
            // prefix bytes. Next() rewinds to this offset before swapping in a
            // different edge byte, which avoids rebuilding the full key each step.
            InnerNode* node{nullptr};
            byte       edge{0};
            size_t     key_size_after_prefix{0};
        };

        explicit Cursor(Art* owner)
            : owner_(owner), expected_version_(owner == nullptr ? 0 : owner->version_) {}

        auto CheckVersion() const -> void {
            if (owner_ == nullptr) {
                return;
            }
            // Grow/shrink and path compression can replace nodes in-place. We keep
            // cursor semantics simple by invalidating every cursor after a mutation.
            CHECK_EQ(expected_version_, owner_->version_) << "ART cursor invalidated by mutation";
        }

        Art*                           owner_{nullptr};
        Node*                          leaf_{nullptr};
        absl::InlinedVector<Frame, 16> stack_{};
        absl::InlinedVector<byte, 64>  key_buf_{};
        uint64_t                       expected_version_{0};

        friend class Art;
    };

public:
    // ================================================
    //      API Function
    // ================================================
    template <class V>
    auto Insert(ArtKey key, V&& value, InsertMode mode = InsertMode::InsertOnly) noexcept
        -> InsertResutl {
        AncestorPath path;
        auto         res = InsertInternal(&root_, key, std::forward<V>(value), mode, path);
        const bool   inserted_new_leaf = res.status == InsertResutl::OK && res.value == nullptr;
        if (inserted_new_leaf) {
            AdjustValueCount(path, +1);
            ++size_;
        }
        if (inserted_new_leaf || res == InsertResutl::UpsertValue) {
            ++version_;
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
        AncestorPath path;
        size_t       erased = EraseInternal(&root_, key, path);
        if (erased != 0) {
            size_ -= erased;
            AdjustValueCount(path, -1);
            ++version_;
        }
        return erased;
    }

    auto Begin() -> Cursor {
        Cursor cursor(this);
        if (root_ == nullptr) {
            return cursor;
        }

        DescendLeftmost(root_, cursor);
        return cursor;
    }

    auto End() -> Cursor { return Cursor(this); }

    auto LowerBound(ArtKey key) -> Cursor {
        Cursor cursor(this);
        if (root_ == nullptr) {
            return cursor;
        }

        // lower_bound is the primitive ordered seek operation. Range scans and any
        // future prefix/ordered scans should build on it instead of open-coding DFS.
        if (!SeekLowerBound(root_, AsSpan(key), cursor)) {
            return End();
        }

        return cursor;
    }

    auto SeekByRank(uint32_t rank) -> Cursor {
        Cursor cursor(this);
        if (root_ == nullptr || static_cast<size_t>(rank) >= size_) {
            return cursor;
        }

        uint32_t remaining = rank;
        Node*    cur       = root_;
        while (cur->Type() != NodeType::Leaf) {
            auto* inner = AsInner(cur);
            AppendPrefix(cur, cursor.key_buf_);

            ChildRef     chosen{};
            bool         found                 = false;
            const size_t key_size_after_prefix = cursor.key_buf_.size();
            ForEachChildInOrder(inner, [&](byte edge, Node* child) {
                const uint32_t child_count = SubtreeValueCount(child);
                if (remaining < child_count) {
                    chosen = ChildRef{edge, child};
                    found  = true;
                    return false;
                }
                remaining -= child_count;
                return true;
            });

            CHECK(found);
            cursor.stack_.push_back(
                typename Cursor::Frame{inner, chosen.edge, key_size_after_prefix});
            cursor.key_buf_.push_back(chosen.edge);
            cur = chosen.child;
        }

        AppendPrefix(cur, cursor.key_buf_);
        cursor.leaf_ = cur;
        return cursor;
    }

    template <class Fn>
    auto IterateByRank(uint32_t rank_start, uint32_t rank_end, Fn&& fn) -> bool {
        if (static_cast<size_t>(rank_start) >= size_) {
            return true;
        }

        rank_end = std::min<uint32_t>(rank_end, static_cast<uint32_t>(size_ - 1));
        auto it  = SeekByRank(rank_start);
        for (uint32_t rank = rank_start; rank <= rank_end && it.Valid();) {
            if (!fn(it)) {
                return false;
            }
            if (rank == rank_end) {
                break;
            }
            ++rank;
            it.Next();
        }
        return true;
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
        if (root_ == nullptr || CompareEncoded(AsSpan(min), AsSpan(max)) > 0) {
            return matches;
        }

        // Range is intentionally layered on top of LowerBound()+Cursor so ordered
        // scans only have one traversal implementation to maintain.
        for (auto it = LowerBound(min); it.Valid(); it.Next()) {
            const auto key = it.Key();
            if (CompareEncoded(key, AsSpan(max)) > 0) {
                break;
            }

            matches.push_back(RangeEntry{std::vector<byte>(key.begin(), key.end()), it.ValuePtr()});
        }
        return matches;
    }

    auto Min() noexcept -> ValueType* { return EdgeValue(/*find_min=*/true); }

    auto Max() noexcept -> ValueType* { return EdgeValue(/*find_min=*/false); }

    auto Size() const noexcept -> size_t { return size_; }

    auto Empty() const noexcept -> bool { return size_ == 0; }

private:
    struct ChildRef {
        // Iteration needs both the selected child and the edge byte that leads to
        // it, because that edge byte becomes part of the reconstructed full key.
        byte  edge{0};
        Node* child{nullptr};
    };

    // ================================================
    //      API Internal Implement
    // ================================================
    template <class V>
    auto InsertInternal(Node** root_ref, ArtKey key, V&& value, InsertMode mode,
                        AncestorPath& path) noexcept -> InsertResutl {
        if (UNLIKELY(*root_ref == nullptr)) [[unlikely]] {
            *root_ref = CreateLeaf(key.Data(), key.Len(), std::forward<V>(value));
            return InsertResutl::OK;
        }

        Node** cur_ref = root_ref;
        Node*  cur     = *cur_ref;
        while (true) {
            const size_t len = cur->CheckPerfix(key.Data(), key.Len());

            if (len != cur->PrefixLen()) {
                // A mismatch inside a compressed prefix means the current path must
                // be split. We materialize a new Node4 at the shared prefix length
                // and hang the old subtree and new leaf off the first differing byte.
                const byte* cur_prefix = cur->PrefixData();
                Node4*      split      = NewNodeWithPerfix<Node4>(cur_prefix, len);
                split->value_count_    = SubtreeValueCount(cur);
                split->SetNext(cur_prefix[len], cur);
                split->SetNext(
                    key.Data()[len],
                    CreateLeaf(key.Data() + len + 1, key.Len() - len - 1, std::forward<V>(value)));

                if (cur->PrefixLen() > len + 1) {
                    cur->SetPrefix(cur_prefix + len + 1, cur->PrefixLen() - len - 1, mr_);
                } else {
                    cur->ClearPrefix(mr_);
                }

                *cur_ref = split;
                path.push_back(split);
                return InsertResutl::OK;
            }

            if (key.Len() == cur->PrefixLen()) {
                // exact match
                CHECK_EQ(cur->Type(), NodeType::Leaf);
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
            InnerNode* inner = AsInner(cur);
            path.push_back(inner);
            Node** next = FindNext(inner, *key.Data());
            if (next == nullptr) {
                InnerNode* maybe_new =
                    SetNext(inner, *key.Data(),
                            CreateLeaf(key.Data() + 1, key.Len() - 1, std::forward<V>(value)));
                if (maybe_new != inner) {
                    *cur_ref    = maybe_new;
                    path.back() = maybe_new;
                }

                return InsertResutl::OK;
            }

            PREFETCH_W(*next, 2);
            key.Cut(1);
            cur_ref = next;
            cur     = *next;
        }
    }

    auto LookupInternal(Node* cur, ArtKey& key) noexcept -> ValueType* {
        while (true) {
            if (cur->CheckPerfix(key.Data(), key.Len()) != cur->PrefixLen()) {
                // prefix mismatch => key doesn't exist
                return nullptr;
            }

            if (key.Len() == cur->PrefixLen()) {
                // exact match
                if (cur->Type() != NodeType::Leaf) {
                    return nullptr;
                }
                PREFETCH_R(&static_cast<DataNode*>(cur)->value_, 1);
                return &static_cast<DataNode*>(cur)->value_;
            }

            InnerNode* inner = AsInner(cur);
            Node**     next  = FindNext(inner, key.Data()[cur->PrefixLen()]);
            if (next == nullptr) {
                return nullptr;
            }
            PREFETCH_R(*next, 2);
            key.Cut(cur->PrefixLen() + 1);
            cur = *next;
        }
    }

    auto EraseInternal(Node** root_ref, ArtKey key, AncestorPath& path) noexcept -> size_t {
        if (UNLIKELY(*root_ref == nullptr)) [[unlikely]] {
            return 0;
        }

        Node**     cur_ref          = root_ref;
        Node*      cur              = *cur_ref;
        Node**     parent_ref       = nullptr;
        InnerNode* parent           = nullptr;
        byte       last_partial_key = 0;
        while (true) {
            size_t len = cur->CheckPerfix(key.Data(), key.Len());
            if (len != cur->PrefixLen()) {
                // prefix mismatch => key doesn't exist
                return 0;
            }

            if (key.Len() == cur->PrefixLen()) {
                // exact match
                // cur must be a leaf node, if not key doesn't exist
                if (cur->Type() != NodeType::Leaf) {
                    return 0;
                }

                if (UNLIKELY(parent == nullptr)) [[unlikely]] {
                    // match results at the root node
                    DestoryArtNode(cur);
                    *cur_ref = nullptr;
                    return 1;
                }

                InnerNode* maybe_new = DelNext(parent, last_partial_key);
                if (maybe_new != parent) {
                    *parent_ref = maybe_new;
                    parent      = maybe_new;
                    path.back() = maybe_new;
                }

                if (parent->size_ == 1) {
                    // path compression
                    // parent must be a node4 type
                    CHECK_EQ(parent->Type(), NodeType::Node4);
                    Node4*       n4    = static_cast<Node4*>(parent);
                    Node*        next  = n4->next_[0];
                    size_t       total = n4->PrefixLen() + 1 + next->PrefixLen();
                    Node::Prefix new_prefix{};
                    byte*        buff = nullptr;

                    if (total <= kInlinePrefixBytes) {
                        buff = new_prefix.inline_;
                    } else {
                        new_prefix.heap_ = static_cast<byte*>(mr_->allocate(total, alignof(byte)));
                        buff             = new_prefix.heap_;
                    }

                    std::memmove(buff, n4->PrefixData(), n4->PrefixLen());
                    buff[n4->PrefixLen()] = n4->keys_[0];
                    std::memmove(buff + n4->PrefixLen() + 1, next->PrefixData(), next->PrefixLen());

                    DestoryArtNode(parent);
                    next->SetNewPrefix(new_prefix, total, mr_);
                    *parent_ref = next;
                    path.pop_back();
                }

                return 1;
            }

            key.Cut(len);
            InnerNode* inner = AsInner(cur);
            path.push_back(inner);
            Node** next = FindNext(inner, *key.Data());
            if (next == nullptr) {
                return 0;
            }
            PREFETCH_W(*next, 2);
            last_partial_key = *key.Data();
            key.Cut(1);
            parent_ref = cur_ref;
            parent     = inner;
            cur_ref    = next;
            cur        = *next;
        }
    }

    template <class Fn>
    auto TraverseInOrder(Node* node, std::vector<byte>& key, Fn& fn) -> bool {
        const size_t original_size = key.size();
        key.insert(key.end(), node->PrefixData(), node->PrefixData() + node->PrefixLen());

        if (node->Type() == NodeType::Leaf) {
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

        const bool keep_going = ForEachChildInOrder(AsInner(node), visit_child);
        key.resize(original_size);
        return keep_going;
    }

    auto DescendLeftmost(Node* node, Cursor& cursor) -> void {
        cursor.leaf_ = nullptr;

        Node* cur = node;
        while (cur->Type() != NodeType::Leaf) {
            auto* inner = AsInner(cur);
            // Leaves only store their local suffix, so the cursor owns a mutable
            // key buffer and appends prefixes/edge bytes as it descends.
            AppendPrefix(cur, cursor.key_buf_);

            auto child = FirstChildRef(inner);
            CHECK(child.has_value());

            cursor.stack_.push_back(
                typename Cursor::Frame{inner, child->edge, cursor.key_buf_.size()});
            cursor.key_buf_.push_back(child->edge);
            cur = child->child;
        }

        AppendPrefix(cur, cursor.key_buf_);
        cursor.leaf_ = cur;
    }

    auto SeekLowerBound(Node* node, std::span<const byte> target, Cursor& cursor) -> bool {
        const size_t entry_key_size   = cursor.key_buf_.size();
        const size_t entry_stack_size = cursor.stack_.size();
        const auto   prefix           = PrefixSpan(node);

        const size_t limit = std::min(prefix.size(), target.size());
        const size_t common =
            std::mismatch(prefix.begin(), prefix.begin() + limit, target.begin()).first -
            prefix.begin();
        if (common < prefix.size() && common < target.size()) {
            cursor.stack_.resize(entry_stack_size);
            cursor.key_buf_.resize(entry_key_size);
            cursor.leaf_ = nullptr;
            // The mismatch happened inside a compressed prefix. If the subtree
            // prefix is already greater than the target, the leftmost key in this
            // subtree is the lower_bound; otherwise the whole subtree is too small.
            if (prefix[common] > target[common]) {
                DescendLeftmost(node, cursor);
                return true;
            }
            return false;
        }

        if (target.size() < prefix.size()) {
            cursor.stack_.resize(entry_stack_size);
            cursor.key_buf_.resize(entry_key_size);
            cursor.leaf_ = nullptr;
            // A shorter target that is a prefix of this compressed path sorts
            // before every key in the subtree, so the answer is the leftmost leaf.
            DescendLeftmost(node, cursor);
            return true;
        }

        AppendPrefix(node, cursor.key_buf_);

        if (node->Type() == NodeType::Leaf) {
            if (target.size() == prefix.size()) {
                cursor.leaf_ = node;
                return true;
            }

            cursor.stack_.resize(entry_stack_size);
            cursor.key_buf_.resize(entry_key_size);
            cursor.leaf_ = nullptr;
            return false;
        }

        auto* inner = AsInner(node);
        if (target.size() == prefix.size()) {
            auto child = FirstChildRef(inner);
            CHECK(child.has_value());

            cursor.stack_.push_back(
                typename Cursor::Frame{inner, child->edge, cursor.key_buf_.size()});
            cursor.key_buf_.push_back(child->edge);
            DescendLeftmost(child->child, cursor);
            return true;
        }

        const size_t after_prefix_key_size = cursor.key_buf_.size();
        const byte   wanted                = target[prefix.size()];
        if (Node** exact = FindNext(inner, wanted); exact != nullptr) {
            cursor.stack_.push_back(typename Cursor::Frame{inner, wanted, after_prefix_key_size});
            cursor.key_buf_.push_back(wanted);
            if (SeekLowerBound(*exact, target.subspan(prefix.size() + 1), cursor)) {
                return true;
            }

            cursor.stack_.resize(entry_stack_size);
            cursor.key_buf_.resize(after_prefix_key_size);
            cursor.leaf_ = nullptr;

            // The exact branch exists but all keys in it are still < target. Fall
            // back to the next greater sibling under the same ancestor.
            if (auto greater = FindChildGt(inner, wanted); greater.has_value()) {
                cursor.stack_.push_back(
                    typename Cursor::Frame{inner, greater->edge, after_prefix_key_size});
                cursor.key_buf_.push_back(greater->edge);
                DescendLeftmost(greater->child, cursor);
                return true;
            }

            cursor.stack_.resize(entry_stack_size);
            cursor.key_buf_.resize(entry_key_size);
            return false;
        }

        if (auto greater_or_equal = FindChildGe(inner, wanted); greater_or_equal.has_value()) {
            cursor.stack_.push_back(
                typename Cursor::Frame{inner, greater_or_equal->edge, after_prefix_key_size});
            cursor.key_buf_.push_back(greater_or_equal->edge);
            DescendLeftmost(greater_or_equal->child, cursor);
            return true;
        }

        cursor.stack_.resize(entry_stack_size);
        cursor.key_buf_.resize(entry_key_size);
        return false;
    }

    auto EdgeValue(bool find_min) noexcept -> ValueType* {
        Node* cur = root_;
        while (cur != nullptr && cur->Type() != NodeType::Leaf) {
            cur = find_min ? FirstChild(cur) : LastChild(cur);
        }

        return cur == nullptr ? nullptr : &static_cast<DataNode*>(cur)->value_;
    }

    auto FirstChild(Node* node) noexcept -> Node* {
        switch (node->Type()) {
        case NodeType::Node4:
            return static_cast<Node4*>(node)->size_ == 0 ? nullptr
                                                         : static_cast<Node4*>(node)->next_[0];
        case NodeType::Node16:
            return static_cast<Node16*>(node)->size_ == 0 ? nullptr
                                                          : static_cast<Node16*>(node)->next_[0];
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
        switch (node->Type()) {
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

    // ================================================
    //  Some Helper Function
    // ================================================
    static auto AsInner(Node* node) -> InnerNode* {
        CHECK(node != nullptr);
        CHECK_NE(node->Type(), NodeType::Leaf);
        return static_cast<InnerNode*>(node);
    }

    static auto AsSpan(ArtKey key) -> std::span<const byte> { return {key.Data(), key.Len()}; }

    static auto PrefixSpan(Node* node) -> std::span<const byte> {
        return {node->PrefixData(), node->PrefixLen()};
    }

    template <class Buffer>
    static auto AppendPrefix(Node* node, Buffer& key_buf) -> void {
        const auto prefix = PrefixSpan(node);
        key_buf.insert(key_buf.end(), prefix.begin(), prefix.end());
    }

    static auto CompareEncoded(std::span<const byte> lhs, std::span<const byte> rhs) -> int {
        // ART keys are binary slices, not C strings. They may contain embedded zero
        // bytes and are compared by explicit length, so strcmp-style APIs are invalid.
        const size_t n   = std::min(lhs.size(), rhs.size());
        const int    cmp = n == 0 ? 0 : std::memcmp(lhs.data(), rhs.data(), n);
        if (cmp != 0) {
            return cmp;
        }
        if (lhs.size() == rhs.size()) {
            return 0;
        }
        return lhs.size() < rhs.size() ? -1 : 1;
    }

    static auto SubtreeValueCount(const Node* node) -> uint32_t {
        CHECK(node != nullptr);
        return node->Type() == NodeType::Leaf ? 1U
                                              : static_cast<const InnerNode*>(node)->value_count_;
    }

    static auto AdjustValueCount(AncestorPath& path, int32_t delta) -> void {
        for (InnerNode* node : path) {
            CHECK(node != nullptr);
            if (delta >= 0) {
                node->value_count_ += static_cast<uint32_t>(delta);
            } else {
                const uint32_t decrement = static_cast<uint32_t>(-delta);
                CHECK_GE(node->value_count_, decrement);
                node->value_count_ -= decrement;
            }
        }
    }

    static auto FirstChildRef(InnerNode* node) -> std::optional<ChildRef> {
        return FindChildGe(node, byte{0});
    }

    static auto FindChildGe(InnerNode* node, byte key) -> std::optional<ChildRef> {
        // Each node family stores children differently, but cursor traversal wants
        // a uniform "edge byte + child pointer" abstraction.
        switch (node->Type()) {
        case NodeType::Node4: {
            auto* n4                = static_cast<Node4*>(node);
            const auto [ref, index] = n4->FindChildGte(key);
            if (ref == nullptr) {
                return std::nullopt;
            }
            return ChildRef{n4->keys_[index], *ref};
        }
        case NodeType::Node16: {
            auto* n16               = static_cast<Node16*>(node);
            const auto [ref, index] = n16->FindChildGte(key);
            if (ref == nullptr) {
                return std::nullopt;
            }
            return ChildRef{n16->keys_[index], *ref};
        }
        case NodeType::Node48: {
            const auto [ref, index] = static_cast<Node48*>(node)->FindChildGte(key);
            if (ref == nullptr) {
                return std::nullopt;
            }
            return ChildRef{static_cast<byte>(index), *ref};
        }
        case NodeType::Node256: {
            const auto [ref, index] = static_cast<Node256*>(node)->FindChildGte(key);
            if (ref == nullptr) {
                return std::nullopt;
            }
            return ChildRef{static_cast<byte>(index), *ref};
        }
        default:
            UNREACHABLE();
        }
    }

    static auto FindChildGt(InnerNode* node, byte key) -> std::optional<ChildRef> {
        if (key == byte{0xFF}) {
            return std::nullopt;
        }
        return FindChildGe(node, static_cast<byte>(key + 1));
    }

    template <class Fn>
    static auto ForEachChildInOrder(InnerNode* node, Fn&& fn) -> bool {
        switch (node->Type()) {
        case NodeType::Node4: {
            auto* n4 = static_cast<Node4*>(node);
            for (int i = 0; i < n4->size_; ++i) {
                if (!fn(n4->keys_[i], n4->next_[i])) {
                    return false;
                }
            }
            return true;
        }
        case NodeType::Node16: {
            auto* n16 = static_cast<Node16*>(node);
            for (int i = 0; i < n16->size_; ++i) {
                if (!fn(n16->keys_[i], n16->next_[i])) {
                    return false;
                }
            }
            return true;
        }
        case NodeType::Node48: {
            auto* n48 = static_cast<Node48*>(node);
            for (int i = 0; i <= 0xFF; ++i) {
                const auto slot = n48->keys_[i];
                if (slot != Node48::Nothing && !fn(static_cast<byte>(i), n48->next_[slot - 1])) {
                    return false;
                }
            }
            return true;
        }
        case NodeType::Node256: {
            auto* n256 = static_cast<Node256*>(node);
            for (int i = 0; i <= 0xFF; ++i) {
                if (n256->next_[i] != nullptr && !fn(static_cast<byte>(i), n256->next_[i])) {
                    return false;
                }
            }
            return true;
        }
        default:
            UNREACHABLE();
        }
    }

    template <class V>
    auto CreateLeaf(const byte* data, size_t len, V&& value) -> Node* {
        DataNode* leaf = NewNodeWithPerfix<DataNode>(data, len);
        leaf->value_   = std::forward<V>(value);
        return leaf;
    }

    auto SetNext(InnerNode* node, byte key, Node* next) -> InnerNode* {
        switch (node->Type()) {
        case NodeType::Node4: {
            auto* n4 = static_cast<Node4*>(node);
            if (!n4->IsFull()) {
                n4->SetNext(key, next);
                return n4;
            }

            auto* bigger = static_cast<Node16*>(n4->Grow(mr_));
            PREFETCH_W(bigger, 2);
            bigger->SetNext(key, next);
            return bigger;
        }
        case NodeType::Node16: {
            auto* n16 = static_cast<Node16*>(node);
            if (!n16->IsFull()) {
                n16->SetNext(key, next);
                return n16;
            }

            auto* bigger = static_cast<Node48*>(n16->Grow(mr_));
            PREFETCH_W(bigger, 2);
            bigger->SetNext(key, next);
            return bigger;
        }
        case NodeType::Node48: {
            auto* n48 = static_cast<Node48*>(node);
            if (!n48->IsFull()) {
                n48->SetNext(key, next);
                return n48;
            }

            auto* bigger = static_cast<Node256*>(n48->Grow(mr_));
            PREFETCH_W(bigger, 2);
            bigger->SetNext(key, next);
            return bigger;
        }
        case NodeType::Node256: {
            auto* n256 = static_cast<Node256*>(node);
            n256->SetNext(key, next);
            return n256;
        }
        default:
            UNREACHABLE();
        }
    }

    auto DelNext(InnerNode* node, byte key) -> InnerNode* {
        switch (node->Type()) {
        case NodeType::Node4: {
            auto* n4              = static_cast<Node4*>(node);
            Node* child_to_delete = n4->DelNext(key);
            DestoryArtNode(child_to_delete);
            return n4;
        }
        case NodeType::Node16: {
            auto* n16             = static_cast<Node16*>(node);
            Node* child_to_delete = n16->DelNext(key);
            DestoryArtNode(child_to_delete);
            if (!n16->UnderFull()) {
                return n16;
            }

            auto* smaller = static_cast<Node4*>(n16->Shrink(mr_));
            PREFETCH_W(smaller, 2);
            return smaller;
        }
        case NodeType::Node48: {
            auto* n48             = static_cast<Node48*>(node);
            Node* child_to_delete = n48->DelNext(key);
            DestoryArtNode(child_to_delete);
            if (!n48->UnderFull()) {
                return n48;
            }

            auto* smaller = static_cast<Node16*>(n48->Shrink(mr_));
            PREFETCH_W(smaller, 2);
            return smaller;
        }
        case NodeType::Node256: {
            auto* n256            = static_cast<Node256*>(node);
            Node* child_to_delete = n256->DelNext(key);
            DestoryArtNode(child_to_delete);
            if (!n256->UnderFull()) {
                return n256;
            }

            auto* smaller = static_cast<Node48*>(n256->Shrink(mr_));
            PREFETCH_W(smaller, 2);
            return smaller;
        }
        default:
            UNREACHABLE();
        }
    }

    static auto FindNext(InnerNode* node, byte key) -> Node** {
        switch (node->Type()) {
        case NodeType::Node4:
            return static_cast<Node4*>(node)->FindNext(key);
        case NodeType::Node16:
            return static_cast<Node16*>(node)->FindNext(key);
        case NodeType::Node48:
            return static_cast<Node48*>(node)->FindNext(key);
        case NodeType::Node256:
            return static_cast<Node256*>(node)->FindNext(key);
        default:
            UNREACHABLE();
        }
    }

    auto DestoryArtNode(Node* node) -> void {
        if (node == nullptr) {
            return;
        }

        node->ClearPrefix(mr_);
        switch (node->Type()) {
        case NodeType::Node4:
        case NodeType::Node16:
        case NodeType::Node48:
        case NodeType::Node256:
            return Node::Free(node->Type(), node, mr_);
        case NodeType::Leaf:
            return Node::FreeDataNode<DataNode>(static_cast<DataNode*>(node), mr_);
        default:
            CHECK(false) << "mismatch node type.";
            UNREACHABLE();
        }
    }

    auto DestroySubtree(Node* node) -> void {
        if (node == nullptr) {
            return;
        }

        std::vector<Node*> stack;
        stack.push_back(node);
        while (!stack.empty()) {
            Node* cur = stack.back();
            stack.pop_back();

            switch (cur->Type()) {
            case NodeType::Node4: {
                auto* n4 = static_cast<Node4*>(cur);
                for (int i = 0; i < n4->size_; ++i) {
                    stack.push_back(n4->next_[i]);
                }
                break;
            }
            case NodeType::Node16: {
                auto* n16 = static_cast<Node16*>(cur);
                for (int i = 0; i < n16->size_; ++i) {
                    stack.push_back(n16->next_[i]);
                }
                break;
            }
            case NodeType::Node48: {
                auto* n48 = static_cast<Node48*>(cur);
                for (int i = 0; i <= 0xFF; ++i) {
                    const auto slot = n48->keys_[i];
                    if (slot != Node48::Nothing) {
                        stack.push_back(n48->next_[slot - 1]);
                    }
                }
                break;
            }
            case NodeType::Node256: {
                auto* n256 = static_cast<Node256*>(cur);
                for (auto* child : n256->next_) {
                    if (child != nullptr) {
                        stack.push_back(child);
                    }
                }
                break;
            }
            case NodeType::Leaf:
                break;
            default:
                CHECK(false) << "mismatch node type.";
                UNREACHABLE();
            }

            DestoryArtNode(cur);
        }

        root_ = nullptr;
        size_ = 0;
    }

    template <class>
    friend struct ArtTestAccess;

    template <class T>
    auto NewNodeWithPerfix(const byte* data, size_t len) -> T* {
        T* node = AllocateNode<T>();
        if (len > 0) {
            node->SetPrefix(data, len, mr_);
        }
        return node;
    }

    template <class T>
    auto AllocateNode() -> T* {
        if constexpr (std::is_same_v<T, Node4>) {
            return static_cast<T*>(Node::New(NodeType::Node4, mr_));
        } else if constexpr (std::is_same_v<T, Node16>) {
            return static_cast<T*>(Node::New(NodeType::Node16, mr_));
        } else if constexpr (std::is_same_v<T, Node48>) {
            return static_cast<T*>(Node::New(NodeType::Node48, mr_));
        } else if constexpr (std::is_same_v<T, Node256>) {
            return static_cast<T*>(Node::New(NodeType::Node256, mr_));
        } else {
            return Node::NewDataNode<T>(mr_);
        }
    }

    Node*  root_{nullptr};
    size_t size_{0};
    // Bumped on every successful structural/value mutation so cursors can cheaply
    // fail fast instead of surviving node rewrites.
    uint64_t                   version_{0};
    std::pmr::memory_resource* mr_;
};

} // namespace idlekv
