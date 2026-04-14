#pragma once

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <memory_resource>
#include <utility>

#if defined(__i386__) || defined(__amd64__)
#include <emmintrin.h>
#endif

namespace idlekv {

using byte = unsigned char;

enum struct NodeType : uint8_t {
    Node4,
    Node16,
    Node48,
    Node256,
    Leaf,
    Unknow,
};

template <class T>
    requires std::default_initializable<T>
class NodeLeaf;

// Most prefixes in our workloads are short. Keeping up to one pointer-sized prefix
// inline avoids a second allocation on the common path while still allowing longer
// compressed prefixes to spill to heap storage.
static constexpr size_t kInlinePrefixBytes = sizeof(char*);
struct Node {
public:
    // ART path compression stores the unmatched suffix directly on each node.
    // The prefix bytes are inline when short and heap-backed otherwise.
    union Prefix {
        byte* heap_ __attribute__((packed)){nullptr};
        byte  inline_[kInlinePrefixBytes];
    };

    template <class DataNode>
    static DataNode* NewDataNode(std::pmr::memory_resource* mr);
    static Node*     New(NodeType type, std::pmr::memory_resource* mr);

    template <class DataNode>
    static void FreeDataNode(DataNode* node, std::pmr::memory_resource* mr);
    static void Free(NodeType type, Node* node, std::pmr::memory_resource* mr);

    Node(const Node& other)                        = delete;
    Node(Node&& other) noexcept                    = delete;
    auto operator=(const Node& other) -> Node&     = delete;
    auto operator=(Node&& other) noexcept -> Node& = delete;

    // return the macthed perfix len.
    auto CheckPerfix(const byte* data, size_t len) -> size_t;
    auto PrefixData() -> byte*;
    auto PrefixData() const -> const byte*;
    auto PrefixLen() const -> size_t { return prefix_len_; }
    auto SetPrefix(const byte* data, size_t len, std::pmr::memory_resource* mr) -> void;
    auto SetNewPrefix(Prefix prefix, size_t len, std::pmr::memory_resource* mr) -> void;
    auto ClearPrefix(std::pmr::memory_resource* mr) -> void;
    auto MovePrefixFrom(Node& other, std::pmr::memory_resource* mr) -> void;
    auto Type() const -> NodeType { return type_; }

protected:
    explicit Node(NodeType type) : type_(type) {}

private:
    auto UsesHeapPrefix() const -> bool { return prefix_len_ > kInlinePrefixBytes; }

    Prefix prefix_;

    // I have considered that a member length of 65535 is unlikely to occur in real-world scenarios,
    // so I have opted for using uint16 here.
    // TODO(cyb):In the future, to support prefixes of longer lengths, chained prefixes can be
    // implemented
    uint16_t prefix_len_{0};

    NodeType type_{NodeType::Unknow};
};

class InnerNode : public Node {
public:
    InnerNode(NodeType type) : Node(type) {}

protected:
    auto MoveHeaderTo(InnerNode& dest, std::pmr::memory_resource* mr) -> void {
        dest.MovePrefixFrom(*this, mr);
        dest.size_        = size_;
        dest.value_count_ = value_count_;
    }

public:
    // All inner node variants keep the number of live children so growth/shrink
    // thresholds and iterator traversal can be handled generically.
    uint8_t size_{0};

    uint32_t value_count_{0};
};

class Node4 : public InnerNode {
public:
    Node4() : InnerNode(NodeType::Node4) {}

    auto FindNext(byte key) -> Node**;
    auto FindChildGte(byte key) -> std::pair<Node**, int>;
    auto SetNext(byte key, Node*) -> void;
    auto DelNext(byte key) -> Node*;
    auto IsFull() const -> bool { return size_ == 4; }
    auto UnderFull() const -> bool {
        // if size_ == 1, we should do path compression, not node shrink.
        return false;
    }
    auto Grow(std::pmr::memory_resource* mr) -> InnerNode*;
    auto Shrink(std::pmr::memory_resource* mr) -> InnerNode*;

    // Node4/16 store children densely and keep keys sorted so a left-to-right scan
    // is already ART lexicographic order.
    byte  keys_[4]{};
    Node* next_[4]{};
};

class Node16 : public InnerNode {
public:
    Node16() : InnerNode(NodeType::Node16) {}

    auto FindNext(byte key) -> Node**;
    auto FindChildGte(byte key) -> std::pair<Node**, int>;
    auto SetNext(byte key, Node*) -> void;
    auto DelNext(byte key) -> Node*;
    auto IsFull() const -> bool { return size_ == 16; }
    auto UnderFull() const -> bool { return size_ == 3; }
    auto Grow(std::pmr::memory_resource* mr) -> InnerNode*;
    auto Shrink(std::pmr::memory_resource* mr) -> InnerNode*;

    // Node16 keeps the same sorted-dense representation as Node4, but FindNext can
    // use SIMD on x86 for exact-match probes.
    Node* next_[16]{};
    byte  keys_[16]{};
};

class Node48 : public InnerNode {
public:
    static constexpr const size_t Nothing    = 0;
    static constexpr uint64_t     VALID_MASK = (1ULL << 48) - 1;

    Node48() : InnerNode(NodeType::Node48) {}

    auto FindNext(byte key) -> Node**;
    auto FindChildGte(byte key) -> std::pair<Node**, int>;
    auto SetNext(byte key, Node*) -> void;
    auto DelNext(byte key) -> Node*;
    auto IsFull() const -> bool { return size_ == 48; }
    auto UnderFull() const -> bool { return size_ == 15; }
    auto Grow(std::pmr::memory_resource* mr) -> InnerNode*;
    auto Shrink(std::pmr::memory_resource* mr) -> InnerNode*;

    // Node48 trades a 256-entry key-to-slot index for a dense 48-entry child array.
    // This keeps lexicographic traversal possible without paying a full 256 pointers.
    byte     keys_[256]{};
    Node*    next_[48]{};
    uint64_t bitmap_{VALID_MASK}; // 1 means no next, 0 means has a next.
};

class Node256 : public InnerNode {
public:
    Node256() : InnerNode(NodeType::Node256) {}

    auto FindNext(byte key) -> Node**;
    auto FindChildGte(byte key) -> std::pair<Node**, int>;
    auto SetNext(byte key, Node*) -> void;
    auto DelNext(byte key) -> Node*;
    auto IsFull() const -> bool { return false; }
    auto UnderFull() const -> bool { return size_ == 47; }
    auto Grow(std::pmr::memory_resource* mr) -> InnerNode*;
    auto Shrink(std::pmr::memory_resource* mr) -> InnerNode*;

    // Node256 is the terminal growth state: direct byte->child addressing with no
    // further promotion, at the cost of a full 256-pointer array.
    Node* next_[256]{};
};

template <class T>
    requires std::default_initializable<T>
class NodeLeaf : public Node {
public:
    NodeLeaf() : Node(NodeType::Leaf) {}

    // Leaves only store the remaining suffix and the value. The full key is rebuilt
    // from parent prefixes/edges by traversal code such as the cursor.
    T value_;
};

template <class DataNode>
DataNode* Node::NewDataNode(std::pmr::memory_resource* mr) {
    return new (mr->allocate(sizeof(DataNode), alignof(DataNode))) DataNode();
}

template <class DataNode>
void Node::FreeDataNode(DataNode* node, std::pmr::memory_resource* mr) {
    node->~DataNode();
    mr->deallocate(node, sizeof(DataNode), alignof(DataNode));
}

} // namespace idlekv
