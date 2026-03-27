#pragma once

#include <algorithm>
#include <concepts>
#include <cstddef>
#include <cstdint>

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
    Leaf4,
    Unknow,
};

template <typename... Ts>
struct TypeList {};

template <typename T, typename List>
struct TypeIndex;

template <typename T, typename... Ts>
struct TypeIndex<T, TypeList<T, Ts...>> {
    static constexpr size_t value = 0;
};

template <typename T, typename U, typename... Ts>
struct TypeIndex<T, TypeList<U, Ts...>> {
    static constexpr size_t value = 1 + TypeIndex<T, TypeList<Ts...>>::value;
};

constexpr size_t        kNodeTypeCount  = 5;
static constexpr size_t kMaxPrefixBytes = 8;
struct Node {
public:
    Node() = default;
    Node(NodeType type) : type_(type) {}

    // return the macthed perfix len.
    auto CheckPerfix(const byte* data) -> size_t;

    struct Prefix {
        uint8_t len_{0};
        byte    data_[kMaxPrefixBytes]{};
    };

    Prefix   prefix_;
    NodeType type_{NodeType::Unknow};
};

class InnerNode : public Node {
public:
    InnerNode() = default;
    InnerNode(NodeType type) : Node(type) {}

    uint8_t size_{0};
};

class Node4 : public InnerNode {
public:
    Node4() : InnerNode(NodeType::Node4) {}

    auto FindNext(byte key) -> Node**;
    auto SetNext(byte key, Node*) -> void;
    auto DelNext(byte key) -> Node*;
    auto IsFull() const -> bool { return size_ == 4; }
    auto UnderFull() const -> bool {
        // if size_ == 1, we should do path compression, not node shrink.
        return false;
    }

    byte  keys_[4]{};
    Node* next_[4]{};
};

class Node16 : public InnerNode {
public:
    Node16() : InnerNode(NodeType::Node16) {}

    auto FindNext(byte key) -> Node**;
    auto SetNext(byte key, Node*) -> void;
    auto DelNext(byte key) -> Node*;
    auto IsFull() const -> bool { return size_ == 16; }
    auto UnderFull() const -> bool { return size_ == 3; }

    Node* next_[16]{};
    byte  keys_[16]{};
};

class Node48 : public InnerNode {
public:
    static constexpr const size_t Nothing    = 0;
    static constexpr uint64_t     VALID_MASK = (1ULL << 48) - 1;

    Node48() : InnerNode(NodeType::Node48) {}

    auto FindNext(byte key) -> Node**;
    auto SetNext(byte key, Node*) -> void;
    auto DelNext(byte key) -> Node*;
    auto IsFull() const -> bool { return size_ == 48; }
    auto UnderFull() const -> bool { return size_ == 15; }

    byte     keys_[256]{};
    Node*    next_[48]{};
    uint64_t bitmap_{VALID_MASK}; // 1 means no next, 0 means has a next.
};

class Node256 : public InnerNode {
public:
    Node256() : InnerNode(NodeType::Node256) {}

    auto FindNext(byte key) -> Node**;
    auto SetNext(byte key, Node*) -> void;
    auto DelNext(byte key) -> Node*;
    auto IsFull() const -> bool { return false; }
    auto UnderFull() const -> bool { return size_ == 47; }

    Node* next_[256]{};
};

template <class T>
    requires std::default_initializable<T>
class NodeLeaf : public Node {
public:
    NodeLeaf() : Node(NodeType::Leaf) {}

    T value_;
};

template <class T>
    requires std::default_initializable<T>
class NodeLeaf4 : public Node {
public:
    NodeLeaf4() : Node(NodeType::Leaf) {}

    T values_[4];
};

template <class T>
    requires std::default_initializable<T>
class NodeLeaf16 : public Node {
public:
    NodeLeaf16() : Node(NodeType::Leaf) {}

    T values_[16];
};

auto NodeGrow(Node* node) -> Node*;
auto NodeShrink(Node* node) -> Node*;

} // namespace idlekv
