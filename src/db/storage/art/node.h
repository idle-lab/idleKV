#pragma once

#include <concepts>
#include <cstddef>
#include <cstdint>

namespace idlekv {

using byte = unsigned char;

enum struct NodeType : uint8_t {
    Unknow,
    Node4,
    Node16,
    Node48,
    Node256,
    Leaf,
};

struct Node {
public:
    Node() = default;
    Node(NodeType type) : type_(type) {}

    NodeType type_{NodeType::Unknow};
    byte* prefix_{nullptr};
    uint32_t prefix_len_{0};
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

    auto FindNext(byte key) -> Node*;
    auto SetNext(byte key, Node*) -> void;
    auto DelNext(byte key) -> Node*;
    auto IsFull() -> bool { return size_ == 4; }

    byte keys_[4]{};
    Node* next_[4]{};
};

class Node16 : public InnerNode {
public:
    Node16() : InnerNode(NodeType::Node16) {}

    auto FindNext(byte key) -> Node*;
    auto SetNext(byte key, Node*) -> void;
    auto DelNext(byte key) -> Node*;
    auto IsFull() -> bool { return size_ == 16; }

    byte keys_[16]{};
    Node* next_[16]{};
};

class Node48 : public InnerNode {
public:
    static constexpr const size_t Nothing = 0;

    Node48() : InnerNode(NodeType::Node48) {}

    auto FindNext(byte key) -> Node*;
    auto SetNext(byte key, Node*) -> void;
    auto DelNext(byte key) -> Node*;
    auto IsFull() -> bool { return size_ == 48; }

    byte keys_[256]{};
    Node* next_[48]{};
};

class Node256 : public InnerNode {
public:
    Node256() : InnerNode(NodeType::Node256) {}

    auto FindNext(byte key) -> Node*;
    auto SetNext(byte key, Node*) -> void;
    auto DelNext(byte key) -> Node*;
    auto IsFull() -> bool { return false; }

    Node* next_[256]{};
};

template<class T>
requires std::default_initializable<T>
class NodeLeaf : public Node {
public:
    NodeLeaf() : Node(NodeType::Leaf) {}

    T value_;
};

auto NodeGrow(Node* node) -> Node* ;
auto NodeShrink(Node* node) -> Node*;


} // namespace idlekv
