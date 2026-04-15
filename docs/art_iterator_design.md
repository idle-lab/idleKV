# ART 迭代器设计

**日期:** 2026-04-13
**范围:** `src/db/storage/art/`
**目标:** 为 idleKV 当前 ART 实现一版可落地的有序迭代器，用于顺序扫描、`lower_bound` 和后续 `Range()` 支持。

---

## 1. 背景

idleKV 当前的 ART 只支持：

- 精确查找：`Insert()`, `Lookup()`, `Erase()`
- 全量遍历：`Iterate(Fn&& fn)`，内部通过递归 DFS 一次性扫完整棵树
- 边界值：`Min()`, `Max()`

当前缺少一个可以暂停、恢复、逐步前进的迭代器接口，因此：

- `Range(ArtKey min, ArtKey max)` 仍未实现
- 上层只能“全树遍历后再过滤”，无法复用扫描状态
- ZSet 等依赖有序扫描的数据结构没有通用 `lower_bound` 支撑

现状可见：

- [art.h](/home/idle/code/idleKV/src/db/storage/art/art.h)
- [node.h](/home/idle/code/idleKV/src/db/storage/art/node.h)
- [node.cc](/home/idle/code/idleKV/src/db/storage/art/node.cc)

---

## 2. 设计目标

### 2.1 v1 目标

- 提供按编码后 key 字典序递增的前向迭代能力
- 提供 `Begin()` / `End()` / `LowerBound()` 三个入口
- 让 `Range(min, max)` 可以基于迭代器实现
- 不修改现有 ART 节点布局，不引入 parent pointer
- 不要求 leaf 持有完整 key

### 2.2 非目标

- 不在 v1 中提供双向迭代器 `Prev()`
- 不保证结构修改后迭代器仍然有效
- 不做并发安全迭代
- 不引入 sibling leaf chain

---

## 3. 当前实现约束

ART 当前实现对迭代器设计有几个硬约束。

### 3.1 没有 parent pointer

当前节点结构只有 prefix、node type 和 children，不保存父节点指针：

- [Node](/home/idle/code/idleKV/src/db/storage/art/node.h#L31)
- [InnerNode](/home/idle/code/idleKV/src/db/storage/art/node.h#L74)

因此迭代器不能靠“当前 leaf 直接跳父亲”前进，必须自己维护从 root 到当前 leaf 的路径栈。

### 3.2 leaf 不保存完整 key

leaf 节点只保存“剩余 suffix”作为 prefix，完整 key 是根路径上的：

- inner node prefix
- child partial key
- leaf prefix

拼出来的。

这意味着迭代器必须自己维护当前 key buffer，不能只持有 `NodeLeaf*`。

### 3.3 inner node 可能换地址

以下操作会替换节点地址：

- grow：`Node4 -> Node16 -> Node48 -> Node256`
- shrink：反方向缩容
- erase 后 path compression

相关逻辑见：

- [art.h](/home/idle/code/idleKV/src/db/storage/art/art.h#L467)
- [art.h](/home/idle/code/idleKV/src/db/storage/art/art.h#L515)

因此迭代器不能在结构修改后继续假定路径上的 `Node*` 还有效。

### 3.4 当前子节点顺序是稳定的

这一点对迭代器是有利的：

- Node4 / Node16 的 `keys_` 按升序保存
- Node48 / Node256 可以按 byte 升序扫描

见：

- [node.cc](/home/idle/code/idleKV/src/db/storage/art/node.cc#L166)
- [node.cc](/home/idle/code/idleKV/src/db/storage/art/node.cc#L236)
- [node.cc](/home/idle/code/idleKV/src/db/storage/art/node.cc#L309)
- [node.cc](/home/idle/code/idleKV/src/db/storage/art/node.cc#L369)

这保证了“按 child 升序 DFS”就是“按 key 字典序遍历”。

---

## 4. 总体方案

v1 不直接暴露完整 STL 风格迭代器，而是先落一版 `Cursor`。

原因：

- 现阶段需求核心是“有序扫描”
- `Cursor` 更容易表达失效语义
- 后续如果需要 `operator++`, `operator*`, `iterator_category`，可以在 `Cursor` 稳定后再包一层

建议 API：

```cpp
template <class ValueType>
class Art {
public:
    class Cursor;

    auto Begin() const -> Cursor;
    auto End() const -> Cursor;
    auto LowerBound(ArtKey key) const -> Cursor;

    auto Range(ArtKey min, ArtKey max) -> std::vector<RangeEntry>;

    class Cursor {
    public:
        auto Valid() const -> bool;
        auto Key() const -> std::span<const byte>;
        auto Value() const -> ValueType&;
        auto ValuePtr() const -> ValueType*;
        auto Next() -> void;
    };
};
```

`Range()` 的实现目标：

```cpp
auto Art::Range(ArtKey min, ArtKey max) -> std::vector<RangeEntry> {
    std::vector<RangeEntry> out;
    for (auto it = LowerBound(min); it.Valid() && CompareEncoded(it.Key(), max) <= 0; it.Next()) {
        out.push_back(...);
    }
    return out;
}
```

---

## 5. Cursor 内部状态

### 5.1 栈帧

迭代器需要保存从 root 到当前 leaf 的路径。

```cpp
struct Frame {
    const InnerNode* node;
    byte             edge;
    uint32_t         key_size_after_prefix;
};
```

字段含义：

- `node`: 当前路径上的 inner node
- `edge`: 从该 inner node 走向当前子树时使用的 partial key
- `key_size_after_prefix`: 当前 node 的 prefix 已追加到 `key_buf_` 后，`edge` 追加前的长度

这个长度用于回溯时把 `key_buf_` 截断回祖先节点状态，避免重复重建整条 key。

### 5.2 Cursor 成员

```cpp
const Art* owner_{nullptr};
const Node* leaf_{nullptr};
absl::InlinedVector<Frame, 16> stack_;
absl::InlinedVector<byte, 64> key_buf_;
uint64_t expected_version_{0};
```

说明：

- `owner_`: 反向指向所属 ART
- `leaf_`: 当前叶子节点；`nullptr` 表示 end
- `stack_`: 根到当前 leaf 的 inner path
- `key_buf_`: 当前 leaf 的完整编码后 key
- `expected_version_`: fail-fast 失效检测

---

## 6. 版本与失效语义

建议在 `Art` 内新增一个单调递增的 `version_`：

```cpp
uint64_t version_{0};
```

更新规则：

- 成功 `Insert()` 后 `++version_`
- 成功 `Erase()` 后 `++version_`
- 成功 `Upsert` 更新 value 后 `++version_`

`Cursor` 构造时记录 `expected_version_ = owner_->version_`。
在 `Next()`, `Key()`, `Value()` 中检查：

```cpp
CHECK_EQ(expected_version_, owner_->version_) << "ART cursor invalidated by mutation";
```

v1 采用“结构修改后全部失效”的保守策略。

这样做的原因：

- grow/shrink 会替换 node 地址
- path compression 会改 prefix 布局
- 不需要在 v1 里维护复杂的局部稳定性规则

---

## 7. 需要补充的内部辅助方法

### 7.1 child 枚举 helper

建议统一出 4 组 helper，供 `Begin()`, `LowerBound()`, `Next()` 复用：

```cpp
struct ChildRef {
    byte  edge;
    Node* child;
};

static auto FirstChildRef(const InnerNode* node) -> std::optional<ChildRef>;
static auto LastChildRef(const InnerNode* node) -> std::optional<ChildRef>;
static auto FindChildGe(const InnerNode* node, byte edge) -> std::optional<ChildRef>;
static auto FindChildGt(const InnerNode* node, byte edge) -> std::optional<ChildRef>;
```

当前已有 `FindChildGte(byte)`，但返回值不统一，且不适合直接给迭代器使用：

- [node.h](/home/idle/code/idleKV/src/db/storage/art/node.h#L93)
- [node.cc](/home/idle/code/idleKV/src/db/storage/art/node.cc#L157)
- [node.cc](/home/idle/code/idleKV/src/db/storage/art/node.cc#L227)
- [node.cc](/home/idle/code/idleKV/src/db/storage/art/node.cc#L299)
- [node.cc](/home/idle/code/idleKV/src/db/storage/art/node.cc#L360)

建议在 `Art` 内部统一包装，避免把节点内部数组布局暴露给 `Cursor`。

### 7.2 编码后 key 比较 helper

`LowerBound()` 需要对“压缩 prefix”和“目标 key 剩余部分”做三路比较。

建议补一个 helper：

```cpp
enum class PrefixCompare {
    Less,
    EqualPrefix,
    Greater,
};

static auto ComparePrefix(std::span<const byte> prefix, std::span<const byte> key) -> PrefixCompare;
```

它和当前 `CheckPerfix()` 不同：

- `CheckPerfix()` 只返回匹配长度
- `LowerBound()` 需要知道“在第一个不相等字节处，node prefix 更小还是更大”

---

## 8. 关键算法

### 8.1 DescendLeftmost

从任意子树根开始，下降到该子树中的最小 leaf。

流程：

1. 若当前是 leaf，追加 leaf prefix，结束
2. 追加当前 inner node 的 prefix
3. 选择最小 child
4. 在 `stack_` 里压入一帧
5. 追加 child edge
6. 继续向下

伪代码：

```cpp
auto DescendLeftmost(Node* cur) -> void {
    while (cur->Type() != NodeType::Leaf) {
        auto* inner = static_cast<InnerNode*>(cur);
        key_buf_.insert(key_buf_.end(), inner->PrefixData(), inner->PrefixData() + inner->PrefixLen());

        auto child = FirstChildRef(inner);
        CHECK(child.has_value());

        stack_.push_back(Frame{
            .node = inner,
            .edge = child->edge,
            .key_size_after_prefix = static_cast<uint32_t>(key_buf_.size()),
        });

        key_buf_.push_back(child->edge);
        cur = child->child;
    }

    leaf_ = cur;
    key_buf_.insert(key_buf_.end(), cur->PrefixData(), cur->PrefixData() + cur->PrefixLen());
}
```

### 8.2 Begin

`Begin()` 逻辑：

- 空树直接返回 `End()`
- 否则清空 `stack_` / `key_buf_`
- 从 `root_` 调 `DescendLeftmost(root_)`

### 8.3 Next

`Next()` 是整个设计的核心。

思路：

1. 当前 leaf 消费完后，需要找“第一个还存在未访问 sibling 的祖先”
2. 找到后切到那个 sibling
3. 再从该 sibling 一路向左下到最小 leaf

流程：

1. 检查 `version_`
2. 若已经是 end，直接返回
3. 丢弃当前 leaf suffix：`key_buf_` 回退到最后一个 frame 的 edge 之后
4. 从 `stack_` 顶开始回溯：
   - 把 `key_buf_` 截断到 `frame.key_size_after_prefix`
   - 调 `FindChildGt(frame.node, frame.edge)`
   - 若找到 sibling：
     - 更新该 frame 的 `edge`
     - 追加新 edge
     - 清掉 frame 之后的栈帧
     - `DescendLeftmost(sibling.child)`
     - 返回
   - 若没找到：
     - 继续向上弹栈
5. 回溯到根仍找不到，置 `leaf_ = nullptr`，表示 end

这个过程是摊还 `O(depth)`。

### 8.4 LowerBound

`LowerBound(target)` 比 `Begin()` 复杂，原因是压缩 prefix 会造成三种情况：

1. `node prefix < target suffix`
2. `node prefix == target suffix` 的公共前缀
3. `node prefix > target suffix`

核心规则：

- 若某个子树根的 prefix 已经严格大于 target 剩余部分，则结果一定在该子树的最左 leaf
- 若某个子树根的 prefix 已经严格小于 target 剩余部分，则该子树整体可以跳过，应该去祖先找下一个 sibling
- 若 prefix 完全相等，则继续按目标 key 往下走

推荐实现为“边下降边记录候选祖先”：

```cpp
auto LowerBound(ArtKey target) const -> Cursor;
```

需要一个临时候选：

```cpp
struct PendingSuccessor {
    absl::InlinedVector<Frame, 16> stack;
    absl::InlinedVector<byte, 64>  key_buf;
    Node*                          subtree_root;
};
```

当路径上遇到“当前 edge 命中，但本层还有更大的 sibling”时，把那个“更大 sibling 子树”的最左候选记下来。
如果之后精确路径走不通，就回退到最近的候选 successor。

这样能覆盖：

- 精确命中
- 命中失败但有后继
- key 落在两个 sibling 中间
- key 比整棵树最大值还大
- key 在压缩 prefix 中间结束

### 8.5 End

`End()` 只需要满足：

- `leaf_ == nullptr`
- `stack_` 和 `key_buf_` 为空

---

## 9. Range 实现方式

`Range(min, max)` 不再单独 DFS。

建议直接基于 `LowerBound(min)`：

```cpp
auto Range(ArtKey min, ArtKey max) -> std::vector<RangeEntry> {
    std::vector<RangeEntry> out;
    for (auto it = LowerBound(min); it.Valid(); it.Next()) {
        if (CompareEncoded(std::span<const byte>(it.Key().data(), it.Key().size()),
                           std::span<const byte>(max.Data(), max.Len())) > 0) {
            break;
        }

        out.push_back(RangeEntry{
            .key = std::vector<byte>(it.Key().begin(), it.Key().end()),
            .value = it.ValuePtr(),
        });
    }
    return out;
}
```

优点：

- 逻辑统一
- `Range()` 和未来 scan 类接口复用同一个 cursor
- 不再复制一套范围遍历逻辑

---

## 10. 复杂度

### 10.1 时间复杂度

- `Begin()`: `O(depth)`
- `Next()`: 摊还 `O(depth)`
- `LowerBound()`: `O(depth * child_scan)`

其中：

- Node4: child scan 最多 4
- Node16: 最多 16
- Node48: 最多 256
- Node256: 最多 256

`Node48` / `Node256` 的扫描常数偏大，但这是当前节点布局的既有约束，不是迭代器独有问题。

### 10.2 空间复杂度

- `stack_`: `O(depth)`
- `key_buf_`: `O(key_length)`

建议使用 `absl::InlinedVector`，减少短 key、浅树下的堆分配。

---

## 11. 为什么 v1 不做双向迭代器

理论上可以做 `Prev()`，但需要补完整的反向 sibling 查找：

- `FindChildLt()`
- `DescendRightmost()`
- `UpperBound()` 或 `LessThan()`

这会把当前实现复杂度几乎翻倍，而目前最直接的需求是：

- `Range(min, max)`
- ZSet / range scan

所以 v1 只做 forward cursor 更合理。

如果后续确实需要 `reverse scan`，再在此基础上扩展。

---

## 12. 测试计划

建议新增以下测试。

### 12.1 Begin / Next

- 空树 `Begin() == End()`
- 单元素树只迭代一次
- 多元素按字典序递增返回
- 覆盖 Node4 / Node16 / Node48 / Node256 四种节点形态

### 12.2 LowerBound

- exact match
- 落在两个 key 之间
- 小于最小 key
- 大于最大 key
- key 是已有 key 的前缀
- key 比某压缩 prefix 更短
- key 在压缩 prefix 中间发生大小分叉

### 12.3 Prefix / path compression

- 长前缀 key
- erase 后 path compression 再迭代
- grow / shrink 后重新获取 cursor

### 12.4 Invalidation

- 获取 cursor 后执行 `Insert()`
- 获取 cursor 后执行 `Erase()`
- 确认 `Next()` / `Key()` / `Value()` fail-fast

---

## 13. 实施顺序

推荐按以下顺序落地。

1. 在 `Art` 内加 `version_` 和 `Cursor` 骨架
2. 抽统一 child helper：`FirstChildRef`, `FindChildGe`, `FindChildGt`
3. 实现 `DescendLeftmost()` 和 `Begin()`
4. 实现 `Next()`
5. 实现 `LowerBound()`
6. 用 cursor 重写 `Range()`
7. 视情况把现有 `Iterate(Fn&&)` 改成基于 cursor 的适配层

`Iterate(Fn&&)` 最终可以保留成兼容 API：

```cpp
template <class Fn>
auto Iterate(Fn&& fn) -> void {
    for (auto it = Begin(); it.Valid(); it.Next()) {
        if (!fn(it.Key(), it.Value())) {
            break;
        }
    }
}
```

---

## 14. 小结

这版设计的核心判断是：

- 不改节点布局
- 不追求修改后迭代器稳定
- 先做一版 fail-fast 的 forward cursor

这样可以用最小代价补齐 ART 的有序扫描能力，并直接解锁：

- `LowerBound()`
- `Range()`
- 上层基于范围的有序访问

如果这版方向确认无误，下一步就可以直接在 [art.h](/home/idle/code/idleKV/src/db/storage/art/art.h) 里实现 `Cursor`，并补对应单测。
