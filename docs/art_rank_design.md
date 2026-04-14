# ART Rank / ToRank 设计

**日期:** 2026-04-14  
**范围:** `src/db/storage/art/`  
**目标:** 为 ART 增加按 rank 定位能力，使 ZSet 的 `ZRANGE start stop` 可以像 Dragonfly 的 `ToRank() + Next()` 一样，从 `start` 直接下沉到目标位置，而不是从头线性跳过。

---

## 1. 问题背景

当前 ART 已经具备：

- 精确查找：`Lookup()`
- 按 key 有序扫描：`Begin()`, `LowerBound()`, `Cursor::Next()`
- 全量遍历：`Iterate(Fn&& fn)`

但对 ZSet 来说，`ZRANGE start stop` 的核心访问模式不是“按 key lower_bound”，而是“按顺序的第 N 个元素开始扫描”。

如果只有前向迭代器，而没有 rank 定位，那么：

- 从 `start = 100000` 开始扫描时，需要先做 100000 次 `Next()`
- 时间复杂度是 `O(start + count)`
- 当 `start` 很大而 `count` 很小时，前缀跳过成本完全浪费

Dragonfly 的思路是：

- B+Tree 节点维护子树元素数
- `ToRank(rank)` 通过子树计数直接下沉到第 `rank` 个元素
- 后续用 `Next()` 顺序扫 `[rank_start, rank_end]`

ART 也可以做同样的事情，只是需要重新定义节点计数语义，并重做 `Insert` / `Erase` 的内部接口，保证路径上的 `value_count_` 始终正确。

---

## 2. 设计目标

### 2.1 目标

- 支持 `ToRank(rank)` / `SeekByRank(rank)`，复杂度为 `O(height + returned_count)`
- 支持 `IterateByRank(start, end, cb)`
- 保持当前 key 的字典序定义不变
- 不引入 parent pointer
- 不要求 leaf 保存完整 key
- 尽量保持对外 `Insert()` / `Erase()` 语义不变，只重做内部 mutation 接口

### 2.2 非目标

- 不在这一版实现 `Prev()` 或双向迭代器
- 不实现并发安全迭代
- 不保证 mutation 后旧 cursor 继续有效
- 不尝试把 Node48 / Node256 的按 rank 选子节点做到位图级极致优化，第一版接受每层最多扫描 256 个 child

---

## 3. 节点计数语义

### 3.1 基本定义

在 `InnerNode` 中新增：

```cpp
uint32_t value_count_{0};
```

语义定义为：

- `Leaf` 的子树元素数隐含为 `1`
- `InnerNode::value_count_` 表示“该 inner node 整棵子树中的 leaf 数量”

统一 helper：

```cpp
static auto SubtreeValueCount(const Node* node) -> uint32_t {
    return node->Type() == NodeType::Leaf ? 1 : static_cast<const InnerNode*>(node)->value_count_;
}
```

### 3.2 必须满足的不变量

对任意 inner node：

```cpp
node->value_count_ == Sum(SubtreeValueCount(child_i))
```

并且：

- `size_` 只表示 child 个数
- `value_count_` 表示整棵子树的 value 个数
- 两者不能混用

### 3.3 Root 语义

- `root_ == nullptr` 时，树为空
- `root_` 是 leaf 时，整棵树大小为 `1`
- `root_` 是 inner node 时，必须满足：

```cpp
Size() == static_cast<InnerNode*>(root_)->value_count_
```

---

## 4. 为什么要重做 Insert / Erase 接口

当前内部接口大致是：

```cpp
template <class V>
auto InsertInternal(Node* cur, Node** cur_ref, ArtKey& key, V&& value, InsertMode mode)
    -> InsertResutl;

auto EraseInternal(Node* cur, Node** cur_ref, ArtKey& key) -> size_t;
```

这套接口的问题是：

1. `ArtKey&` 会在函数里被 `Cut()` 原地修改，调用方很难复用上下文
2. 接口只返回“插入/删除是否成功”，没有明确表达“是否新增/删除了一个 leaf”
3. 为了维护 `value_count_`，需要知道从 root 到 mutation 点的整条 ancestor path
4. grow / shrink / path compression 可能替换节点地址，计数更新时机如果不清楚，很容易更新到错误的对象

rank 功能本质上要求：

- 每次新增一个 leaf，都要对祖先链 `+1`
- 每次删除一个 leaf，都要对祖先链 `-1`
- prefix split 新创建的 inner node，要初始化正确的 `value_count_`
- grow / shrink 后 replacement node 不能丢失计数

所以 mutation 接口必须显式围绕“路径”和“leaf 数量变化”设计，而不能只围绕“是否找到 key”设计。

---

## 5. 对外 API 与内部 API 的边界

### 5.1 对外 API 保持不变

对调用方来说，不需要因为 rank 功能改动现有语义：

```cpp
template <class V>
auto Insert(ArtKey key, V&& value, InsertMode mode = InsertMode::InsertOnly) noexcept
    -> InsertResutl;

auto Erase(ArtKey key) noexcept -> size_t;
```

这样 ZSet 和其它上层逻辑不需要跟着改。

### 5.2 新增对外 rank API

新增：

```cpp
auto SeekByRank(uint32_t rank) -> Cursor;

template <class Fn>
auto IterateByRank(uint32_t rank_start, uint32_t rank_end, Fn&& fn) -> bool;
```

语义：

- `rank` 为 0-based
- `SeekByRank(rank)` 在 `rank >= Size()` 时返回 `End()`
- `IterateByRank(start, end, cb)` 是闭区间 `[start, end]`

建议 `IterateByRank()` 直接做成 Dragonfly 风格：

```cpp
template <class Fn>
auto IterateByRank(uint32_t rank_start, uint32_t rank_end, Fn&& fn) -> bool {
    if (rank_start >= Size()) {
        return true;
    }

    rank_end = std::min<uint32_t>(rank_end, Size() - 1);
    auto it = SeekByRank(rank_start);
    for (uint32_t i = rank_start; i <= rank_end && it.Valid(); ++i, it.Next()) {
        if (!fn(it)) {
            return false;
        }
    }
    return true;
}
```

---

## 6. 新的内部 mutation 接口

### 6.1 设计原则

新的内部接口要解决三件事：

- 清楚表达“这次 mutation 是否让 leaf 数量变化”
- 在 mutation 过程中维护一条可回填的 ancestor path
- 在 grow / shrink / path compression / prefix split 后，path 仍然只指向最终需要调 `value_count_` 的节点

### 6.2 路径结构

第一版推荐由外层创建并传入一个 ancestor path：

```cpp
using AncestorPath = absl::InlinedVector<InnerNode*, 16>;
```

路径按 root -> leaf 方向保存，并且它的语义不是“访问过的所有 inner node”，而是：

- 在本次操作结束后
- 仍然存活于树中
- 并且需要在外层统一做 `value_count_ += delta` 的那些 inner node

因此 `AncestorPath` 在内部并不是只追加不修改。发生结构替换时，内部必须同步修正它。

关键约束：

- 只有在 `cur` 的 prefix 已经完全匹配时，才能把 `cur` 放进 path
- 如果 mismatch 发生在 `cur` 的 prefix 内部，那么 `cur` 不能进 path
  因为新增 key 不在 `cur` 旧子树中，而是在 `cur` 上方新建一个 split node
- 如果后续发生 grow / shrink / path compression / prefix split，需要同步替换、删除或追加 path 的末尾元素

### 6.3 返回值

这里不需要新造一层 `Outcome`。

当前结果类型已经足够表达“leaf 是否真的增删”：

- `InsertInternal()` 继续返回 `InsertResutl`
- `EraseInternal()` 继续返回 `size_t`

判定规则：

- 新增 leaf：`result.status == InsertResutl::OK && result.value == nullptr`
- `IfExistGetValue` 命中旧值：`result.status == InsertResutl::OK && result.value != nullptr`
- `Upsert`：`result.status == InsertResutl::UpsertValue`
- `DuplicateKey`：没有新增 leaf
- `EraseInternal()` 返回 `1` 表示删除了 leaf，返回 `0` 表示 miss

如果后面觉得 `InsertResutl::OK + value==nullptr` 这个约定不够直观，可以只给
`InsertResutl` 增加一个轻量 helper，例如 `InsertedNewValue()`，但没有必要再包一层结构体。

### 6.4 新接口草案

```cpp
template <class V>
auto InsertInternal(Node** root_ref, ArtKey key, V&& value, InsertMode mode,
                    AncestorPath& path) noexcept
    -> InsertResutl;

auto EraseInternal(Node** root_ref, ArtKey key, AncestorPath& path) noexcept -> size_t;
```

与现状相比，变化点是：

- 只传 `Node** root_ref`，不把 `cur` / `cur_ref` 暴露成接口的一部分
- `ArtKey` 按值传递，内部可以安全 `Cut()`
- path 由外层创建、内部维护
- 返回类型保持原有语义，只调整内部控制流

这样顶层 wrapper 会变得很清楚：

```cpp
template <class V>
auto Insert(ArtKey key, V&& value, InsertMode mode) -> InsertResutl {
    AncestorPath path;
    auto result = InsertInternal(&root_, key, std::forward<V>(value), mode, path);
    if (result.status == InsertResutl::OK && result.value == nullptr) {
        AdjustValueCount(path, +1);
        ++size_;
    }
    if ((result.status == InsertResutl::OK && result.value == nullptr) ||
        result.status == InsertResutl::UpsertValue) {
        ++version_;
    }
    return result;
}

auto Erase(ArtKey key) -> size_t {
    AncestorPath path;
    size_t erased = EraseInternal(&root_, key, path);
    if (erased != 0) {
        AdjustValueCount(path, -1);
        --size_;
        ++version_;
    }
    return erased;
}
```

---

## 7. 计数更新时机

### 7.1 核心原则

这一版采用“结构先变更，计数后回填”的策略：

- `InsertInternal()` / `EraseInternal()` 只负责完成结构修改，并修正 `AncestorPath`
- 外层 `Insert()` / `Erase()` 根据最终返回值决定是否对 path 做一次统一的 `AdjustValueCount(path, delta)`

优点是：

- `size_` / `version_` / `value_count_` 都在外层统一收口
- 内部 mutation 分支只需要关心“最终哪些节点应该被调计数”
- 对 insert / erase 来说，统一回填点都在 wrapper，入口更一致

代价是：

- 在 `InsertInternal()` / `EraseInternal()` 执行期间，`value_count_` 可能暂时滞后于真实结构
- 因此内部控制流不能依赖“当前节点的 `value_count_` 已经和结构同步”
- 为了让外层回填安全，内部必须同步修正 `AncestorPath`

统一 helper：

```cpp
static auto AdjustValueCount(AncestorPath& path, int32_t delta) -> void {
    for (InnerNode* node : path) {
        CHECK_NE(node, nullptr);
        node->value_count_ += delta;
    }
}
```

### 7.2 AncestorPath 的修正规则

如果只是简单把“访问过的祖先”塞进 path，再等外层统一调整，代码会在结构替换后打到失效节点。

因此内部必须维护如下规则：

- grow：如果最后一个命中的 inner node 被更大的节点替换，用 replacement node 覆盖 `path.back()`
- shrink：如果最后一个 parent 被更小的节点替换，用 replacement node 覆盖 `path.back()`
- path compression：如果最后一个 parent 被直接删掉，`path.pop_back()`
- prefix split：新建的 split node 也需要吃到本次 `+1`，因此应追加到 path 末尾

这样外层统一 `AdjustValueCount()` 时，path 中剩下的就是“最终仍然活着、且确实需要加减一”的节点集合。

### 7.3 这一时机下的实现约束

采用延迟回填后，要遵守两个约束：

1. 内部 mutation 逻辑不能依赖“`value_count_` 已经和当前结构同步”
2. 所有会替换或删除节点地址的分支，都必须同步修正 `AncestorPath`

---

## 8. 节点迁移时的计数规则

### 8.1 Grow / Shrink

`Node4/16/48/256` 的 grow / shrink 都会调用 `MoveHeaderTo()`，因此它必须扩展为同时搬运 `value_count_`：

```cpp
auto MoveHeaderTo(InnerNode& dest, std::pmr::memory_resource* mr) -> void {
    dest.MovePrefixFrom(*this, mr);
    dest.size_ = size_;
    dest.value_count_ = value_count_;
}
```

否则：

- promotion 之后 replacement node 会丢失 subtree count
- `ToRank()` 会立刻出错

在“外层统一回填”的方案里，这里搬运的是旧计数，而不是已经调整过的新计数。
如果 grow / shrink 替换了 path 末尾节点，内部还要同步把 `path.back()` 指向 replacement node。

### 8.2 Prefix Split 生成的新 inner node

当 insert 在某个节点 `cur` 的 compressed prefix 内部发生 mismatch 时，会在 `cur` 上方新建一个 `Node4`。

此时：

- 旧 child `cur` 的 subtree count 是 `SubtreeValueCount(cur)`
- 新插入 leaf 的 subtree count 是 `1`

为了让 split node 也能参与外层统一 `+1`，它的初始计数应先设置为“旧子树计数”：

```cpp
split->value_count_ = SubtreeValueCount(cur);
```

注意：

- 这里不直接写 `+1`
- split node 创建后应追加到 path 末尾
- 外层统一 `AdjustValueCount(path, +1)` 后，split 的最终计数才会变成 `SubtreeValueCount(cur) + 1`

### 8.3 Path Compression

erase 后如果某个 `Node4` 只剩一个 child，会被压缩掉：

```cpp
parent(prefix) + edge + child(prefix)
```

会合并进 child 的 prefix。

这里不需要把 parent 的 `value_count_` 复制给 child，原因是：

- child 的 subtree 没变
- path compression 之后 parent 已经不存在，不应再出现在最终 path 中
- 外层统一 `AdjustValueCount(path, -1)` 时，只需要更新仍然存活的祖先

因此 path compression 只负责 prefix 合并，并在必要时 `path.pop_back()`，不负责迁移 subtree count。

---

## 9. Insert 详细流程

下面只讨论和 rank/count 相关的部分。

### 9.1 空树插入

如果 `root_ == nullptr`：

- 直接创建 leaf
- 不存在 inner node，因此不用维护 `value_count_`

### 9.2 在 prefix 内部发生 split

场景：

- `cur->CheckPrefix(...) < cur->PrefixLen()`

行为：

1. 创建新的 `Node4 split`
2. `split` 的 prefix 为公共前缀
3. `split` 的两个 child：
   - 指向旧子树 `cur`
   - 指向新 leaf
4. 初始化：

```cpp
split->value_count_ = SubtreeValueCount(cur);
```

5. 用 `split` 替换 `*cur_ref`
6. 把 `split` 追加到 path 末尾
7. 返回后由外层统一对 path 做 `+1`

说明：

- `cur` 本身不在 path 中，因为 mismatch 发生在 `cur` 前缀内部
- path 中最终包含“旧祖先 + 新 split”

### 9.3 prefix 完全匹配，但 child 不存在

场景：

- 当前 `cur` 是 inner node
- `FindNext(inner, edge) == nullptr`

行为：

1. 把 `inner` 放进 path
2. 确认这是一次“新增 leaf”
3. `SetNext()` 挂新 leaf
4. 如果触发 grow，用 replacement node 覆盖 `*cur_ref`
5. 如果发生 grow，同步用 replacement node 覆盖 `path.back()`
6. 返回后由外层统一对 path 做 `+1`

说明：

- 直到外层统一回填前，`inner/replacement` 上的 `value_count_` 都还是旧值

### 9.4 exact match

场景：

- 找到了 leaf，且 key 完全相等

行为：

- `InsertOnly`：返回 duplicate，不改计数
- `IfExistGetValue`：返回 value 指针，不改计数
- `Upsert`：只更新 value，不改计数

---

## 10. Erase 详细流程

### 10.1 miss

以下情况都不改计数：

- prefix mismatch
- exact match 但 `cur` 不是 leaf
- 目标 child 不存在

### 10.2 删除唯一 root leaf

如果整棵树只有一个 root leaf：

- 直接销毁 leaf
- `root_ = nullptr`
- 没有 inner node，因此不需要维护 `value_count_`

### 10.3 删除普通 leaf

场景：

- 命中 leaf，且存在 parent inner node

行为：

1. path 中已经包含从 root 到 parent 的所有 inner node
2. `DelNext(parent, edge)`
3. 如果 parent underfull，则 shrink
4. 如果发生 shrink，同步用 replacement node 覆盖 `path.back()`
5. 如果 parent 最终只剩一个 child，则 path compression
6. 如果发生 path compression，则 `path.pop_back()`
7. 返回后由外层统一对 path 做 `-1`

### 10.4 shrink

如果删除后触发：

- `Node16 -> Node4`
- `Node48 -> Node16`
- `Node256 -> Node48`

replacement node 通过 `MoveHeaderTo()` 继承的是删除前的旧计数，真正的 `-1` 由外层统一回填。

### 10.5 path compression

如果删除后 parent 只剩一个 child：

- 把 `parent.prefix + edge + child.prefix` 合并到 child
- 用 child 替换 `parent`
- 不需要修改 child 的 subtree count

---

## 11. ToRank / SeekByRank 设计

### 11.1 API

建议直接复用现有 cursor：

```cpp
auto SeekByRank(uint32_t rank) -> Cursor;
```

如果 `rank >= Size()`，返回 `End()`。

### 11.2 算法

假设 `remaining = rank`。

从 root 开始：

1. 如果当前是 leaf：
   - 必须满足 `remaining == 0`
   - 命中目标
2. 如果当前是 inner node：
   - 按 child 的字典序从小到大扫描
   - 对每个 child 取：

```cpp
child_count = SubtreeValueCount(child)
```

   - 若 `remaining < child_count`：
     - 目标在这个 child 子树内
     - 追加当前 prefix 和 edge，继续向下
   - 否则：

```cpp
remaining -= child_count;
```

   - 继续找下一个 child

直到到达 leaf。

### 11.3 伪代码

```cpp
auto SeekByRank(uint32_t rank) -> Cursor {
    Cursor cursor(this);
    if (root_ == nullptr || rank >= Size()) {
        return cursor;
    }

    uint32_t remaining = rank;
    Node* cur = root_;

    while (cur->Type() != NodeType::Leaf) {
        auto* inner = AsInner(cur);
        AppendPrefix(cur, cursor.key_buf_);

        ChildRef chosen{};
        bool found = false;
        ForEachChildInOrder(inner, [&](byte edge, Node* child) {
            uint32_t child_count = SubtreeValueCount(child);
            if (remaining < child_count) {
                chosen = {edge, child};
                found = true;
                return false;
            }
            remaining -= child_count;
            return true;
        });

        CHECK(found);
        cursor.stack_.push_back({inner, chosen.edge, cursor.key_buf_.size()});
        cursor.key_buf_.push_back(chosen.edge);
        cur = chosen.child;
    }

    AppendPrefix(cur, cursor.key_buf_);
    cursor.leaf_ = cur;
    return cursor;
}
```

### 11.4 复杂度

- 高度部分：`O(height)`
- 每层选 child：第一版最多扫描 256 个 edge，视作常数
- 总体：`O(height)`

之后连续 `Next()` 扫描 `count` 个元素，总复杂度是：

```cpp
O(height + count)
```

这就是 ZSet `ZRANGE start stop` 需要的复杂度形态。

---

## 12. IterateByRank 设计

有了 `SeekByRank()` 之后，区间扫描就不应再从 `Begin()` 线性跳到 `start`。

建议实现：

```cpp
template <class Fn>
auto IterateByRank(uint32_t rank_start, uint32_t rank_end, Fn&& fn) -> bool;
```

行为：

- `rank_start >= Size()` 时直接返回 `true`
- `rank_end` 大于末尾时自动截断到 `Size() - 1`
- 闭区间 `[rank_start, rank_end]`

实现方式：

1. `Cursor it = SeekByRank(rank_start)`
2. 依次回调并 `Next()`
3. 当 `i == rank_end` 或 `it == End()` 时停止

这样上层 ZSet 的 `ZRANGE` 就不再需要自己写“从头跳 start 次”的逻辑。

---

## 13. 为何不选“递归向上返回 delta”的设计

另一种可选方案是：

- `InsertInternal()` / `EraseInternal()` 改成递归
- 每一层返回 `+1 / 0 / -1`
- 父节点据此修正自己的 `value_count_`

这个方案理论上成立，但不推荐作为当前代码基线，原因有三点：

1. 现有 ART 实现本身是迭代下降风格，不是递归风格
2. 当前代码已经大量依赖 `Node** cur_ref` 做 inplace replacement，迭代实现更自然
3. rank 之外，`Cursor`, `LowerBound`, path compression 这些逻辑都已经围绕“显式路径”展开，mutation 也统一成 path 风格会更一致

因此第一版建议：

- 继续保留迭代式 descent
- 由外层创建并传入 `AncestorPath`
- 内部负责在结构变更过程中修正 path
- 外层在操作成功后统一回填 `value_count_`

---

## 14. 测试计划

### 14.1 计数不变量

每次 insert / erase 后，递归校验：

- leaf 返回 `1`
- inner 返回 `sum(children)`
- inner 的递归结果必须等于 `value_count_`
- 根的结果必须等于 `Size()`

### 14.2 Insert 场景

- 空树插入
- prefix split 插入
- 在 Node4/16/48/256 上插入新 child
- duplicate insert
- upsert existing value
- grow 路径上计数不丢失

### 14.3 Erase 场景

- erase miss
- 删除 root 唯一 leaf
- 删除普通 leaf
- shrink 后计数保持正确
- path compression 后计数保持正确

### 14.4 Rank 场景

- `SeekByRank(0)`
- `SeekByRank(Size() - 1)`
- `SeekByRank(Size()) == End()`
- 多个相同 score、不同 member 的 key 顺序是否正确
- `IterateByRank(start, end)` 是否和全量遍历切片结果一致

### 14.5 回归场景

- `LowerBound()`
- `Range(min, max)`
- 现有 cursor 的 `Next()`

因为 rank 功能会复用现有 cursor，所以这些都需要一起回归。

---

## 15. 实现顺序建议

建议按下面顺序落地：

1. 把 `MoveHeaderTo()` 扩展为同时搬运 `value_count_`
2. 加 `SubtreeValueCount()` 和调试校验 helper
3. 重构 `InsertInternal()` / `EraseInternal()` 为“接收外层 `AncestorPath` 并维护它”的版本
4. 在所有 insert / erase / split / grow / shrink / compression 路径上补齐 path 修正逻辑
5. 先写不变量测试，确认计数始终正确
6. 再实现 `SeekByRank()`
7. 最后实现 `IterateByRank()` 并接入 ZSet `ZRANGE`

---

## 16. 最终结论

ART 要支持像 Dragonfly 一样的 `ToRank()`，关键不在 cursor，而在 mutation 期把 `value_count_` 维护成强不变量。

这一版设计的核心决策是：

- `InnerNode::value_count_` 表示子树 leaf 数
- 对外 `Insert()` / `Erase()` 语义尽量不变
- 外层创建 `AncestorPath`，内部负责维护它的最终有效性
- 计数更新时机放在 wrapper 末尾，由外层统一回填
- split 节点先初始化旧计数，再通过 path 参与外层回填
- grow / shrink 通过 `MoveHeaderTo()` 继承计数

在这个基础上，`SeekByRank(rank)` 和 `IterateByRank(start, end)` 都可以直接建立起来，ZSet 的 `ZRANGE` 也就不再需要做 `O(start)` 的前缀跳过。
