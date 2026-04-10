# ZSet 支持开发计划

**Issue:** [#2 Sorted Set Support](https://github.com/idle-lab/idleKV/issues/2)
**目标命令:** ZADD, ZREM, ZRANGE
**日期:** 2026-04-09

---

## 1. 概述

本计划实现 Redis ZSet 的基础命令支持，采用 ART (Adaptive Radix Tree) 作为底层存储结构。

### 1.1 核心设计

**存储结构：**
- **Key（ART key）：** score + member 组合键（使用可排序的二进制编码）
- **Value（ART value）：** 无直接存储（score 已编码在 key 中）

**排序语义：** ART 按 key 字典序排列，即先按 score 排序，score 相同时按 member 字典序排序。

> 实现要点：
> - score 需要转换为可排序的二进制格式（如网络字节序double或专门的编码）
> - 当 score 相同时，通过 member 的字典序进行二次排序
> - ZRANGE 返回按 score 升序排列的元素，score 相同时按 member 字典序

---

## 2. ART 迭代器实现

### 2.1 背景

idleKV 当前 ART 只有精确查找 `FindNext(key)`，缺少顺序遍历能力。需要新增方法支持迭代器。

### 2.2 参考实现分析

参考 `adaptive-radix-tree` 库的迭代器实现（`tree_it.hpp`, `child_it.hpp`）：

**核心机制：**
- **Leaf Stack（叶子栈）：** 维护从根到当前叶子节点的完整路径
- **Stack Frame：** 包含节点指针、当前 child 索引、累计的 key 前缀
- **子节点顺序遍历：** 通过 `next_partial_key(partial_key)` 找到下一个 >= partial_key 的 child

**idleKV 与参考实现的差异：**

| 特性 | idleKV | 参考实现 |
|------|--------|----------|
| 存储结构 | `keys_[N]`, `children_[N]`（children 按 key 排序） | 同左 |
| 精确查找 | `FindNext(key)` | `find_child(key)` |
| 顺序遍历 | **缺失** | `next_partial_key(key)` |
| 迭代器 | **缺失** | `tree_it` |

### 2.3 需要在 Node 类新增的方法

各节点类型需新增 `FindChildGte(byte key)` 方法，返回第一个 `>= key` 的 child 指针和索引。

```cpp
// Node4, Node16, Node48, Node256 各自实现
auto FindChildGte(byte key) -> std::pair<Node**, int>;  // 返回 {child_ptr, index}

// Node4 示例实现
auto Node4::FindChildGte(byte key) -> std::pair<Node**, int> {
    for (int i = 0; i < size_; i++) {
        if (keys_[i] >= key) {
            return {&next_[i], i};
        }
    }
    return {nullptr, -1};
}
```

**各节点类型的 FindChildGte 复杂度：**

| 节点类型 | 实现方式 | 复杂度 |
|----------|----------|--------|
| Node4 | 线性扫描 keys_ | O(4) |
| Node16 | 线性扫描 keys_（SIMD 用于精确查找，不适用于范围） | O(16) |
| Node48 | 线性扫描 indexes_[256] | O(256) worst |
| Node256 | 直接访问 children_[key]，但需找下一个存在的 | O(256) worst |

### 2.4 ArtIterator 实现

**文件：** `src/db/storage/art/iterator.h`（新建）

```cpp
template <class ValueType>
class ArtIterator {
public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = ValueType;
    using difference_type = std::ptrdiff_t;
    using pointer = ValueType*;
    using reference = ValueType&;

    ArtIterator() = default;
    explicit ArtIterator(Node* root, bool at_end = false);

    // 迭代器操作
    auto operator++() -> ArtIterator&;      // 前置++
    auto operator++(int) -> ArtIterator;   // 后置++
    auto operator*() const -> ValueType&;
    auto operator->() const -> ValueType*;
    auto operator==(const ArtIterator&) const -> bool;
    auto operator!=(const ArtIterator&) const -> bool;

    // Key 重建
    auto Key() const -> ArtKey;  // 从栈帧重建完整 key

    // 工厂方法
    static auto Begin(Node* root) -> ArtIterator;
    static auto End(Node* root) -> ArtIterator;
    static auto GreaterEqual(Node* root, ArtKey key) -> ArtIterator;

private:
    struct StackFrame {
        Node*       node;           // 当前节点
        int         child_idx;      // 当前 child 索引（-1 表示初始）
        int         depth;          // 深度
        std::array<byte, 64> key_prefix;  // 累计的 key 前缀（上限 64 字节）
        size_t      key_len;        // key 前缀长度
    };

    auto SeekToLeaf() -> void;
    auto AdvanceToNext() -> bool;   // 尝试移动到下一个元素，返回是否成功

    std::vector<StackFrame> stack_;
    Node* root_{nullptr};
    bool at_end_{false};
};
```

**核心算法：**

1. **Begin():** 从根开始，深度优先向下找最左叶子
2. **operator++():**
   - 若当前节点还有未遍历的 child，移动到下一个 child，深度优先下降到最左叶子
   - 否则向上回溯，找第一个有未遍历 sibling 的祖先
   - 若回到根且无更多 sibling，标记 `at_end_ = true`
3. **Key():** 拼接所有栈帧的 prefix 和路径上的 partial key

### 2.5 Art 类新增方法

**文件：** `src/db/storage/art/art.h`

```cpp
// 新增迭代器支持
auto Begin() -> ArtIterator<ValueType>;
auto End() -> ArtIterator<ValueType>;
auto GreaterEqual(ArtKey key) -> ArtIterator<ValueType>;
auto LessThan(ArtKey key) -> ArtIterator<ValueType>;
auto Range(ArtKey min, ArtKey max) -> std::pair<ArtIterator<ValueType>, ArtIterator<ValueType>>;
```

---

## 3. ZSet 数据结构实现

### 3.1 ZSet 类设计

**文件：** `src/db/storage/zset.h`

```cpp
class ZSet {
public:
    explicit ZSet(std::pmr::memory_resource* mr);

    // ZADD: 添加 member（score 为 0 时正常添加，不会删除）
    // 返回 true 表示新添加，false 表示更新
    auto Add(std::string member, double score) -> bool;

    // ZREM: 删除 member
    // 返回 true 表示删除成功，false 表示不存在
    auto Rem(std::string member) -> bool;

    // ZRANGE: 按 score 升序返回 [start, stop] 区间元素
    // score 相同时按 member 字典序排序
    // 返回 (member, score) 对的向量
    struct MemberScore {
        std::string member;
        double score;
    };
    auto Range(size_t start, size_t stop, bool with_scores) -> std::vector<MemberScore>;

    // ZCOUNT: 统计 score 在 [min, max] 区间内的成员数
    auto Count(double min, double max) -> size_t;

    auto Size() const -> size_t { return size_; }

private:
    Art<std::nullptr_t> data_;  // (score, member) -> nullptr (score 已编码在 key 中)
    size_t size_{0};
};
```

**注意：** ZRANGE 返回按 score 升序排列的元素，score 相同时按 member 字典序。ZCOUNT 使用线性扫描统计。

### 3.2 ZSet 操作实现

**ZADD key member [score member ...]**

```
行为：
- 若 member 不存在：添加新 member
- 若 member 存在：更新 score

返回值：新添加到 ZSet 的 member 数量（不包括更新 score 的 member）
注意：score 为 0 不会自动删除 member，ZREM 用于删除 member
```

**ZREM key member [member ...]**

```
行为：
- 删除所有指定的 member
- 不存在的 member 忽略

返回值：实际删除的 member 数量
```

**ZRANGE key start stop [WITHSCORES]**

```
行为：
- 返回按 score 升序排列的 [start, stop] 区间元素（inclusive 闭区间）
- score 相同时按 member 字典序排序
- start/stop 支持负索引（-1 表示最后一个元素）
- stop < 0 时转换为 stop = size + stop + 1
- WITHSCORES 时返回 member 和 score 交替的列表

返回值：
- 无 WITHSCORES：[member1, member2, ...]
- 有 WITHSCORES：[member1, score1, member2, score2, ...]
```

---

## 4. 命令实现

### 4.1 ZADD

**文件：** `src/db/command/zset.cc`

**语法：** `ZADD key score member [score member ...]`

**选项（第一阶段暂不支持）：**
- `NX` — 只在 member 不存在时添加
- `XX` — 只更新已存在的 member
- `LT` — 仅当新 score 小于当前 score 时更新
- `GT` — 仅当新 score 大于当前 score 时更新
- `CH` — 返回改变（新增或更新）的 member 数量
- `INCR` — 将 score 累加到现有值上，类似于 ZINCRBY

**第一阶段实现：** 基础版本（无选项），仅支持 `ZADD key score member [score member ...]`

**响应：** 整数，新添加到 ZSet 的 member 数量（不包括更新 score 的 member）

### 4.2 ZREM

**语法：** `ZREM key member [member ...]`

**响应：** 整数，实际删除的 member 数量

### 4.3 ZRANGE

**语法：** `ZRANGE key start stop [WITHSCORES]`

**选项（第一阶段支持）：**
- `WITHSCORES` — 同时返回 score

**索引语义：**
- 0 = 第一个元素
- -1 = 最后一个元素
- -2 = 倒数第二个元素
- start > stop 时返回空列表

**响应：**
- 无 WITHSCORES：字符串数组
- 有 WITHSCORES：字符串数组，score 作为浮点数穿插在 member 后

### 4.4 ZRANGEBYSCORE（第一阶段暂不实现，已废弃）

**语法：** `ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]`

**说明：** Redis 6.2+ 已废弃此命令，改用 `ZRANGE key min max BYSCORE [WITHSCORES] [LIMIT offset count]`。利用 ART 的 (score, member) 组合键可实现高效的范围查找。

---

## 5. 命令注册

**文件：** `src/db/engine.cc`

```cpp
auto IdleEngine::InitCommand() -> void {
    InitSystemCmd(this);
    InitStrings(this);
    InitHash(this);
    InitList(this);
    InitZSet(this);  // 添加此行
}
```

**文件：** `src/db/command/zset.cc`

```cpp
auto InitZSet(IdleEngine* eng) -> void {
    eng->RegisterCmd("zadd", -4, 1, 1, ZAdd, SingleWriteKey, CmdFlags::Transactional);
    eng->RegisterCmd("zrem", -3, 1, 1, ZRem, SingleWriteKey, CmdFlags::Transactional);
    eng->RegisterCmd("zrange", -4, 1, 1, ZRange, SingleReadKey, CmdFlags::Transactional);
}
```

> 注意：ZRANGE 的 arity = -4 表示最少 4 个参数（zrange key start stop），可变形参数为 WITHSCORES。

---

## 6. 数据流

```
Client → TCP accept → RedisService::Handle()
  → Connection (fiber per connection) → Parser (RESP2) → CmdArgs
  → IdleEngine::DispatchCmd()
  → ZAdd/ZRem/ZRange 执行
  → Shard::Add(task) → TaskQueue → fiber runs task on DB
  → ZSet::Add/Rem/Range 操作 ART
  → Sender writes response back
```

---

## 7. 实现步骤

| Phase | 任务 | 文件 | 依赖 |
|-------|------|------|------|
| 1 | Node 新增 `FindChildGte` 方法 | `node.h`, `node.cc` | 无 |
| 2 | 实现 `ArtIterator` | `iterator.h`（新建） | Phase 1 |
| 3 | Art 类新增迭代器方法 | `art.h` | Phase 2 |
| 4 | 完善 ZSet 数据结构 | `zset.h` | Phase 2 |
| 5 | 实现 ZADD 命令 | `zset.cc` | Phase 4 |
| 6 | 实现 ZREM 命令 | `zset.cc` | Phase 4 |
| 7 | 实现 ZRANGE 命令 | `zset.cc` | Phase 4 |
| 8 | 命令注册 | `engine.cc` | Phase 5-7 |
| 9 | 编译验证 + 测试 | - | Phase 8 |

---

## 8. 风险点

| 风险 | 描述 | 缓解措施 |
|------|------|----------|
| Node48/256 的 FindChildGte 性能 | 最坏 O(256) 扫描 | 这是 ART 的固有特性，可接受 |
| ZRANGE 负索引处理 | 需要转换负索引为正索引 | 在命令层处理，简单直接 |
| ZSet 内存管理 | Value 存储 double，ZSet 对象析构需正确释放 ART | 检查 `Value::ReleaseValue()` 中 ZSet 的析构路径 |
| 迭代器 key 重建 | 每次 operator++ 需要重建完整 key | 栈帧中缓存 key 前缀，复杂度 O(tree depth) |

---

## 9. 测试计划

**基础功能测试：**
- ZADD 添加单个/多个 member
- ZADD 更新已存在 member 的 score
- ZREM 删除存在的 member
- ZREM 删除不存在的 member（应返回 0）
- ZRANGE 正索引范围查询
- ZRANGE 负索引范围查询
- ZRANGE WITHSCORES

**边界测试：**
- ZRANGE start > stop
- ZRANGE 超出范围（start >= size）
- ZADD score 为 0（member 不会被删除，score 正常存储为 0）
- 空 ZSet 上的所有操作

---

## 10. 后续优化方向

1. **ZRANGE BYSCORE：** 利用 score 排序直接用 ART 范围查找（Redis 6.2+ 替代 ZRANGEBYSCORE）
2. **ZRANGE BYLEX：** 按 member 字典序范围查找（所有元素 score 相同时）
3. **ZINCRBY 命令：** 原子地增加 member 的 score
4. **ZSCORE 命令：** 查询单个 member 的 score
5. **ZRANK 命令：** 返回 member 的排名（按 score 排序）
