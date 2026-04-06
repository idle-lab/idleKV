

## Optimistic Locks


一个乐观锁由一个锁和一个版本计数组成。

```c
struct Node {
    atomic<uint64_t> version;
    // ... other node fields
};

// Helper function: Wait for node to become unlocked
uint64_t awaitNodeUnlocked(Node node) {
    uint64_t version = node.version.load();
    while ((version & 2) == 2) {  // Spin while locked
        pause();
        version = node.version.load();
    }
    return version;
}

// Check if version indicates obsolete node
bool isObsolete(uint64_t version) {
    return (version & 1) == 1;
}

// Read lock with restart semantics
uint64_t readLockOrRestart(Node node) {
    uint64_t version = awaitNodeUnlocked(node);
    if (isObsolete(version)) {
        restart();  // Restart operation
    }
    return version;
}

// Validate version and restart if changed
void checkOrRestart(Node node, uint64_t version) {
    readUnlockOrRestart(node, version);
}

// Read unlock with restart semantics
void readUnlockOrRestart(Node node, uint64_t version) {
    if (version != node.version.load()) {
        restart();
    }
}

// Read unlock with restart and write lock handoff
void readUnlockOrRestart(Node node, uint64_t version, Node lockedNode) {
    if (version != node.version.load()) {
        writeUnlock(lockedNode);
        restart();
    }
}

// Upgrade read lock to write lock
void upgradeToWriteLockOrRestart(Node node, uint64_t version) {
    if (!node.version.compare_exchange_weak(version, setLockedBit(version))) {
        restart();
    }
}

// Upgrade read lock to write lock with handoff
void upgradeToWriteLockOrRestart(Node node, uint64_t version, Node lockedNode) {
    if (!node.version.compare_exchange_weak(version, setLockedBit(version))) {
        writeUnlock(lockedNode);
        restart();
    }
}

// Acquire write lock
void writeLockOrRestart(Node node) {
    uint64_t version;
    do {
        version = readLockOrRestart(node);
    } while (!upgradeToWriteLockOrRestart(node, version));
}

// Release write lock
void writeUnlock(Node node) {
    // Reset locked bit and increment version counter
    node.version.fetch_add(2);
}

// Release write lock and mark obsolete
void writeUnlockObsolete(Node node) {
    // Set obsolete flag and reset locked bit
    node.version.fetch_add(3);
}

// Helper function: Set locked bit in version
uint64_t setLockedBit(uint64_t version) {
    return version + 2;
}
```


```python
function lookupOpt(key, node, level, parent, versionParent):
    version = readLockOrRestart(node)
    if parent != null:
        readUnlockOrRestart(parent, versionParent)
    // 检查前缀是否匹配，可能增加层级
    if !prefixMatches(node, key, level):
        readUnlockOrRestart(node, version)
        return null  // 键未找到
    // 查找子节点
    nextNode = node.findChild(key[level])
    checkOrRestart(node, version)
    if isLeaf(nextNode):
        value = getLeafValue(nextNode)
        readUnlockOrRestart(node, version)
        return value  // 键找到
    if nextNode == null:
        readUnlockOrRestart(node, version)
        return null  // 键未找到
    // 递归到下一层
    return lookupOpt(key, nextNode, level+1, node, version)
```