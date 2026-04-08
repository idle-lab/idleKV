# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

```bash
# Release build (default)
make release

# Debug build
make debug

# Clean
make clean

# Format check
make format-check

# Auto-format
make format

# Run all tests (after building)
ninja unit_test -C build -j$(nproc) && ./build/unit_test

# Run a single test
./build/unit_test --gtest_filter=TestSuiteName.TestName

# Run with AddressSanitizer
cmake -DCMAKE_BUILD_TYPE=Debug -DENABLE_ASAN=ON -B build -S . -G Ninja && ninja idlekv -C build

# Build with io_uring backend
cmake -DCMAKE_BUILD_TYPE=Release -DENABLE_ASIO_IO_URING=ON -B build -S . -G Ninja && ninja idlekv -C build
```

Build uses Ninja + CMake. The binary output is `build/idlekv`. Tests use Google Test.

## Architecture

idleKV is a Redis-protocol-compatible (RESP2) key-value store written in C++20.

### Concurrency Model: Boost.Fiber + Boost.Asio

Each CPU core runs one `EventLoop` thread with a `boost::asio::io_context` and a custom `Priority` fiber scheduler. The scheduler has three priority levels (HIGH > NORMAL > BACKGROUND) and uses hardware cycle counters (`rdtsc` on x86, `cntvct_el0` on ARM) for CPU budget enforcement — a fiber that exceeds its cycle budget yields to others via `MaybeYieldOnCpuBudget()`.

Fibers and Asio are bridged through `yield_t` / `async_result` specializations that let Asio async operations suspend/resume fibers directly, avoiding callback hell.

### Request Flow

```
Client → TCP accept (Asio) → RedisService::Handle()
  → Connection (fiber per connection) → Parser (RESP2) → CmdArgs
  → IdleEngine::DispatchCmd() → Transaction::Execute()
  → Shard::Add(task) → TaskQueue (buffered_channel) → fiber runs task on DB
  → Sender writes response back on connection fiber
```

### Sharding

Keys are hashed with xxhash to `ShardId` (default: 6 shards). Each `Shard` owns:
- A `TaskQueue` (bounded `buffered_channel` of capacity 64, consumed by a HIGH-priority fiber)
- Multiple `DB` instances (default: 16, matching Redis `SELECT`)
- A `ShardMemoryResource` backed by a dedicated mimalloc heap (`mi_heap_t`) — tracks per-shard memory usage

Cross-shard commands dispatch via `Shard::Add()` which posts into the target shard's TaskQueue.

### Pipeline Squashing (`CmdSquasher`)

When a pipeline has multiple commands, the `CmdSquasher` groups them by shard. Commands targeting the same shard execute sequentially in a single shard fiber hop, avoiding per-command fiber scheduling overhead. Results are collected and sent back in order.

### Value System (`PrimeValue`)

`PrimeValue = shared_ptr<Value>`. `Value` uses a tagged union with inline optimization: strings ≤16 bytes are stored inline (no heap allocation), longer strings and container objects (ZSet) use pmr-allocated memory through the shard's `ShardMemoryResource`.

### Key Source Paths

- `src/server/` — EventLoop, EventLoopPool, fiber runtime, thread state
- `src/redis/` — RESP2 parser, connection handler, Sender/Writer
- `src/db/` — Engine (command dispatch), Shard, DB, Transaction, CmdSquasher, command implementations
- `src/db/storage/` — KvStore (absl flat_hash_map), Value, ART index, ZSet, WAL (stub), EBR allocator
- `src/db/command/` — Individual Redis command implementations (strings, hash, list, zset, system)
- `src/common/` — Config, logger, result types

### Key Global State

- `idlekv::engine` — The single `IdleEngine` instance, holds command map and shard pointers
- `ThreadState::Tlocal()` — Per-thread EventLoop pointer
- `FiberProps` — Per-fiber priority and cycle tracking, set at fiber creation

### Incomplete / In-Progress Features

- Hash and Set object types have stub implementations (`CHECK(false)`)
- WAL and snapshot are structurally stubbed
- Config file parsing not implemented (`ParseFromFile` throws)
- Multi-shard transactions not yet supported (asserts `active_shard_count == 1`)
- INT_TAG values not implemented

### Third-Party Dependencies (vendored in `third_part/`)

abseil, mimalloc, spdlog, gtest. Non-vendored: Boost (fiber, context, system, thread), xxhash, CLI11.
