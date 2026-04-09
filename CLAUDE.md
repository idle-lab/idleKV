# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Instructions

**For complete build instructions, see [docs/build-from-source.md](docs/build-from-source.md)**

## Architecture

**IdleKV** is a high-performance, Redis compatible in-memory data store written in C++20. It delivers significantly higher throughput than traditional single-threaded Redis implementations.

### Key Characteristics

- **Language**: C++20
- **Architecture**: Shared-nothing multi-threaded design (via `Boost.fiber` library)
- **Build System**: CMake(3.28.3) + Ninja
- **Target Platform**: Linux (kernel 5.11+ recommended)
- **Protocols**: Redis RESP2


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

## Development Rules

### Validation Checklist

Before claiming a task is complete, verify:

#### Code Quality

- [ ] Code compiles without errors: `cd build-dbg && ninja dragonfly`
- [ ] Code follows Google C++ Style Guide (run `make format-check`)
- [ ] No new ASAN/UBSAN violations

### Documentation

- [ ] Public APIs have comments explaining purpose
- [ ] Complex algorithms have explanatory comments
- [ ] README or docs (include `README.md`, `CLAUDE.md`, files in `docs/` folder etc) updated if behavior changes

#### Performance

- [ ] No obvious performance regressions (run benchmarks if needed)
- [ ] No unnecessary allocations in hot paths
- [ ] Lock-free data structures used where appropriate

