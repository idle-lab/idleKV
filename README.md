# IdleKV

IdleKV is a Redis-compatible in-memory key-value store written in C++20.
The codebase is built around a shared-nothing, multi-threaded runtime that combines
Boost.Asio with Boost.Fiber to execute network I/O, request parsing, shard dispatch,
and command execution with low scheduling overhead.

## Highlights

- Linux-first runtime with optional `io_uring` backend support
- One event-loop thread per online CPU by default
- Fiber-based scheduling with explicit priorities and CPU-budget yielding
- Shared-nothing sharding model with per-shard task queue and memory resource
- Strict serializability for the currently implemented command set
- RESP2 parser and Redis-compatible TCP service
- Pipeline squashing to reduce per-command scheduling overhead
- ZSet implementation backed by an Adaptive Radix Tree (ART) rank iterator

## Current Scope

IdleKV is best described today as a high-performance Redis-compatible prototype rather
than a complete Redis replacement. The runtime, sharding model, string path, and ZSet
path are implemented; several other areas are still intentionally incomplete.

### Implemented commands

- System: `PING`, `ECHO`, `SELECT`, `INFO`
- Strings: `SET`, `GET`, `DEL`, `MSET`, `MGET`
- Sorted sets: `ZADD`, `ZREM`, `ZRANGE`, `ZRANGE ... WITHSCORES`

### Not implemented or incomplete

- Hash commands are not registered yet
- List commands are not registered yet
- Persistence is incomplete: WAL and snapshot are only structural stubs
- Config-file parsing exists as a CLI option but `ParseFromFile()` is not implemented
- General multi-shard transactions are still incomplete; current multi-shard support is limited
  to the implemented string multi-key commands (`MSET`, `MGET`, `DEL key [key ...]`)
- Integer-tagged `Value` path is not implemented

## Consistency

For the command set implemented today, IdleKV guarantees **strict serializability**:
each completed command appears to take effect atomically at a single point between
its invocation and response, and the resulting execution respects real-time order.

This guarantee is scoped to the commands listed in the "Implemented commands" section
above. In particular, current cross-shard support is limited to the implemented
multi-key string commands (`MSET`, `MGET`, `DEL key [key ...]`); unsupported or
incomplete features should not be read as covered by this claim.

The repository also includes an Elle-based history-checking workflow for validating
strict serializability of emitted workloads. See
[`docs/strict_serializability_with_elle.md`](docs/strict_serializability_with_elle.md).

## Architecture Overview

### Runtime model

At startup, IdleKV creates one `EventLoop` thread per online CPU by default.
Each event loop owns a `boost::asio::io_context` and installs a custom fiber scheduler.
Fibers are assigned priorities (`HIGH`, `NORMAL`, `BACKGROUND`), and the runtime can
yield a busy fiber when it exceeds a CPU-cycle budget.

The important pieces live in:

- `src/server/` for event loops, thread state, server startup, and handlers
- `src/utils/fiber/` for the custom scheduler and fiber utilities
- `src/redis/` for RESP2 parsing, connections, and request/response handling


### Sharding

Keys are hashed with `XXH64` to a shard id. By default, the engine creates:

- `6` shards
- `16` logical databases per shard (`SELECT`)
- one bounded shard task queue (`boost::fibers::buffered_channel`) per shard
- one `ShardMemoryResource` per shard backed by a dedicated mimalloc heap

This keeps command execution shard-local whenever possible and avoids a shared global
write path.


## Building

For full instructions, see [docs/build-from-source.md](docs/build-from-source.md).

### Requirements

- Linux (x86_64 or ARM64)
- C++20 compiler
- CMake
- Ninja
- Boost: `fiber`, `context`, `system`, `thread`
- `xxhash`

### Quick start

```bash
git clone git@github.com:idle-lab/idleKV.git
cd idleKV
git submodule update --init --recursive

make release
```

The main binary is generated at:

```text
build/src/idlekv
```

### Useful build targets

```bash
make debug
make release
make format
make format-check
```

### Relevant CMake options

| Option | Default | Meaning |
| --- | --- | --- |
| `ENABLE_TEST` | `ON` | Build unit tests |
| `ENABLE_BENCHMARK` | `ON` | Build benchmark tools |
| `DISABLE_CHECK` | `ON` | Disable runtime `CHECK` macros |
| `ENABLE_ASIO_IO_URING` | `OFF` | Use `io_uring` backend when available |
| `ENABLE_ASAN` | `OFF` | Enable AddressSanitizer |


## Repository Layout

```text
.
├── src/
│   ├── common/          configuration and logging
│   ├── db/              engine, shards, transactions, commands
│   ├── db/storage/      value model, kv store, ART, ZSet, WAL stub
│   ├── redis/           RESP2 parser, connections, Redis service
│   ├── server/          event loops, accept loop, metrics service
│   ├── metric/          Prometheus counters/gauges
│   └── utils/           fibers, CPU helpers, ranges, pools
├── test/                unit tests
├── scripts/             benchmark and plotting scripts
├── docs/                build notes and design documents
├── benchmark/           benchmark command notes
├── paper/               paper sources and generated figures
└── third_part/          vendored dependencies
```
