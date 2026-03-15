# Project Structure

```
idlekv/
├── src/
│   ├── main.cc               # Entry point: Config → Server → Engine → listen
│   ├── common/               # Shared utilities
│   │   ├── config.h          # Config struct (CLI11-backed)
│   │   ├── logger.h/cc       # Logger setup, LOG() macro
│   │   ├── result.h          # ResultT<T> — error-or-value type
│   │   └── asio_no_exceptions.h
│   ├── server/               # TCP server, event loop pool
│   │   ├── server.h/cc       # Server: accepts connections, dispatches to Handler
│   │   ├── handler.h         # Abstract Handler base class
│   │   ├── el_pool.h/cc      # EventLoopPool: manages io_context threads
│   │   └── thread_state.h/cc # Thread-local state (pool index, etc.)
│   ├── redis/                # Redis protocol layer
│   │   ├── parser.h/cc       # RESP parser (Reader/Writer/Parser/Sender)
│   │   ├── connection.h/cc   # Connection: owns socket, reader, writer
│   │   ├── service.h/cc      # RedisService: implements Handler, drives command loop
│   │   ├── service_interface.h
│   │   └── error.h/cc        # Redis error string constants
│   ├── db/                   # Database engine
│   │   ├── engine.h/cc       # IdleEngine: global engine, command dispatch, sharding
│   │   ├── db.h/cc           # DB: per-database state
│   │   ├── shard.h           # Shard: unit of data ownership
│   │   ├── command.h         # Cmd, CmdContext, Exector/Prepare function pointer types
│   │   ├── result.h          # ExecResult type
│   │   ├── strings.cc        # String command implementations
│   │   ├── hash.cc           # Hash command implementations
│   │   ├── list.cc           # List command implementations
│   │   ├── systemcmd.cc      # System commands (PING, SELECT, etc.)
│   │   ├── wal.h             # WAL structures
│   │   ├── task_queue.h      # Task queue for async dispatch
│   │   ├── xmalloc.h/cc      # Allocator wrappers
│   │   └── storage/          # Storage backends
│   │       ├── kvstore.h     # KVStore interface
│   │       ├── result.h
│   │       ├── art/          # Adaptive Radix Tree implementation
│   │       ├── dash/         # DASH hash table implementation
│   │       └── skiplist/     # Skiplist implementation
│   ├── metric/               # Metrics (avg.h — rolling average)
│   └── utils/                # Generic utilities
│       ├── block_queue/      # Thread-safe blocking queue
│       ├── condition_variable/
│       ├── cpu/              # CPU affinity helpers
│       ├── pool/             # Object pool
│       └── timer/            # Timer utilities
├── test/                     # GTest unit tests (mirrors src/ structure)
├── benchmark/                # Benchmarks
├── third_part/               # Vendored dependencies
├── scripts/                  # Tooling scripts (flamegraph, bench clients)
├── docs/                     # Architecture and design docs
└── cmake/                    # CMake helper modules
```

## Key Architectural Patterns

- All source files use `.cc` / `.h` extensions.
- Everything lives in the `idlekv` namespace.
- Async code uses `asio::awaitable<T>` coroutines with `co_await` / `co_return`.
- Errors are propagated via `ResultT<T>` (wraps `std::error_code` + optional value). Check with `.ok()`, get value with `.value()`, get error with `.err()`.
- `idlekv_core` is a static library containing all `src/**/*.cc` except `main.cc`. The `idlekv` executable links against it.
- Commands are registered on `IdleEngine` as `Cmd` entries with function pointers (`Exector`, `Prepare`). Command implementations live in `src/db/<type>.cc`.
- Include paths are relative to `src/` (e.g., `#include "db/engine.h"`, not `#include "src/db/engine.h"`).
- Third-party headers are included with their library name prefix (e.g., `<asio/asio.hpp>`, `<spdlog/spdlog.h>`, `<mimalloc.h>`).
