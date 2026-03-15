# Tech Stack

## Language & Standard
- C++20 (required), Linux only

## Build System
- CMake 3.17+, out-of-source builds in `build/`
- Default build type: `Debug` if not specified
- Compiler flags: `-Wall -Wextra -O3 -march=native` (release: `-O3 -DNDEBUG`, debug: `-g -O0 -DDEBUG`)

## Key Libraries (in `third_part/`)
| Library | Purpose |
|---|---|
| asio (standalone) | Async I/O, coroutines (`asio::awaitable`) |
| asiochan | Channel primitives over asio |
| spdlog | Logging (`LOG(level, ...)` macro, `spdlog::`) |
| mimalloc | High-performance allocator (linked globally) |
| CLI11 | CLI argument parsing |
| gtest | Unit testing |

## External Dependencies (system)
- `xxhash` — key hashing (`find_library(XXHASH_LIBRARY xxhash REQUIRED)`)
- `liburing` — io_uring backend for asio (enabled by default via `ENABLE_ASIO_IO_URING=ON`)

## CMake Options
| Option | Default | Description |
|---|---|---|
| `ENABLE_TEST` | ON | Build unit tests |
| `ENABLE_BENCHMARK` | ON | Build benchmarks |
| `ENABLE_ASIO_IO_URING` | ON | Use io_uring backend (requires liburing-dev) |

## Compile Definitions (always set)
- `ASIOCHAN_USE_STANDALONE_ASIO=ON`
- `ASIO_NO_EXCEPTIONS=1`
- `ASIO_HAS_IO_URING=1` / `ASIO_DISABLE_EPOLL=1` (when io_uring enabled)

## Common Commands

```bash
# Configure (debug)
cmake -B build -DCMAKE_BUILD_TYPE=Debug

# Configure (release)
cmake -B build -DCMAKE_BUILD_TYPE=Release

# Build everything
cmake --build build -j$(nproc)

# Build only the server
cmake --build build --target idlekv -j$(nproc)

# Run tests
cmake --build build --target test
# or directly:
./build/test/<test_binary>

# Run benchmarks
./build/benchmark/<bench_binary>

# Disable io_uring (if liburing not available)
cmake -B build -DENABLE_ASIO_IO_URING=OFF
```
