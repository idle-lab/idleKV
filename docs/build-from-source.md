# Build from Source

## 1. Recommended Operating System

**idleKV** supports **Linux (x86_64 and ARM64)**. The minimum kernel requirements are:

| Feature | Minimum Kernel Version |
|---------|----------------------|
| Basic build & run | Linux 5.4+ |
| io_uring backend | Linux 5.19+ |
| ARM64 support | Linux 5.4+ |


### Non-Linux Support

- **macOS**: Not supported due to Boost.Fiber/Boost.Context compatibility issues
- **Windows**: Use WSL2

---

## 2. Dependency Installation

### Ubuntu/Debian

```bash
# Install build tools
sudo apt update
sudo apt install -y \
    build-essential \
    cmake \
    ninja-build \
    git \
    pkg-config

# Install Boost (required components)
sudo apt install -y \
    libboost-fiber-dev \
    libboost-context-dev \
    libboost-system-dev \
    libboost-thread-dev \
    libboost-chrono-dev

# Install xxhash
sudo apt install -y libxxhash-dev
```

### Verify Installation

```bash
g++ --version      # Should be 11+
cmake --version    # Should be 3.17+
ninja --version    # Should be 1.10+
pkg-config --modversion xxhash  # Should show version
```

---

## 3. Git Repository Initialization

### Clone the Repository

```bash
git clone git@github.com:idle-lab/idleKV.git
cd idleKV
```

### Initialize Submodules

The project uses git submodules for third-party dependencies (spdlog):

```bash
# Initialize submodules
git submodule update --init --recursive

# Or with depth=1 for faster clone
git clone --recurse-submodules --shallow-submodules git@github.com:idle-lab/idleKV.git
```

### Verify Submodules

```bash
ls -la third_part/
# Should show: abseil/  CLI11/  gtest/  mimalloc/  spdlog/
```

---

## 4. Build Options

### Quick Build

```bash
# Release build (default, optimized)
make release

# Debug build (with symbols, no optimization)
make debug

# Output binary: build/src/idlekv
```

### Manual CMake Build

```bash
cmake -DCMAKE_BUILD_TYPE=Release \
       -DCMAKE_EXPORT_COMPILE_COMMANDS=TRUE \
       -B build -S . -G Ninja

ninja idlekv -C build -j$(nproc)
```

### CMake Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `CMAKE_BUILD_TYPE` | String | `Debug` | Build type: `Debug` or `Release` |
| `ENABLE_TEST` | Bool | `ON` | Compile unit tests |
| `ENABLE_BENCHMARK` | Bool | `ON` | Compile benchmark tools |
| `DISABLE_CHECK` | Bool | `ON` | Disable runtime CHECK macros |
| `ENABLE_ASIO_IO_URING` | Bool | `OFF` | Use io_uring backend (requires kernel 5.19+) |
| `ENABLE_ASAN` | Bool | `OFF` | Enable AddressSanitizer |

### Build Examples

```bash
# Release build with io_uring (Linux, kernel 5.19+)
cmake -DCMAKE_BUILD_TYPE=Release \
       -DENABLE_ASIO_IO_URING=ON \
       -B build -S . -G Ninja
ninja idlekv -C build

# Debug build with AddressSanitizer
cmake -DCMAKE_BUILD_TYPE=Debug \
       -DENABLE_ASAN=ON \
       -B build -S . -G Ninja
ninja idlekv -C build

# Release build without tests/benchmarks
cmake -DCMAKE_BUILD_TYPE=Release \
       -DENABLE_TEST=OFF \
       -DENABLE_BENCHMARK=OFF \
       -B build -S . -G Ninja
ninja idlekv -C build
```

### Build Output

```
build/
├── src/
│   ├── idlekv              # Main binary
│   └── libidlekv_core.a    # Core library
├── benchmark/
│   └── index_bench         # Index benchmark
└── test/
    └── unit_test           # Unit tests
```

---

## 5. Running idleKV

### Basic Usage

```bash
# Default: listen on 0.0.0.0:4396
./build/src/idlekv

# Custom IP and port
./build/src/idlekv --ip 127.0.0.1 --port 6379

# Disable metrics endpoint
./build/src/idlekv --metrics-port 0

# Write logs to a file and force info-level logging
./build/src/idlekv --log-file /tmp/idlekv.log --log-level info
```

### Command-Line Options

| Option | Default | Description |
|--------|---------|-------------|
| `--ip` | `0.0.0.0` | Listen IP address |
| `--port` | `4396` | Redis protocol port |
| `--metrics-ip` | `0.0.0.0` | Prometheus metrics IP |
| `--metrics-port` | `9108` | Prometheus metrics port (0 to disable) |
| `--log-file` | - | Append logs to the specified file while keeping console output |
| `--log-level` | `debug` in debug builds, `info` in release builds | Log level override: `trace`, `debug`, `info`, `warn`, `error`, `critical`, `off` |
| `--DbNum` | `16` | Number of databases (SELECT command) |
| `-c, --config` | - | Config file path (not yet implemented) |

### Output Example

```
    ___      _ _         _  ____     __
   |_ _|  __| | |  ___  | |/ /\ \   / /
    | |  / _` | | / _ \ | ' /  \ \ / /
    | | | (_| | |/  __/ | . \   \ V /
   |___| \__,_|_|\___|  |_|\_\   \_/

    >> High Performance Key-Value Store <<

[2026-04-09 00:00:00.000] [info] you are running in release mode
[2026-04-09 00:00:00.000] [info] I/O backend: io_uring disabled, using reactor backend
[2026-04-09 00:00:00.000] [info] Running 6 io threads
[2026-04-09 00:00:00.000] [info] register handler: Redis
[2026-04-09 00:00:00.000] [info] register handler: Metrics
[2026-04-09 00:00:00.000] [info] start server
[2026-04-09 00:00:00.000] [info] start handler Redis, listening on 0.0.0.0:4396
[2026-04-09 00:00:00.000] [info] start handler Metrics, listening on 0.0.0.0:9108
```

### Redis Protocol Compatibility

Connect with any Redis client:

```bash
# Using redis-cli
redis-cli -p 4396 PING
# Response: +PONG

# Using netcat
echo -e '*1\r\n$4\r\nPING\r\n' | nc localhost 4396
# Response: +PONG

# Using Python
python3 -c "import redis; r = redis.Redis(port=4396); print(r.ping())"
# Response: True
```

### Prometheus Metrics

Metrics available at `--metrics-port` (default 9108):

```bash
curl http://localhost:9108/metrics
```

---

## 6. Testing

```bash
# Build and run unit tests
ninja unit_test -C build -j$(nproc)
./build/unit_test

# Run specific test
./build/unit_test --gtest_filter=TestSuiteName.TestName
```

---

## 7. Troubleshooting

### Boost Not Found

```bash
# Ubuntu 24.04
sudo apt install libboost1.83-dev

# Or specify Boost path manually
cmake -DBOOST_ROOT=/path/to/boost -B build -S . -G Ninja
```

### io_uring Not Available

If you see:
```
[warn] io_uring not available, falling back to epoll
```

This is normal on older kernels. For io_uring support:
- Requires Linux kernel 5.19+
- Enable with `-DENABLE_ASIO_IO_URING=ON` (automatically detected on supported kernels)

### Build Errors

```bash
# Clean and rebuild
make clean
make release
```
