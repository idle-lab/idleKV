# Index Benchmark

`benchmark/index_bench` is a reusable harness for measuring index implementations in idleKV.

It currently registers:

- `dash`: `idlekv::dash::DashEH<uint64_t, uint64_t>`

It reports:

- load/read/mixed throughput
- sampled latency percentiles (`p50`, `p95`, `p99`, `max`)
- current and peak RSS during each phase
- bytes per resident entry
- index-specific metrics such as DASH split/merge counters

## Build

```bash
cmake -S . -B build -DENABLE_BENCHMARK=ON -DENABLE_TEST=OFF -DENABLE_ASIO_IO_URING=OFF
cmake --build build --target index_bench -j
```

## Examples

Single-thread latency-oriented run:

```bash
./build/benchmark/index_bench --index dash --threads 1 --initial-keys 200000 --operations 200000
```

Multi-thread throughput run:

```bash
./build/benchmark/index_bench --index dash --threads 8 --initial-keys 1000000 --operations 2000000
```

CSV output for scripts:

```bash
./build/benchmark/index_bench --index dash --format csv
```

## Workload Model

- `load`: bulk upsert of `initial-keys` sequential keys
- `read`: random lookups against the preloaded stable key range with configurable miss ratio
- `mixed`: YCSB-like mix of `read`, `upsert`, and `erase`

For `read` and `mixed`, the benchmark preloads the index first. `all` runs `load`, `read`, and `mixed` sequentially on the same index instance.

## Extending To New Indexes

To benchmark a new index implementation:

1. Add a new adapter class in `benchmark/index_bench.cc` that implements the `IIndex` interface.
2. Register it in `build_registry()`.
3. Rebuild `index_bench`.

The adapter only needs to implement:

- `upsert(key, value)`
- `find(key)`
- `erase(key)`
- `size()`
- optional `metrics()` for index-specific counters

For reliable memory comparisons, run one index per process instead of comparing multiple implementations inside the same process.
