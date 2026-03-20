# Index Benchmark

这个目录下提供了 `idlekv` 当前索引实现的单线程基准程序，当前支持：

- `art`
- `std::unordered_map`
- `absl::flat_hash_map`

重点关注：

- 吞吐量：`QPS`
- 延迟：`avg / p50 / p95 / p99 / p999 / max`
- 内存：`live / peak / bytes per key / amplification / RSS`
- 其他指标：`MiB/s`、`alloc_calls/op`

## 构建

```bash
cmake -S . -B build -DENABLE_BENCHMARK=ON
cmake --build build --target index_bench -j
```

## 运行

```bash
./build/benchmark/index_bench --keys 200000 --ops 200000
```

也可以只跑部分 key 分布：

```bash
./build/benchmark/index_bench --datasets shared_prefix
./build/benchmark/index_bench --datasets wide_fanout,mixed
```

也可以只跑部分索引，并导出 CSV：

```bash
./build/benchmark/index_bench \
  --indexes art,std_unordered_map,absl_flat_hash_map \
  --csv-out benchmark/results/index_compare.csv
```

## 画图

```bash
MPLCONFIGDIR=/tmp/matplotlib-cache \
python3 benchmark/plot_index_bench.py \
  --input benchmark/results/index_compare.csv \
  --output-dir benchmark/results/plots
```

默认会生成：

- `qps_by_operation.png`
- `p99_latency_by_operation.png`
- `bytes_per_key.png`
- `amplification.png`
- `alloc_calls_per_op.png`

## Key 分布

- `shared_prefix`
  大量 key 共享长前缀，更容易触发 ART 的路径压缩优势。
- `wide_fanout`
  前缀分散，更多考验高扇出节点访问。
- `mixed`
  混合 shared-prefix、wide-fanout 和层级型 key，更接近通用索引场景。

## Workload

- `InsertUnique`
- `LookupHit`
- `LookupMiss`
- `UpsertHit`
- `EraseHit`
- `MixedReadWrite`

`MixedReadWrite` 默认组合了命中查找、未命中查找、upsert、insert、erase，会在输出里额外打印实际操作分布。

对 hash map 类索引，bulk load 场景下会先 `reserve(key_count)`，避免把多次 rehash 成本混进主要 workload。

## 指标说明

- `live(start/end/peak)`
  通过自定义 `pmr::memory_resource` 统计的索引逻辑存活字节数和峰值。
- `rss(before/setup/after)`
  进程 RSS 的近似观测值，受分配器缓存影响，适合作为辅助指标，不建议单独用它判断索引真实占用。
- `bytes/key`
  当前逻辑存活字节数除以当前 resident key 数。
- `amplification`
  当前逻辑存活字节数与原始 `(key bytes + value bytes)` 负载的比值。
- `alloc_calls/op`
  只统计 benchmark phase 本身，每个操作平均触发的索引分配次数。
- `phase_allocated`
  只统计 benchmark phase 本身累计向索引请求过的字节数。
- `bytes_per_key / amplification`
  这些指标对三种索引都统一从导出的 CSV 里计算，便于直接横向比较。

## 注意

- 这是单线程 benchmark，主要用于看索引结构本身的访问成本。
- value 类型固定为 `uint64_t`，目的是尽量减少大 value 对索引测试结果的干扰。
- 如果你想测更贴近业务的数据形态，可以直接在 `index_bench.cc` 里扩展 dataset generator。 
