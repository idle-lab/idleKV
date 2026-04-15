# Redis vs IdleKV ZSet Benchmark Plan

## 目标

建立一套可以稳定复现的对比方案，评估 `Redis` 和 `IdleKV` 在以下维度上的差异：

- `ZADD` 吞吐
- `ZRANGE` 吞吐
- 内存占用

本方案优先覆盖当前 IdleKV 已实现的基础语义：

- `ZADD key score member [score member ...]`
- `ZRANGE key start stop [WITHSCORES]`

不包含 Redis 额外选项和扩展语义，例如 `NX` / `XX` / `GT` / `LT` / `REV` / `BYSCORE`。

## 前置条件

截至 `2026-04-14`，这套基准所需的两个基础能力已经在仓库中具备：

- IdleKV 已支持 `INFO memory`，可返回：
  - `used_memory`
  - `used_memory_peak`
  - `used_memory_rss`
  - `mem_allocator`
  - `idlekv_memory_accounting`
  - `idlekv_shard_num`
  - `idlekv_shard_<n>_used_memory`
  - `idlekv_shard_<n>_used_memory_peak`
- 仓库中已有统一的 RESP 基准脚本：
  - `scripts/resp_zset_bench.go`
  - 支持 `--mode memory`
  - 支持 `--mode bench`
  - 支持 CSV 追加导出

这意味着：

- `ZSet 内存对比基准` 不再缺少服务端逻辑内存口径。
- 当前工作的重点，已经从“先补观测能力”转为“固化执行流程、跑完整矩阵、整理结果”。
- RSS 仍然要采样，但它现在是辅助指标，不再是唯一口径。

## 脚本模式说明

`scripts/resp_zset_bench.go` 有两个模式，它们不是“快/慢两种跑法”，而是服务于两类不同结果。

### `--mode memory`

用途：测内存，不测吞吐。

执行流程：

1. 采样一次空实例 baseline memory。
2. 按当前 workload preload 全量数据集。
3. 可选执行 `--verify-correctness` 语义校验。
4. 静置一段时间。
5. 连续采样 `INFO memory`，取中位数。

输出重点：

- `used_memory_baseline`
- `used_memory_after`
- `used_memory_delta`
- `bytes_per_member`
- `used_memory_rss`
- 可选的 `/proc` RSS

适用场景：

- 做 `bytes/member` 对比
- 看 preload 后的逻辑内存成本
- 验证 `INFO memory` 观测链路

不会做的事：

- 不会进入定时压测循环
- 不会产出 `ops/s`、`p95`、`p99`

### `--mode bench`

用途：测吞吐和延迟，不测内存差值。

执行流程：

1. 如果当前 op 需要已有数据，例如 `zadd_update` / `zrange_*`，先 preload 数据。
2. 可选执行 `--verify-correctness` 语义校验。
3. warmup。
4. 在固定 `duration` 内持续发请求。
5. 汇总吞吐、流量和延迟指标。

输出重点：

- `ops_per_sec`
- `elements_per_sec`
- `avg_us`
- `p50_us`
- `p95_us`
- `p99_us`
- `total_errors`

适用场景：

- 跑 `ZADD` / `ZRANGE` 性能矩阵
- 比较不同 `clients` / `pipeline` 下的扩展性
- 观察 `head` / `mid` / `deep` 的读取退化曲线

不会做的事：

- 不会计算 `used_memory_delta`
- CSV 里的内存列会留空

### 选择规则

- 要测 `bytes/member`，用 `--mode memory`
- 要测 `ops/s` / `p99`，用 `--mode bench`
- 要做 smoke test，可以优先用 `--mode memory --verify-correctness`
- 正式跑吞吐矩阵时，用 `--mode bench`，通常不要开 `--verify-correctness`

## 对比原则

- 同一台机器、同一操作系统、同一 CPU 拓扑。
- Redis 和 IdleKV 均使用 loopback 网络，避免外部网络抖动。
- Redis 关闭持久化，避免磁盘路径影响基准：
  - `save ""`
  - `appendonly no`
- IdleKV 固定 shard 数，并在报告中记录配置。
- 每轮测试前都重启服务，避免 allocator 污染上一次结果。
- 每组结果至少执行 `3` 次，取中位数。
- 除 `ops/s` 外，`ZRANGE` 额外记录 `elements/s`，否则不同返回长度下的吞吐无法横向比较。

## Workload 设计

### 1. 单 key / 大 ZSet

目的：评估单个大型有序集合的数据结构访问成本，尽量弱化分片并行收益。

建议数据规模：

- `1 key`
- `1,000,000 members`

示例 key：

- `bench:zset:single`

适合观测：

- `ZADD` 插入成本
- `ZADD` 更新已有 member 的成本
- `ZRANGE` 在不同 offset 下的顺序访问成本
- 单个大型 ZSet 的 `bytes/member`

### 2. 多 key / 分布式 ZSet

目的：评估服务整体吞吐，并观察 IdleKV 多 shard 设计在更真实 key 分布下的收益。

建议数据规模：

- `4096 keys`
- 每个 key `1024 members`

示例 key：

- `bench:zset:{0000}` 到 `bench:zset:{4095}`

适合观测：

- 多连接并发下的 `ZADD` / `ZRANGE` 系统吞吐
- key 分布更均匀时的 shard 并行收益
- 多个中等大小 ZSet 的总内存占用

## 基准项

### ZADD

`ZADD` 至少拆成两类：

#### 1. Insert Throughput

持续插入新的 member。

目的：

- 观察成员索引插入路径的真实成本
- 观察 score tree 与 member map 双写带来的开销

示例命令：

```text
ZADD bench:zset:single 1 member:00000001
ZADD bench:zset:single 2 member:00000002
```

#### 2. Update Throughput

预先装载数据后，仅更新已存在 member 的 score。

目的：

- 观察已有 member 更新时的 remove + insert 路径成本
- 避免只测 append-only 场景

示例命令：

```text
ZADD bench:zset:single 1001 member:00000001
ZADD bench:zset:single 1002 member:00000002
```

### ZRANGE

`ZRANGE` 必须固定返回条数，否则 `ops/s` 结果不可比。

建议至少覆盖：

#### 1. Head Scan

```text
ZRANGE bench:zset:single 0 9
```

目的：

- 观察小 offset 下的范围读取吞吐

#### 2. Mid Scan

```text
ZRANGE bench:zset:single 1000 1009
```

目的：

- 观察中等 offset 下的读取吞吐
- 验证 offset 增大后是否有明显性能退化

#### 3. Deep Scan

```text
ZRANGE bench:zset:single 100000 100009
```

目的：

- 观察大 offset 场景
- 对比 IdleKV 的按 rank 访问路径是否稳定

#### 4. WITHSCORES

对以上至少挑一组再测一遍：

```text
ZRANGE bench:zset:single 0 9 WITHSCORES
```

目的：

- 观察返回 payload 翻倍后的网络和序列化成本

## 并发矩阵

建议测试矩阵：

- `clients = 1, 8, 32, 64`
- 默认候选 `pipeline = 1, 16, 64`

为了减少 case 数量，默认跑批脚本会对小 `clients` 的组合做裁剪：

- `clients = 1`:
  - 仅保留 `pipeline = 64`
- `clients = 8`:
  - 仅保留 `pipeline = 16, 64`
- `clients = 32, 64`:
  - 保留 `pipeline = 1, 16, 64`

说明：

- 默认矩阵不再覆盖 `clients=1, pipeline=1`。
- 如果需要观察纯单连接延迟和单线程路径，可以单独手工补跑这一组。
- 中等并发可以观察实现的扩展曲线。
- 较高 pipeline 能减少 syscall 和 RTT 干扰，更贴近服务端上限吞吐。

## 数据生成规则

为了保证 Redis 和 IdleKV 接收到的负载完全一致，客户端应统一生成：

- 相同的 key 集合
- 相同的 member 集合
- 相同的 score 分布
- 相同的请求顺序

建议规则：

- member 命名固定长度，例如 `member:00000001`
- score 使用单调整数或可重复生成的伪随机数
- update 场景使用同一批 member，只改变 score
- key 分布固定，不随服务端类型切换

## 计量指标

### 吞吐

- `ops/s`
- `elements/s`

说明：

- `ZADD` 的 `elements/s` 与 `ops/s` 相同，除非单次命令携带多个成员。
- `ZRANGE` 的 `elements/s = ops/s * returned_members_per_op`。

### 延迟

- `avg`
- `p50`
- `p95`
- `p99`

### 内存

建议同时记录两类口径。

#### 1. 服务端逻辑内存

用于做主结论。

- Redis：`INFO memory`
- IdleKV：Redis 风格内存命令返回的内部统计值

目标指标：

- `used_memory`
- `bytes_per_member`

其中：

```text
bytes_per_member = (used_memory_after_load - used_memory_baseline) / total_members
```

#### 2. 进程物理占用

用于辅助解释 allocator 行为，不作为主结论。

- 基准脚本当前采样 `/proc/<pid>/status` 中的 `VmRSS`
- 如果出现异常 case，再额外留存 `/proc/<pid>/smaps_rollup` 作为人工分析材料

## 内存采样流程

对每一种数据集都执行如下流程：

1. 启动空实例。
2. 等待服务完成初始化。
3. 采集一次 baseline memory。
4. 批量装载固定数据集。
5. 静置 `10s`，让异步释放和分配器缓存趋于稳定。
6. 连续采样 `5` 次，取中位数作为 `after_load`。
7. 计算：

```text
used_memory_delta = after_load - baseline
bytes_per_member  = used_memory_delta / total_members
```

补充说明：

- `used_memory` 作为主结论口径。
- `used_memory_rss` 和 `/proc/<pid>/status` 的 `VmRSS` 用于解释 allocator cache、线程栈和运行时额外开销。
- 如果不传 `--pid`，脚本仍可完成 `INFO memory` 采样，只是不会补充 `/proc` 口径。

## 推荐执行流程

每个 benchmark case 使用如下节奏：

- warmup: `10s`
- measure: `30s`
- repeat: `3 runs`

每轮之间：

- 清空数据
- 重启服务
- 再次装载 workload

## 结果表建议

建议统一导出 CSV，字段至少包括：

```text
server,workload,op,clients,pipeline,withscores,key_count,members_per_key,total_members,
returned_members,run,ops_per_sec,elements_per_sec,avg_us,p50_us,p95_us,p99_us,
used_memory_baseline,used_memory_after,used_memory_delta,bytes_per_member,rss_baseline,rss_after
```

## 基准工具策略

建议继续使用仓库内的统一 RESP benchmark client，而不是回退到 `redis-benchmark`。

当前推荐入口：

- `go run ./scripts/resp_zset_bench.go`
- `scripts/run_zset_benchmark_matrix.sh`
- `scripts/plot_zset_results.py`

原因：

- `redis-benchmark` 不擅长维护完整的数据装载、预热、内存采样和 CSV 导出流程。
- 当前脚本已经内建了：
  - 固定 workload 默认值
  - preload 流程
  - warmup / measure 节奏
  - `INFO memory` 采样
  - CSV 字段对齐
- Redis 与 IdleKV 共用同一个 client，能最大程度减少客户端实现差异。

当前脚本支持的 benchmark op：

- `zadd_insert`
- `zadd_update`
- `zrange_head`
- `zrange_mid`
- `zrange_deep`
- `zrange_head_withscores`

默认 workload 参数：

- `single`:
  - `key_count = 1`
  - `members_per_key = 1,000,000`
- `multi`:
  - `key_count = 4096`
  - `members_per_key = 1024`

## 一键执行脚本

为了避免手工拼接几百个 case，仓库中补充了两个配套脚本：

### 1. 跑完整矩阵

```bash
scripts/run_zset_benchmark_matrix.sh
```

默认行为：

- 依次跑 Redis 和 IdleKV
- 依次跑 `memory` 和 `bench` 矩阵
- 每个 case 前启动目标服务
- case 结束后立刻停止该服务
- 原始结果输出到：
  - `out/zset_benchmark/<run-name>/raw/bench.csv`
  - `out/zset_benchmark/<run-name>/raw/memory.csv`
- 服务端日志和 client 日志输出到：
  - `out/zset_benchmark/<run-name>/logs/`
- 跑完后自动调用画图脚本

常用参数：

- `--run-name <name>`
- `--bench-duration 30s`
- `--memory-runs 3`
- `--bench-runs 3`
- `--verify-correctness`
- `--skip-plot`
- `--single-members <n>`
- `--multi-keys <n>`
- `--multi-members <n>`

示例：

```bash
scripts/run_zset_benchmark_matrix.sh \
  --run-name zset-full \
  --bench-duration 30s \
  --memory-runs 3 \
  --bench-runs 3
```

### 2. 基于已有 CSV 单独出图

```bash
python3 scripts/plot_zset_results.py \
  --bench-csv out/zset_benchmark/<run-name>/raw/bench.csv \
  --memory-csv out/zset_benchmark/<run-name>/raw/memory.csv \
  --out-dir out/zset_benchmark/<run-name>
```

默认输出：

- summary CSV:
  - `out/zset_benchmark/<run-name>/summary/bench_medians.csv`
  - `out/zset_benchmark/<run-name>/summary/memory_medians.csv`
- charts:
  - `out/zset_benchmark/<run-name>/charts/memory_overview.png`
  - `out/zset_benchmark/<run-name>/charts/single_ops_per_sec.png`
  - `out/zset_benchmark/<run-name>/charts/single_p99_us.png`
  - `out/zset_benchmark/<run-name>/charts/multi_ops_per_sec.png`
  - `out/zset_benchmark/<run-name>/charts/multi_p99_us.png`

## 当前实现状态

### 已完成

- IdleKV `INFO memory` 已可用于基准采样。
- `scripts/resp_zset_bench.go` 已覆盖本文要求的主要 workload、操作类型和 CSV 导出。
- 脚本支持：
  - `--mode memory`
  - `--mode bench`
  - `--server`
  - `--host`
  - `--port`
  - `--pid`
  - `--workload`
  - `--op`
  - `--clients`
  - `--pipeline`
  - `--csv-out`
  - `--run-id`
  - `--verify-correctness`

### 仍需注意

- `resp_zset_bench.go` 当前对 benchmark 响应的校验重点是：
  - 默认模式下会校验 RESP 类型和返回长度。
  - 开启 `--verify-correctness` 后，会进一步校验：
    - preload 后抽样 `ZRANGE` 返回的 member 和 score 内容
    - benchmark 过程中 `ZADD` 的返回整数值
    - benchmark 过程中 `ZRANGE` 返回数组中的 member / score 内容
- `--verify-correctness` 会增加额外请求和更重的响应校验。
- 因此，建议仅在 smoke test、小数据集调试或回归验证时开启，正式跑完整吞吐矩阵时默认关闭。

## 执行前检查

正式跑基准前，先完成以下检查：

1. Redis 使用无持久化配置启动：
   - `save ""`
   - `appendonly no`
2. IdleKV 使用固定端口启动，并记录构建方式、CPU 核数和实例 PID。
3. 用 `redis-cli` 手工确认 IdleKV 的 `INFO memory` 可读：

```bash
redis-cli -p 4396 INFO memory
```

4. 确认 `INFO memory` 至少包含以下字段：
   - `used_memory`
   - `used_memory_peak`
   - `used_memory_rss`
   - `idlekv_memory_accounting`
   - `idlekv_shard_num`
5. 跑一个小数据集 smoke test，自动确认基础语义正确：

```bash
go run ./scripts/resp_zset_bench.go \
  --mode memory \
  --server idlekv-smoke \
  --host 127.0.0.1 \
  --port 4396 \
  --workload single \
  --members-per-key 1024 \
  --memory-samples 3 \
  --verify-correctness
```

## 推荐命令模板

### 1. 启动 Redis

```bash
redis-server \
  --bind 127.0.0.1 \
  --port 6379 \
  --save "" \
  --appendonly no
```

### 2. 启动 IdleKV

```bash
./build/src/idlekv \
  --ip 127.0.0.1 \
  --port 4396 \
  --metrics-port 0
```

### 3. 采集单 key workload 的内存结果

Redis:

```bash
go run ./scripts/resp_zset_bench.go \
  --mode memory \
  --server redis \
  --host 127.0.0.1 \
  --port 6379 \
  --pid <redis_pid> \
  --workload single \
  --csv-out out/zset_memory.csv \
  --run-id redis-single-memory-r1
```

IdleKV:

```bash
go run ./scripts/resp_zset_bench.go \
  --mode memory \
  --server idlekv \
  --host 127.0.0.1 \
  --port 4396 \
  --pid <idlekv_pid> \
  --workload single \
  --csv-out out/zset_memory.csv \
  --run-id idlekv-single-memory-r1
```

### 4. 采集吞吐结果

示例：`single + zrange_head + clients=32 + pipeline=16`

```bash
go run ./scripts/resp_zset_bench.go \
  --mode bench \
  --server idlekv \
  --host 127.0.0.1 \
  --port 4396 \
  --workload single \
  --op zrange_head \
  --clients 32 \
  --pipeline 16 \
  --duration 30s \
  --warmup 10s \
  --csv-out out/zset_bench.csv \
  --run-id idlekv-single-zrange_head-c32-p16-r1
```

### 5. 覆盖完整矩阵

建议用以下精简矩阵展开：

- workload:
  - `single`
  - `multi`
- op:
  - `zadd_insert`
  - `zadd_update`
  - `zrange_head`
  - `zrange_mid`
  - `zrange_deep`
  - `zrange_head_withscores`
- clients:
  - `1`
  - `8`
  - `32`
  - `64`
- pipeline:
  - `clients=1` 时只跑 `64`
  - `clients=8` 时跑 `16, 64`
  - `clients=32, 64` 时跑 `1, 16, 64`
- run:
  - `1`
  - `2`
  - `3`

## 推荐执行顺序

为了减少长时间跑批后才发现配置错误，建议按下面顺序推进：

### Phase 0: 语义和观测烟测

- 小数据量跑通 `single` workload。
- 确认 `INFO memory` 正常返回。
- 手工抽样校验 `ZRANGE ... WITHSCORES` 结果。

退出条件：

- Redis 和 IdleKV 都能稳定返回 `INFO memory`。
- benchmark 脚本在 `memory` 和 `bench` 模式都能跑通。

### Phase 1: 内存基线

- 分别采集 `single` 和 `multi` workload 的内存数据。
- 每种 workload 对 Redis 和 IdleKV 都至少跑 `3` 次。

退出条件：

- 成功产出 `used_memory_delta` 和 `bytes_per_member`。
- 能解释 `used_memory` 与 RSS 的差异来源。

### Phase 2: 吞吐矩阵

- 对两种 workload 跑完整 op / clients / pipeline 矩阵。
- 统一导出到同一份 CSV。

退出条件：

- 每个 case 都有 `3` 次有效结果。
- 无批量错误日志刷屏。
- `errors = 0` 或已单独解释。

### Phase 3: 结果整理

- 以中位数汇总每个 case。
- 输出最终对比表和图。
- 标注所有环境参数和 caveat。

退出条件：

- 能直接回答：
  - IdleKV 在哪些 workload 上明显领先 Redis
  - 领先主要来自并行吞吐还是数据结构本身
  - 内存成本是否以吞吐收益为代价

## 结果判读建议

重点看四组结论：

1. 单 key 场景下，IdleKV 是否仍有优势。
   - 这一组更接近“单个大型 ZSet 实现本身”的对比。
2. 多 key 场景下，IdleKV 优势是否明显扩大。
   - 这一组更能体现 shared-nothing 多 shard 并行能力。
3. `ZRANGE` 从 `head` 到 `deep` 的退化曲线是否平滑。
   - 如果 deep scan 明显掉速，需要单独分析 rank 访问路径。
4. `used_memory` 与 `bytes/member` 是否可接受。
   - 如果吞吐提升显著但 `bytes/member` 也显著抬升，需要把这个 tradeoff 写进结论。

补充判读口径：

- `used_memory` 是主结论。
- `used_memory_rss` 和 `VmRSS` 用于解释 allocator 行为。
- 如果 `used_memory` 很低但 RSS 很高，不应直接得出“内存占用更差”的结论，必须注明这是逻辑内存与物理驻留页的差异。

## 风险与后续增强

当前计划还有几项明确的后续增强点：

- 为 `resp_zset_bench.go` 增加更强的结果校验：
  - 小数据量模式下逐项校验 `ZRANGE` 返回内容
  - 对 `WITHSCORES` 校验 score 字符串格式
- 增加自动化跑批脚本：
  - 自动重启服务
  - 自动遍历矩阵
  - 自动写入 `run-id`
- 增加结果汇总脚本：
  - 从原始 CSV 聚合中位数
  - 生成 Markdown 表格或图表输入
- 后续如果需要做更细粒度归因，再考虑：
  - `MEMORY STATS`
  - `MEMORY USAGE key`
  - perf / flamegraph / allocator profiling

## 结论

截至 `2026-04-14`，本计划的基础设施阶段已经基本完成：

1. IdleKV 已支持 `INFO memory`。
2. 仓库中已具备 `scripts/resp_zset_bench.go` 作为统一 benchmark client。

因此，接下来的实际优先级应为：

1. 先做语义 smoke test，确认 benchmark driver 与服务端观测链路稳定。
2. 按本文矩阵跑完 Redis / IdleKV 的 memory 和 throughput case。
3. 汇总 CSV，输出中位数结果和结论说明。
