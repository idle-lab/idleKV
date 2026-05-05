
### 启动 Redis

```
redis-server \
--bind 127.0.0.1 \
--port 6379 \
--save "" \
--appendonly no
```

### 启动 IdleKV

```
./build/src/idlekv \
--ip 127.0.0.1 \
--port 4396 \
--metrics-port 0
```


## Get/Set

### 读写 9：1

```
memtier_benchmark \
--server=127.0.0.1 \
--port=4396 \
--ratio=9:1 \
--clients=64 \
--pipeline=64 \
--key-minimum=1 \
--key-maximum=100000 \
--requests=500000 \
--json-out-file=out/get_set_benchmark/now/raw/idlekv-get-set-9-1.json \
--out-file=out/get_set_benchmark/now/raw/idlekv-get-set-9-1.txt
```

### 读写 1：1

```
memtier_benchmark \
--server=127.0.0.1 \
--port=4396 \
--ratio=1:1 \
--clients=64 \
--pipeline=64 \
--key-minimum=1 \
--key-maximum=1000000 \
--requests=500000 \
--json-out-file=out/get_set_benchmark/raw/idlekv-get-set-1-1.json \
--out-file=out/get_set_benchmark/raw/idlekv-get-set-1-1.txt
```

### 读写 1：9

```
memtier_benchmark \
--server=127.0.0.1 \
--port=4396 \
--ratio=1:9 \
--clients=64 \
--pipeline=64 \
--key-minimum=1 \
--key-maximum=1000000 \
--requests=500000 \
--json-out-file=out/get_set_benchmark/raw/idlekv-get-set-1-9.json \
--out-file=out/get_set_benchmark/raw/idlekv-get-set-1-9.txt
```


### 1GB数据

```
python3 scripts/load_kv_dataset.py \
--host 127.0.0.1 \
--port 6379 \
--target-bytes 1GB \
--flushdb
```

上面的命令显式指定了 `--target-bytes 1GB`。  
脚本默认值是精确 `1 GiB` 的 value 数据，value 大小默认 `1 KiB`，也就是大约 `1048576` 个 key。  
实际内存占用会比目标 value 数据更大，因为还包含 key、本体对象和哈希表元数据。

如果要写到 IdleKV，把端口改成 `4396`：

```
python3 scripts/load_kv_dataset.py \
--host 127.0.0.1 \
--port 4396 \
--target-bytes 1GB
```

如果你想改 value 大小或总数据量，例如写 `256 MiB`、每个 value `4 KiB`：

```
python3 scripts/load_kv_dataset.py \
--host 127.0.0.1 \
--port 6379 \
--target-bytes 256MiB \
--value-size 4KiB \
--flushdb
```

写完以后可以看 key 数和内存：

```
redis-cli -p 6379 DBSIZE
redis-cli -p 6379 INFO memory
```


## ZSet

默认以下命令以 IdleKV 为例，端口是 `4396`。  
如果要测 Redis，把 `--port=4396` 改成 `--port=6379` 即可。

### 单 key 建数

给 `zrange_head`、`zrange_mid`、`zrange_deep`、`zrange_head_withscores` 用。

```
memtier_benchmark \
--server=127.0.0.1 \
--port=4396 \
--threads=1 \
--clients=1 \
--pipeline=64 \
--requests=1000000 \
--key-prefix=zset:single:range: \
--key-minimum=1 \
--key-maximum=1 \
--command="ZADD __key__ 1 __data__" \
--data-size=32 \
--random-data
```

### 单 key 插入

```
memtier_benchmark \
--server=127.0.0.1 \
--port=4396 \
--threads=4 \
--clients=64 \
--pipeline=64 \
--requests=500000 \
--key-prefix=zset:single:insert: \
--key-minimum=1 \
--key-maximum=1 \
--command="ZADD __key__ 1 __data__" \
--data-size=32 \
--random-data
```

### 单 key 更新

这里用两个 `ZADD` 交替改同一个 member 的 score，避免退化成重复写同值。

```
memtier_benchmark \
--server=127.0.0.1 \
--port=4396 \
--threads=4 \
--clients=64 \
--pipeline=64 \
--requests=500000 \
--key-prefix=zset:single:update: \
--key-minimum=1 \
--key-maximum=1 \
--command="ZADD __key__ 1 member:fixed" \
--command-ratio=1 \
--command="ZADD __key__ 2 member:fixed" \
--command-ratio=1
```

### 单 key 头部范围查询

先执行一次“单 key 建数”，再跑：

```
memtier_benchmark \
--server=127.0.0.1 \
--port=4396 \
--threads=4 \
--clients=64 \
--pipeline=64 \
--requests=500000 \
--key-prefix=zset:single:range: \
--key-minimum=1 \
--key-maximum=1 \
--command="ZRANGE __key__ 0 9"
```

### 单 key 中段范围查询

先执行一次“单 key 建数”，再跑：

```
memtier_benchmark \
--server=127.0.0.1 \
--port=4396 \
--threads=4 \
--clients=64 \
--pipeline=64 \
--requests=500000 \
--key-prefix=zset:single:range: \
--key-minimum=1 \
--key-maximum=1 \
--command="ZRANGE __key__ 1000 1009"
```

### 单 key 深翻页范围查询

先执行一次“单 key 建数”，再跑：

```
memtier_benchmark \
--server=127.0.0.1 \
--port=4396 \
--threads=4 \
--clients=64 \
--pipeline=64 \
--requests=500000 \
--key-prefix=zset:single:range: \
--key-minimum=1 \
--key-maximum=1 \
--command="ZRANGE __key__ 100000 100009"
```

### 单 key 范围查询 WITHSCORES

先执行一次“单 key 建数”，再跑：

```
memtier_benchmark \
--server=127.0.0.1 \
--port=4396 \
--threads=4 \
--clients=64 \
--pipeline=64 \
--requests=500000 \
--key-prefix=zset:single:range: \
--key-minimum=1 \
--key-maximum=1 \
--command="ZRANGE __key__ 0 9 WITHSCORES"
```

### 多 key 建数

这里按 `4096` 个 key、每个 key `1024` 个 member 建数。  
`--command-key-pattern=S` 会顺序轮询 key 空间，配合 `--threads=1 --clients=1` 可以把数据均匀打到每个 key 上。

```
memtier_benchmark \
--server=127.0.0.1 \
--port=4396 \
--threads=1 \
--clients=1 \
--pipeline=64 \
--requests=4194304 \
--key-prefix=zset:multi:range: \
--key-minimum=1 \
--key-maximum=4096 \
--command="ZADD __key__ 1 __data__" \
--command-key-pattern=S \
--data-size=32 \
--random-data
```

### 多 key 插入

```
memtier_benchmark \
--server=127.0.0.1 \
--port=4396 \
--threads=4 \
--clients=64 \
--pipeline=64 \
--requests=500000 \
--key-prefix=zset:multi:insert: \
--key-minimum=1 \
--key-maximum=4096 \
--command="ZADD __key__ 1 __data__" \
--data-size=32 \
--random-data
```

### 多 key 更新

```
memtier_benchmark \
--server=127.0.0.1 \
--port=4396 \
--threads=4 \
--clients=64 \
--pipeline=64 \
--requests=500000 \
--key-prefix=zset:multi:update: \
--key-minimum=1 \
--key-maximum=4096 \
--command="ZADD __key__ 1 member:fixed" \
--command-ratio=1 \
--command="ZADD __key__ 2 member:fixed" \
--command-ratio=1
```

### 多 key 头部范围查询

先执行一次“多 key 建数”，再跑：

```
memtier_benchmark \
--server=127.0.0.1 \
--port=4396 \
--threads=4 \
--clients=64 \
--pipeline=64 \
--requests=500000 \
--key-prefix=zset:multi:range: \
--key-minimum=1 \
--key-maximum=4096 \
--command="ZRANGE __key__ 0 9"
```

### 多 key 中段范围查询

先执行一次“多 key 建数”，再跑：

```
memtier_benchmark \
--server=127.0.0.1 \
--port=4396 \
--threads=4 \
--clients=64 \
--pipeline=64 \
--requests=500000 \
--key-prefix=zset:multi:range: \
--key-minimum=1 \
--key-maximum=4096 \
--command="ZRANGE __key__ 1000 1009"
```

### 多 key 范围查询 WITHSCORES

先执行一次“多 key 建数”，再跑：

```
memtier_benchmark \
--server=127.0.0.1 \
--port=4396 \
--threads=4 \
--clients=64 \
--pipeline=64 \
--requests=500000 \
--key-prefix=zset:multi:range: \
--key-minimum=1 \
--key-maximum=4096 \
--command="ZRANGE __key__ 0 9 WITHSCORES"
```

### 多 key 深翻页范围查询

如果坚持按 `4096` 个 key 跑 `zrange_deep`，每个 key 至少要先建到 `100010` 个 member。  
对应建数命令如下，体量很大，默认不建议直接跑：

```
memtier_benchmark \
--server=127.0.0.1 \
--port=4396 \
--threads=1 \
--clients=1 \
--pipeline=64 \
--requests=409640960 \
--key-prefix=zset:multi:range-deep: \
--key-minimum=1 \
--key-maximum=4096 \
--command="ZADD __key__ 1 __data__" \
--command-key-pattern=S \
--data-size=32 \
--random-data
```

建数完成后，再跑：

```
memtier_benchmark \
--server=127.0.0.1 \
--port=4396 \
--threads=4 \
--clients=64 \
--pipeline=64 \
--requests=500000 \
--key-prefix=zset:multi:range-deep: \
--key-minimum=1 \
--key-maximum=4096 \
--command="ZRANGE __key__ 100000 100009"
```

### 注意

同一类测试重复执行前，最好换一个新的 `--key-prefix`，或者先清理旧数据。  
`__data__` 生成的是 benchmark 用 member 数据，不保证人眼可读，但适合做 `zadd_insert` 和建数。
