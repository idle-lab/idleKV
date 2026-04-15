


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
--key-maximum=1000000 \
--requests=500000
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
--requests=500000
```

### 读写 1：3

```
memtier_benchmark \
--server=127.0.0.1 \
--port=4396 \
--ratio=1:3 \
--clients=64 \
--pipeline=64 \
--key-minimum=1 \
--key-maximum=1000000 \
--requests=500000
```

## ZSet

