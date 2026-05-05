# Validate Strict Serializability with Elle

This repository now includes a small Elle wrapper for checking transactional histories emitted by idleKV clients.

## Files

- `scripts/gen_history.py`: Generates concurrent JSONL histories by issuing Redis commands directly.
- `scripts/elle_check.py`: Converts JSONL histories to Jepsen/Elle EDN and invokes the checker.
- `scripts/elle/deps.edn`: Local Clojure dependency definition for Elle.
- `scripts/elle/src/idlekv/elle_check.clj`: Minimal `elle.rw-register/check` entry point.
- `scripts/elle/examples/*.jsonl`: Small example histories.

## Why `elle.rw-register`

idleKV currently exposes Redis-style read/write commands over key-value registers. For black-box checking, the most direct Elle model is `elle.rw-register`.

Recommended micro-op mapping:

- `SET k v` -> `{"type":"w","key":"k","value":"v"}`
- `GET k -> v` -> `{"type":"r","key":"k","value":"v"}`
- `MSET k1 v1 k2 v2` -> multiple write micro-ops in one transaction
- `MGET k1 k2` -> multiple read micro-ops in one transaction

`DEL` is tricky with `elle.rw-register`: repeated deletes all move a key to the
same logical "absent" state, but the rw-register model assumes writes are
distinguishable. The current wrapper handles this as follows:

- generated `DEL` requests are restricted to a single key
- `DEL key -> 1` is encoded as a unique tombstone write for that key
- `DEL key -> 0` is encoded as a read of `nil` for that key
- `GET/MGET -> nil` is rewritten to the latest completed delete tombstone for
  that key when realtime order makes that source unambiguous; otherwise it stays
  as the initial-state `nil`

Because of that, `scripts/gen_history.py` still does not include `DEL` in its
default mix, but explicit `DEL` traffic is now directly checkable.

## Input Format

The wrapper expects one JSON object per line:

```json
{"process":0,"txn":"t1","start_ns":100,"end_ns":150,"status":"ok","ops":[{"type":"w","key":"x","value":"1"},{"type":"r","key":"y","value":"1"}]}
```

Fields:

- `process`: Logical client id. Reuse the same id for the same client session.
- `txn` or `txn_id`: Optional label, used only for debugging output.
- `start_ns`: Client-side invoke timestamp.
- `end_ns`: Client-side completion timestamp.
- `status`: `ok`, `fail`, or `info`.
- `ops`: Micro-ops inside the transaction. Each op must have `type`, `key`, and optionally `value`.

Supported `type` values:

- Reads: `r`, `read`, `get`, `mget`
- Writes: `w`, `write`, `set`, `mset`, `del`, `delete`

For `del` / `delete`, the `value` field is optional. If omitted, the wrapper
assigns a unique tombstone value automatically.

## Timestamp Requirements

Strict serializability depends on real-time order, so the timestamps must be usable for cross-transaction ordering.

- Best option: run all workload clients on one host and record `CLOCK_MONOTONIC` or `CLOCK_MONOTONIC_RAW`.
- If you use multiple hosts, their clocks must be tightly synchronized. Otherwise Elle may miss or invent realtime edges.

## Usage

Generate a workload history first:

```bash
python3 scripts/gen_history.py \
  --host 127.0.0.1 \
  --port 4396 \
  --clients 8 \
  --ops-per-client 200 \
  --key-space 8 \
  --flushdb \
  --out your_history.jsonl
```

If you still want DEL traffic in the generated workload, opt in explicitly:

```bash
python3 scripts/gen_history.py \
  --host 127.0.0.1 \
  --port 4396 \
  --allow-del \
  --mix 'set=25,get=25,mset=15,mget=15,del=20' \
  --out your_history_with_del.jsonl
```

Then check it with Elle:

```bash
python3 scripts/elle_check.py your_history.jsonl --runner maven
```

If you already have a local `clojure` command:

```bash
python3 scripts/elle_check.py scripts/elle/examples/strict_ok.jsonl --runner local
```

If you do not have Clojure locally but you do have Java + Maven:

```bash
python3 scripts/elle_check.py scripts/elle/examples/strict_ok.jsonl --runner maven
```

If you prefer Docker:

```bash
python3 scripts/elle_check.py scripts/elle/examples/strict_ok.jsonl --runner docker
```

To keep the generated EDN and write anomaly plots:

```bash
python3 scripts/elle_check.py workload.jsonl \
  --runner docker \
  --edn-out /tmp/workload.edn \
  --graphs-dir /tmp/elle-out
```

The default model is `strict-serializable`. You can override it:

```bash
python3 scripts/elle_check.py workload.jsonl --model serializable
```

## Expected Outcomes

- `scripts/elle/examples/strict_ok.jsonl` should pass.
- `scripts/elle/examples/serializability_violation.jsonl` should fail with a dependency cycle.

## Notes

- Elle is sound but incomplete. Passing histories do not prove the system is correct for all executions.
- `elle.rw-register` is the right baseline for idleKV today. If you later add richer black-box observability, you can consider stronger modeling.
