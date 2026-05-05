#!/usr/bin/env python3
"""
Generate a transactional JSONL history for Elle from a Redis-compatible server.

The workload uses multiple concurrent TCP connections. Each external command is
recorded as one JSON object compatible with scripts/elle_check.py.
"""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
import json
from pathlib import Path
import random
import time
from typing import Any


DEFAULT_MIX = "set=35,get=35,mset=15,mget=15"
SUPPORTED_COMMANDS = ("set", "get", "mset", "mget", "del")


class RespError(RuntimeError):
    pass


@dataclass(frozen=True)
class WeightedCommand:
    name: str
    cumulative_weight: int


@dataclass(frozen=True)
class CommandPlan:
    kind: str
    parts: tuple[bytes, ...]
    keys: tuple[str, ...]
    values: tuple[str, ...]


@dataclass
class RunState:
    total_ops: int
    completed_ops: int = 0
    start_time: float = 0.0


class HistoryWriter:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = path.open("w", encoding="utf-8", buffering=1)
        self._lock = asyncio.Lock()

    async def write_record(self, record: dict[str, Any]) -> None:
        line = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
        async with self._lock:
            self._fh.write(line)
            self._fh.write("\n")
            self._fh.flush()

    def close(self) -> None:
        self._fh.close()


def build_resp_command(*parts: bytes) -> bytes:
    chunks = [f"*{len(parts)}\r\n".encode("ascii")]
    for part in parts:
        chunks.append(f"${len(part)}\r\n".encode("ascii"))
        chunks.append(part)
        chunks.append(b"\r\n")
    return b"".join(chunks)


def build_ping_request(message: str | None) -> tuple[bytes, bytes]:
    if message is None:
        return build_resp_command(b"PING"), b"+PONG\r\n"

    payload = message.encode("utf-8")
    return build_resp_command(b"PING", payload), b"+" + payload + b"\r\n"


def parse_mix(text: str) -> tuple[WeightedCommand, ...]:
    cumulative = 0
    choices: list[WeightedCommand] = []

    for raw_part in text.split(","):
        part = raw_part.strip()
        if not part:
            continue
        if "=" not in part:
            raise argparse.ArgumentTypeError(
                f"invalid --mix item {part!r}; expected name=weight"
            )

        raw_name, raw_weight = part.split("=", 1)
        name = raw_name.strip().lower()
        if name not in SUPPORTED_COMMANDS:
            raise argparse.ArgumentTypeError(
                f"unsupported command {name!r}; supported: {', '.join(SUPPORTED_COMMANDS)}"
            )

        try:
            weight = int(raw_weight)
        except ValueError as exc:
            raise argparse.ArgumentTypeError(
                f"invalid weight {raw_weight!r} for {name!r}"
            ) from exc

        if weight < 0:
            raise argparse.ArgumentTypeError(
                f"weight for {name!r} must be non-negative"
            )
        if weight == 0:
            continue

        cumulative += weight
        choices.append(WeightedCommand(name=name, cumulative_weight=cumulative))

    if not choices:
        raise argparse.ArgumentTypeError("--mix must contain at least one positive weight")

    return tuple(choices)


def choose_command(rng: random.Random, mix: tuple[WeightedCommand, ...]) -> str:
    ticket = rng.randrange(1, mix[-1].cumulative_weight + 1)
    for entry in mix:
        if ticket <= entry.cumulative_weight:
            return entry.name
    raise AssertionError("unreachable")


def mix_contains(mix: tuple[WeightedCommand, ...], *names: str) -> bool:
    wanted = set(names)
    return any(entry.name in wanted for entry in mix)


def format_mix(mix: tuple[WeightedCommand, ...]) -> str:
    parts: list[str] = []
    previous = 0
    for entry in mix:
        parts.append(f"{entry.name}={entry.cumulative_weight - previous}")
        previous = entry.cumulative_weight
    return ", ".join(parts)


def decode_bulk(value: bytes | None) -> str | None:
    if value is None:
        return None
    return value.decode("utf-8", errors="replace")


def format_key(prefix: str, width: int, index: int) -> str:
    return f"{prefix}{index:0{width}x}"


def choose_unique_key_indices(
    rng: random.Random, key_space: int, min_count: int, max_count: int
) -> list[int]:
    upper = min(key_space, max_count)
    if upper < min_count:
        raise ValueError(
            f"key space {key_space} is too small for commands needing at least {min_count} keys"
        )

    count = rng.randint(min_count, upper)
    return rng.sample(range(key_space), count)


def build_value(process: int, sequence: int, slot: int, rng: random.Random) -> str:
    return f"v:{process}:{sequence}:{slot}:{rng.getrandbits(32):08x}"


def build_command_plan(
    *,
    rng: random.Random,
    mix: tuple[WeightedCommand, ...],
    process: int,
    sequence: int,
    key_space: int,
    key_prefix: str,
    key_width: int,
    max_multi_keys: int,
) -> CommandPlan:
    kind = choose_command(rng, mix)

    if kind == "set":
        key_index = rng.randrange(key_space)
        key = format_key(key_prefix, key_width, key_index)
        value = build_value(process, sequence, 0, rng)
        return CommandPlan(
            kind="set",
            parts=(b"SET", key.encode("utf-8"), value.encode("utf-8")),
            keys=(key,),
            values=(value,),
        )

    if kind == "get":
        key_index = rng.randrange(key_space)
        key = format_key(key_prefix, key_width, key_index)
        return CommandPlan(
            kind="get",
            parts=(b"GET", key.encode("utf-8")),
            keys=(key,),
            values=(),
        )

    if kind == "mset":
        key_indices = choose_unique_key_indices(rng, key_space, 2, max_multi_keys)
        keys = [format_key(key_prefix, key_width, index) for index in key_indices]
        values = [build_value(process, sequence, slot, rng) for slot in range(len(keys))]

        parts: list[bytes] = [b"MSET"]
        for key, value in zip(keys, values, strict=True):
            parts.append(key.encode("utf-8"))
            parts.append(value.encode("utf-8"))

        return CommandPlan(
            kind="mset",
            parts=tuple(parts),
            keys=tuple(keys),
            values=tuple(values),
        )

    if kind == "mget":
        key_indices = choose_unique_key_indices(rng, key_space, 2, max_multi_keys)
        keys = [format_key(key_prefix, key_width, index) for index in key_indices]
        parts = [b"MGET", *(key.encode("utf-8") for key in keys)]
        return CommandPlan(
            kind="mget",
            parts=tuple(parts),
            keys=tuple(keys),
            values=(),
        )

    if kind == "del":
        # Redis only returns the total delete count for DEL. Restrict generated
        # workloads to single-key deletes so the history can distinguish a
        # delete hit (external write to "absent") from a delete miss (read of
        # an already-absent key).
        key_index = rng.randrange(key_space)
        key = format_key(key_prefix, key_width, key_index)
        parts = [b"DEL", key.encode("utf-8")]
        return CommandPlan(
            kind="del",
            parts=tuple(parts),
            keys=(key,),
            values=(),
        )

    raise AssertionError(f"unsupported command kind {kind!r}")


async def send_one_roundtrip(
    host: str, port: int, io_timeout: float, request: bytes
) -> bytes:
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(host, port), timeout=io_timeout
    )
    try:
        writer.write(request)
        await asyncio.wait_for(writer.drain(), timeout=io_timeout)
        return await asyncio.wait_for(reader.readuntil(b"\r\n"), timeout=io_timeout)
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


async def probe_ping(host: str, port: int, io_timeout: float) -> str:
    plain_request, plain_expected = build_ping_request(None)
    line = await send_one_roundtrip(host, port, io_timeout, plain_request)
    if line == plain_expected:
        return "PING"

    if line.startswith(b"-ERR wrong number of arguments"):
        message = "__idlekv_history__"
        msg_request, msg_expected = build_ping_request(message)
        line = await send_one_roundtrip(host, port, io_timeout, msg_request)
        if line == msg_expected:
            return "PING <message>"

    raise RuntimeError(f"probe failed, unexpected reply: {line!r}")


async def read_resp_line(reader: asyncio.StreamReader, io_timeout: float) -> bytes:
    return await asyncio.wait_for(reader.readuntil(b"\r\n"), timeout=io_timeout)


async def read_resp(reader: asyncio.StreamReader, io_timeout: float) -> Any:
    prefix = await asyncio.wait_for(reader.readexactly(1), timeout=io_timeout)
    line = await read_resp_line(reader, io_timeout)
    payload = line[:-2]

    if prefix == b"+":
        return payload.decode("utf-8", errors="replace")
    if prefix == b"-":
        raise RespError(payload.decode("utf-8", errors="replace"))
    if prefix == b":":
        return int(payload)
    if prefix == b"$":
        length = int(payload)
        if length == -1:
            return None
        data = await asyncio.wait_for(reader.readexactly(length + 2), timeout=io_timeout)
        if data[-2:] != b"\r\n":
            raise RuntimeError("bulk string missing CRLF terminator")
        return data[:-2]
    if prefix == b"*":
        length = int(payload)
        if length == -1:
            return None
        return [await read_resp(reader, io_timeout) for _ in range(length)]

    raise RuntimeError(f"unsupported RESP prefix {prefix!r}")


async def send_command(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    io_timeout: float,
    *parts: bytes,
) -> Any:
    writer.write(build_resp_command(*parts))
    await asyncio.wait_for(writer.drain(), timeout=io_timeout)
    return await read_resp(reader, io_timeout)


async def open_client(
    host: str, port: int, io_timeout: float, db: int
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(host, port), timeout=io_timeout
    )
    try:
        if db != 0:
            reply = await send_command(
                reader, writer, io_timeout, b"SELECT", str(db).encode("ascii")
            )
            if reply != "OK":
                raise RuntimeError(f"SELECT returned unexpected reply: {reply!r}")
    except Exception:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        raise

    return reader, writer


async def maybe_flushdb(host: str, port: int, io_timeout: float, db: int) -> None:
    reader, writer = await open_client(host, port, io_timeout, db)
    try:
        reply = await send_command(reader, writer, io_timeout, b"FLUSHDB")
        if reply != "OK":
            raise RuntimeError(f"FLUSHDB returned unexpected reply: {reply!r}")
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


def build_record(
    *,
    process: int,
    sequence: int,
    start_ns: int,
    end_ns: int,
    ops: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "process": process,
        "txn": f"p{process}-{sequence + 1}",
        "start_ns": start_ns,
        "end_ns": end_ns,
        "status": "ok",
        "ops": ops,
    }


def record_from_reply(
    plan: CommandPlan,
    reply: Any,
    *,
    process: int,
    sequence: int,
    start_ns: int,
    end_ns: int,
) -> dict[str, Any]:
    if plan.kind == "set":
        if reply != "OK":
            raise RuntimeError(f"SET returned unexpected reply: {reply!r}")
        return build_record(
            process=process,
            sequence=sequence,
            start_ns=start_ns,
            end_ns=end_ns,
            ops=[{"type": "w", "key": plan.keys[0], "value": plan.values[0]}],
        )

    if plan.kind == "get":
        if reply is not None and not isinstance(reply, bytes):
            raise RuntimeError(f"GET returned unexpected reply: {reply!r}")
        return build_record(
            process=process,
            sequence=sequence,
            start_ns=start_ns,
            end_ns=end_ns,
            ops=[{"type": "r", "key": plan.keys[0], "value": decode_bulk(reply)}],
        )

    if plan.kind == "mset":
        if reply != "OK":
            raise RuntimeError(f"MSET returned unexpected reply: {reply!r}")
        return build_record(
            process=process,
            sequence=sequence,
            start_ns=start_ns,
            end_ns=end_ns,
            ops=[
                {"type": "w", "key": key, "value": value}
                for key, value in zip(plan.keys, plan.values, strict=True)
            ],
        )

    if plan.kind == "mget":
        if not isinstance(reply, list) or len(reply) != len(plan.keys):
            raise RuntimeError(f"MGET returned unexpected reply: {reply!r}")
        ops: list[dict[str, Any]] = []
        for key, value in zip(plan.keys, reply, strict=True):
            if value is not None and not isinstance(value, bytes):
                raise RuntimeError(f"MGET element returned unexpected reply: {value!r}")
            ops.append({"type": "r", "key": key, "value": decode_bulk(value)})
        return build_record(
            process=process,
            sequence=sequence,
            start_ns=start_ns,
            end_ns=end_ns,
            ops=ops,
        )

    if plan.kind == "del":
        if not isinstance(reply, int):
            raise RuntimeError(f"DEL returned unexpected reply: {reply!r}")
        if reply not in (0, 1):
            raise RuntimeError(
                "single-key DEL must return 0 or 1, "
                f"but got {reply!r} for key {plan.keys[0]!r}"
            )
        if reply == 0:
            return build_record(
                process=process,
                sequence=sequence,
                start_ns=start_ns,
                end_ns=end_ns,
                ops=[{"type": "r", "key": plan.keys[0], "value": None}],
            )
        return build_record(
            process=process,
            sequence=sequence,
            start_ns=start_ns,
            end_ns=end_ns,
            ops=[{"type": "del", "key": plan.keys[0]}],
        )

    raise AssertionError(f"unsupported plan kind {plan.kind!r}")


async def reporter(state: RunState, interval: float) -> None:
    while state.completed_ops < state.total_ops:
        await asyncio.sleep(interval)
        elapsed = max(1e-9, time.perf_counter() - state.start_time)
        rate = state.completed_ops / elapsed
        print(
            f"[{elapsed:7.2f}s] completed={state.completed_ops}/{state.total_ops}  "
            f"rate={rate:8.2f} ops/s"
        )


async def worker(
    *,
    process: int,
    args: argparse.Namespace,
    mix: tuple[WeightedCommand, ...],
    key_width: int,
    writer: HistoryWriter,
    state: RunState,
) -> None:
    rng = random.Random(args.seed + process)
    reader, stream = await open_client(args.host, args.port, args.io_timeout, args.db)
    try:
        for sequence in range(args.ops_per_client):
            plan = build_command_plan(
                rng=rng,
                mix=mix,
                process=process,
                sequence=sequence,
                key_space=args.key_space,
                key_prefix=args.key_prefix,
                key_width=key_width,
                max_multi_keys=args.max_multi_keys,
            )

            start_ns = time.monotonic_ns()
            reply = await send_command(reader, stream, args.io_timeout, *plan.parts)
            end_ns = time.monotonic_ns()

            record = record_from_reply(
                plan,
                reply,
                process=process,
                sequence=sequence,
                start_ns=start_ns,
                end_ns=end_ns,
            )
            await writer.write_record(record)
            state.completed_ops += 1
    finally:
        stream.close()
        try:
            await stream.wait_closed()
        except Exception:
            pass


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Generate a concurrent JSONL transaction history by issuing SET/GET/"
            "MSET/MGET/DEL against a Redis-compatible server."
        )
    )
    parser.add_argument("--host", default="127.0.0.1", help="server host")
    parser.add_argument("--port", type=int, default=4396, help="server port")
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("your_history.jsonl"),
        help="output history path (default: your_history.jsonl)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="overwrite the output file if it already exists",
    )
    parser.add_argument(
        "--clients",
        type=int,
        default=8,
        help="number of concurrent client connections (default: 8)",
    )
    parser.add_argument(
        "--ops-per-client",
        type=int,
        default=200,
        help="number of commands issued by each client (default: 200)",
    )
    parser.add_argument(
        "--key-space",
        type=int,
        default=8,
        help="number of logical keys used by the workload (default: 8)",
    )
    parser.add_argument(
        "--key-prefix",
        default="elle:kv:",
        help="key prefix for generated keys (default: elle:kv:)",
    )
    parser.add_argument(
        "--max-multi-keys",
        type=int,
        default=3,
        help="maximum number of keys used by MSET/MGET (default: 3)",
    )
    parser.add_argument(
        "--mix",
        type=parse_mix,
        default=parse_mix(DEFAULT_MIX),
        help=(
            "command mix as name=weight pairs "
            f"(default: {DEFAULT_MIX})"
        ),
    )
    parser.add_argument(
        "--allow-del",
        action="store_true",
        help=(
            "allow DEL in the workload mix; generated DEL requests are single-key "
            "so the resulting history remains checkable"
        ),
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=1,
        help="base random seed (default: 1)",
    )
    parser.add_argument(
        "--db",
        type=int,
        default=0,
        help="database index to SELECT before running the workload (default: 0)",
    )
    parser.add_argument(
        "--flushdb",
        action="store_true",
        help="run FLUSHDB on the selected DB before starting",
    )
    parser.add_argument(
        "--io-timeout",
        type=float,
        default=10.0,
        help="socket read/write timeout in seconds (default: 10)",
    )
    parser.add_argument(
        "--report-interval",
        type=float,
        default=5.0,
        help="progress report interval in seconds (default: 5)",
    )
    return parser


async def async_main(args: argparse.Namespace) -> int:
    if args.clients <= 0:
        raise ValueError("--clients must be greater than zero")
    if args.ops_per_client <= 0:
        raise ValueError("--ops-per-client must be greater than zero")
    if args.key_space <= 0:
        raise ValueError("--key-space must be greater than zero")
    if args.max_multi_keys <= 0:
        raise ValueError("--max-multi-keys must be greater than zero")
    if args.db < 0:
        raise ValueError("--db must be greater than or equal to zero")
    if args.report_interval <= 0:
        raise ValueError("--report-interval must be greater than zero")
    if mix_contains(args.mix, "mset", "mget") and min(args.key_space, args.max_multi_keys) < 2:
        raise ValueError(
            "--key-space and --max-multi-keys must allow at least 2 keys when mset/mget is enabled"
        )
    if mix_contains(args.mix, "del") and not args.allow_del:
        raise ValueError(
            "DEL is disabled by default so delete traffic stays an explicit opt-in; "
            "pass --allow-del if you want single-key DEL operations in the workload"
        )

    out_path = args.out.resolve()
    if out_path.exists() and not args.overwrite:
        raise ValueError(
            f"output file already exists: {out_path}; pass --overwrite to replace it"
        )

    ping_mode = await probe_ping(args.host, args.port, args.io_timeout)
    if args.flushdb:
        await maybe_flushdb(args.host, args.port, args.io_timeout, args.db)

    key_width = max(4, len(f"{args.key_space - 1:x}"))
    total_ops = args.clients * args.ops_per_client
    state = RunState(total_ops=total_ops, start_time=time.perf_counter())

    print("History generation configuration:")
    print(f"  endpoint:        {args.host}:{args.port}")
    print(f"  ping probe:      {ping_mode}")
    print(f"  db:              {args.db}")
    print(f"  clients:         {args.clients}")
    print(f"  ops/client:      {args.ops_per_client}")
    print(f"  total ops:       {total_ops}")
    print(f"  key space:       {args.key_space}")
    print(f"  key prefix:      {args.key_prefix}")
    print(f"  max multi keys:  {args.max_multi_keys}")
    print(f"  mix:             {format_mix(args.mix)}")
    print(f"  flushdb:         {'yes' if args.flushdb else 'no'}")
    print(f"  output:          {out_path}")

    writer = HistoryWriter(out_path)
    report_task = asyncio.create_task(reporter(state, args.report_interval))

    try:
        async with asyncio.TaskGroup() as task_group:
            for process in range(args.clients):
                task_group.create_task(
                    worker(
                        process=process,
                        args=args,
                        mix=args.mix,
                        key_width=key_width,
                        writer=writer,
                        state=state,
                    )
                )
    finally:
        report_task.cancel()
        try:
            await report_task
        except asyncio.CancelledError:
            pass
        writer.close()

    elapsed = max(1e-9, time.perf_counter() - state.start_time)
    print("History generation complete:")
    print(f"  elapsed:         {elapsed:.2f}s")
    print(f"  completed ops:   {state.completed_ops}")
    print(f"  throughput:      {state.completed_ops / elapsed:.2f} ops/s")
    print(f"  history file:    {out_path}")
    return 0


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()
    return asyncio.run(async_main(args))


if __name__ == "__main__":
    raise SystemExit(main())
