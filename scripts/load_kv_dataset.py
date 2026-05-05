#!/usr/bin/env python3
"""
Load a deterministic KV dataset into a Redis-compatible server.

By default this writes exactly 1 GiB of value bytes via pipelined SET commands.
Actual in-memory usage will be higher because key bytes and object metadata are
not included in the target size.
"""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
import math
import re
import time


SIZE_UNITS = {
    "": 1,
    "B": 1,
    "KB": 1000,
    "MB": 1000**2,
    "GB": 1000**3,
    "TB": 1000**4,
    "KIB": 1024,
    "MIB": 1024**2,
    "GIB": 1024**3,
    "TIB": 1024**4,
}

VALUE_FILLER_CHUNK = b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"


def parse_bytes(text: str) -> int:
    normalized = text.strip().replace("_", "")
    match = re.fullmatch(r"(?i)(\d+(?:\.\d+)?)\s*([kmgt]?i?b?)", normalized)
    if not match:
        raise argparse.ArgumentTypeError(
            f"invalid size {text!r}; examples: 1073741824, 1GB, 1GiB, 512MiB"
        )

    value = float(match.group(1))
    unit = match.group(2).upper()
    factor = SIZE_UNITS.get(unit)
    if factor is None:
        raise argparse.ArgumentTypeError(f"unsupported size unit in {text!r}")

    total = int(value * factor)
    if total <= 0:
        raise argparse.ArgumentTypeError("size must be greater than zero")
    return total


def format_bytes(num_bytes: int) -> str:
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    value = float(num_bytes)
    for unit in units:
        if value < 1024.0 or unit == units[-1]:
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.2f} {unit}"
        value /= 1024.0
    return f"{num_bytes} B"


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
        message = "__idlekv_load__"
        msg_request, msg_expected = build_ping_request(message)
        line = await send_one_roundtrip(host, port, io_timeout, msg_request)
        if line == msg_expected:
            return "PING <message>"

    raise RuntimeError(f"probe failed, unexpected reply: {line!r}")


async def read_simple_line(
    reader: asyncio.StreamReader, io_timeout: float, context: str
) -> bytes:
    line = await asyncio.wait_for(reader.readuntil(b"\r\n"), timeout=io_timeout)
    if line.startswith(b"-"):
        message = line.decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"{context} failed: {message}")
    return line


async def run_optional_command(
    host: str, port: int, io_timeout: float, *parts: bytes
) -> None:
    request = build_resp_command(*parts)
    reply = await send_one_roundtrip(host, port, io_timeout, request)
    if not reply.startswith(b"+"):
        raise RuntimeError(
            f"command {' '.join(part.decode('utf-8', errors='replace') for part in parts)} "
            f"failed: {reply!r}"
        )


def build_key(prefix: bytes, index: int, index_width: int) -> bytes:
    return prefix + f"{index:0{index_width}x}".encode("ascii")


def build_value(index: int, size: int, filler: bytes) -> bytes:
    header = f"value:{index:016x}:".encode("ascii")
    if len(header) >= size:
        return header[:size]
    return header + filler[: size - len(header)]


@dataclass
class LoaderState:
    total_keys: int
    next_index: int = 0
    loaded_keys: int = 0
    key_bytes: int = 0
    value_bytes: int = 0
    wire_bytes: int = 0
    start_time: float = 0.0

    def claim_batch(self, batch_size: int) -> tuple[int, int] | None:
        if self.next_index >= self.total_keys:
            return None
        start = self.next_index
        end = min(self.total_keys, start + batch_size)
        self.next_index = end
        return start, end


async def worker(
    worker_id: int,
    host: str,
    port: int,
    io_timeout: float,
    batch_size: int,
    state: LoaderState,
    total_value_bytes: int,
    base_value_size: int,
    last_value_size: int,
    key_prefix: bytes,
    index_width: int,
    filler: bytes,
) -> None:
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(host, port), timeout=io_timeout
    )
    try:
        while True:
            batch = state.claim_batch(batch_size)
            if batch is None:
                break

            batch_start, batch_end = batch
            request_chunks: list[bytes] = []
            batch_key_bytes = 0
            batch_value_bytes = 0

            for index in range(batch_start, batch_end):
                value_size = (
                    last_value_size if index == state.total_keys - 1 else base_value_size
                )
                key = build_key(key_prefix, index, index_width)
                value = build_value(index, value_size, filler)
                request_chunks.append(build_resp_command(b"SET", key, value))
                batch_key_bytes += len(key)
                batch_value_bytes += len(value)

            request = b"".join(request_chunks)
            writer.write(request)
            await asyncio.wait_for(writer.drain(), timeout=io_timeout)

            for _ in range(batch_start, batch_end):
                line = await read_simple_line(reader, io_timeout, "SET")
                if line != b"+OK\r\n":
                    raise RuntimeError(
                        f"worker {worker_id}: unexpected SET reply {line!r}"
                    )

            state.loaded_keys += batch_end - batch_start
            state.key_bytes += batch_key_bytes
            state.value_bytes += batch_value_bytes
            state.wire_bytes += len(request)

            if state.value_bytes > total_value_bytes:
                raise RuntimeError("internal error: loaded more value bytes than requested")
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


async def reporter(
    state: LoaderState, total_value_bytes: int, interval: float
) -> None:
    while state.loaded_keys < state.total_keys:
        await asyncio.sleep(interval)
        elapsed = max(1e-9, time.perf_counter() - state.start_time)
        progress = state.value_bytes / total_value_bytes
        value_rate = state.value_bytes / elapsed
        print(
            f"[{elapsed:7.2f}s] keys={state.loaded_keys}/{state.total_keys}  "
            f"values={format_bytes(state.value_bytes)}/{format_bytes(total_value_bytes)}  "
            f"progress={progress * 100:6.2f}%  "
            f"value_rate={format_bytes(int(value_rate))}/s"
        )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Load a deterministic KV dataset into a Redis-compatible server using "
            "pipelined SET commands."
        )
    )
    parser.add_argument("--host", default="127.0.0.1", help="server host")
    parser.add_argument("--port", type=int, default=6379, help="server port")
    parser.add_argument(
        "--target-bytes",
        type=parse_bytes,
        default=parse_bytes("1GiB"),
        help="target value bytes to write (default: 1GiB)",
    )
    parser.add_argument(
        "--value-size",
        type=parse_bytes,
        default=parse_bytes("1KiB"),
        help="value size per key before the final partial key (default: 1KiB)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=512,
        help="number of SET commands per pipeline batch (default: 512)",
    )
    parser.add_argument(
        "--connections",
        type=int,
        default=4,
        help="parallel TCP connections to use (default: 4)",
    )
    parser.add_argument(
        "--key-prefix",
        default="bench:kv:",
        help="key prefix for generated keys (default: bench:kv:)",
    )
    parser.add_argument(
        "--flushdb",
        action="store_true",
        help="run FLUSHDB before loading; only use this on a disposable dataset",
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
    if args.value_size <= 0:
        raise ValueError("--value-size must be greater than zero")
    if args.batch_size <= 0:
        raise ValueError("--batch-size must be greater than zero")
    if args.connections <= 0:
        raise ValueError("--connections must be greater than zero")

    ping_mode = await probe_ping(args.host, args.port, args.io_timeout)
    if args.flushdb:
        await run_optional_command(args.host, args.port, args.io_timeout, b"FLUSHDB")

    total_keys = math.ceil(args.target_bytes / args.value_size)
    last_value_size = args.target_bytes - (total_keys - 1) * args.value_size
    index_width = max(8, len(f"{total_keys - 1:x}"))
    key_prefix = args.key_prefix.encode("utf-8")
    filler = VALUE_FILLER_CHUNK * ((args.value_size // len(VALUE_FILLER_CHUNK)) + 2)

    print("Load configuration:")
    print(f"  endpoint:      {args.host}:{args.port}")
    print(f"  ping probe:    {ping_mode}")
    print(f"  key prefix:    {args.key_prefix}")
    print(f"  target values: {format_bytes(args.target_bytes)} ({args.target_bytes} bytes)")
    print(f"  value size:    {format_bytes(args.value_size)}")
    print(f"  total keys:    {total_keys}")
    print(f"  connections:   {args.connections}")
    print(f"  batch size:    {args.batch_size}")
    print(f"  flushdb:       {'yes' if args.flushdb else 'no'}")

    state = LoaderState(total_keys=total_keys, start_time=time.perf_counter())

    report_task = asyncio.create_task(
        reporter(state, total_value_bytes=args.target_bytes, interval=args.report_interval)
    )
    workers = [
        asyncio.create_task(
            worker(
                worker_id=worker_id,
                host=args.host,
                port=args.port,
                io_timeout=args.io_timeout,
                batch_size=args.batch_size,
                state=state,
                total_value_bytes=args.target_bytes,
                base_value_size=args.value_size,
                last_value_size=last_value_size,
                key_prefix=key_prefix,
                index_width=index_width,
                filler=filler,
            )
        )
        for worker_id in range(args.connections)
    ]

    try:
        await asyncio.gather(*workers)
    finally:
        report_task.cancel()
        try:
            await report_task
        except asyncio.CancelledError:
            pass

    elapsed = max(1e-9, time.perf_counter() - state.start_time)
    logical_payload_bytes = state.key_bytes + state.value_bytes

    print("Load complete:")
    print(f"  elapsed:       {elapsed:.2f}s")
    print(f"  keys written:  {state.loaded_keys}")
    print(
        f"  key bytes:     {format_bytes(state.key_bytes)} ({state.key_bytes} bytes)"
    )
    print(
        f"  value bytes:   {format_bytes(state.value_bytes)} ({state.value_bytes} bytes)"
    )
    print(
        "  logical bytes: "
        f"{format_bytes(logical_payload_bytes)} ({logical_payload_bytes} bytes)"
    )
    print(f"  wire bytes:    {format_bytes(state.wire_bytes)} ({state.wire_bytes} bytes)")
    print(
        "  value rate:    "
        f"{format_bytes(int(state.value_bytes / elapsed))}/s"
    )

    return 0


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()
    return asyncio.run(async_main(args))


if __name__ == "__main__":
    raise SystemExit(main())
