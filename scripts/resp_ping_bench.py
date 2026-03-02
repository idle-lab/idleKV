#!/usr/bin/env python3
"""
RESP PING benchmark client.

Use Redis protocol (RESP2) to send PING commands and measure I/O throughput.
"""

from __future__ import annotations

import argparse
import asyncio
from collections import deque
import socket
import time
from dataclasses import dataclass
from typing import Optional


def build_ping_request(message: Optional[str]) -> tuple[bytes, bytes]:
    if message is None:
        req = b"*1\r\n$4\r\nPING\r\n"
        return req, b"+PONG\r\n"

    payload = message.encode("utf-8")
    req = (
        b"*2\r\n$4\r\nPING\r\n$"
        + str(len(payload)).encode("ascii")
        + b"\r\n"
        + payload
        + b"\r\n"
    )
    expected = b"+" + payload + b"\r\n"
    return req, expected


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


async def probe_ping_command(
    host: str, port: int, io_timeout: float
) -> tuple[bytes, bytes, str]:
    plain_req, plain_expected = build_ping_request(None)
    line = await send_one_roundtrip(host, port, io_timeout, plain_req)
    if line == plain_expected:
        return plain_req, plain_expected, "PING"

    if line.startswith(b"-ERR wrong number of arguments"):
        msg_req, msg_expected = build_ping_request("__idlekv_bench__")
        line = await send_one_roundtrip(host, port, io_timeout, msg_req)
        if line == msg_expected:
            return msg_req, msg_expected, "PING <message> (auto-probed)"

    raise RuntimeError(f"probe failed, unexpected reply: {line!r}")


@dataclass
class Stats:
    requests: int = 0
    errors: int = 0
    bytes_sent: int = 0
    bytes_recv: int = 0
    latency_seconds_sum: float = 0.0

    def reset(self) -> None:
        self.requests = 0
        self.errors = 0
        self.bytes_sent = 0
        self.bytes_recv = 0
        self.latency_seconds_sum = 0.0

    def copy(self) -> "Stats":
        return Stats(
            requests=self.requests,
            errors=self.errors,
            bytes_sent=self.bytes_sent,
            bytes_recv=self.bytes_recv,
            latency_seconds_sum=self.latency_seconds_sum,
        )


class BenchmarkState:
    def __init__(self) -> None:
        self.stats = Stats()
        self.measuring = False
        self.stop_event = asyncio.Event()
        self.measure_start: float = 0.0
        self.measure_end: float = 0.0


async def worker(
    idx: int,
    state: BenchmarkState,
    host: str,
    port: int,
    request: bytes,
    expected_reply: bytes,
    pipeline: int,
    verify: bool,
    io_timeout: float,
    reconnect_delay: float,
) -> None:
    batch_req = request * pipeline
    req_sent = len(request)

    while not state.stop_event.is_set():
        writer: Optional[asyncio.StreamWriter] = None
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), timeout=io_timeout
            )
            sock = writer.get_extra_info("socket")
            if sock is not None:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

            while not state.stop_event.is_set():
                start = time.perf_counter()
                writer.write(batch_req)
                await asyncio.wait_for(writer.drain(), timeout=io_timeout)

                for _ in range(pipeline):
                    line = await asyncio.wait_for(
                        reader.readuntil(b"\r\n"), timeout=io_timeout
                    )
                    if verify and line != expected_reply:
                        raise RuntimeError(
                            f"worker-{idx}: unexpected reply: {line!r}, expect {expected_reply!r}"
                        )

                    # Count per completed request instead of per pipeline batch.
                    if state.measuring:
                        elapsed = time.perf_counter() - start
                        state.stats.requests += 1
                        state.stats.bytes_sent += req_sent
                        state.stats.bytes_recv += len(line)
                        state.stats.latency_seconds_sum += elapsed
        except asyncio.CancelledError:
            raise
        except Exception:
            if state.measuring:
                state.stats.errors += 1
            await asyncio.sleep(reconnect_delay)
        finally:
            if writer is not None:
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass


def print_progress(
    base: Stats, cur: Stats, dt: float, elapsed: float, report_window: float
) -> None:
    if dt <= 0:
        return

    req_delta = cur.requests - base.requests
    sent_delta = cur.bytes_sent - base.bytes_sent
    recv_delta = cur.bytes_recv - base.bytes_recv
    err_delta = cur.errors - base.errors

    qps = req_delta / dt
    tx_mib = sent_delta / dt / (1024 * 1024)
    rx_mib = recv_delta / dt / (1024 * 1024)

    print(
        f"[{elapsed:7.2f}s] qps({report_window:.1f}s)={qps:10.0f}  "
        f"tx={tx_mib:8.2f} MiB/s  rx={rx_mib:8.2f} MiB/s  "
        f"errors+={err_delta}"
    )


async def reporter(state: BenchmarkState, interval: float, report_window: float) -> None:
    while not state.measuring and not state.stop_event.is_set():
        await asyncio.sleep(0.05)

    history: deque[tuple[float, Stats]] = deque()
    history.append((state.measure_start, Stats()))

    while not state.stop_event.is_set():
        await asyncio.sleep(interval)
        now = time.perf_counter()
        cur = state.stats.copy()
        history.append((now, cur))

        cutoff = now - report_window
        while len(history) >= 2 and history[1][0] <= cutoff:
            history.popleft()

        base_time, base_stats = history[0]
        window_dt = now - base_time
        print_progress(
            base_stats,
            cur,
            window_dt,
            now - state.measure_start,
            min(report_window, now - state.measure_start),
        )


async def control(state: BenchmarkState, warmup: float, duration: float) -> None:
    if warmup > 0:
        print(f"Warmup: {warmup:.1f}s")
        await asyncio.sleep(warmup)

    state.stats.reset()
    state.measuring = True
    state.measure_start = time.perf_counter()
    print(f"Benchmarking: {duration:.1f}s")

    await asyncio.sleep(duration)
    state.measure_end = time.perf_counter()
    state.stop_event.set()


def summarize(state: BenchmarkState) -> None:
    total = state.stats
    elapsed = max(1e-9, state.measure_end - state.measure_start)
    qps = total.requests / elapsed
    tx_mib = total.bytes_sent / elapsed / (1024 * 1024)
    rx_mib = total.bytes_recv / elapsed / (1024 * 1024)
    avg_us = (
        (total.latency_seconds_sum / total.requests) * 1e6 if total.requests > 0 else 0.0
    )

    print("\n=== Summary ===")
    print(f"duration_sec           : {elapsed:.3f}")
    print(f"total_requests         : {total.requests}")
    print(f"total_errors           : {total.errors}")
    print(f"throughput_req_per_sec : {qps:.0f}")
    print(f"tx_mib_per_sec         : {tx_mib:.2f}")
    print(f"rx_mib_per_sec         : {rx_mib:.2f}")
    print(f"avg_req_latency_us     : {avg_us:.2f}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Benchmark Redis RESP PING throughput against a target service."
    )
    p.add_argument("--host", default="127.0.0.1", help="Target host.")
    p.add_argument("--port", type=int, default=4396, help="Target port.")
    p.add_argument(
        "-c", "--clients", type=int, default=32, help="Number of concurrent TCP clients."
    )
    p.add_argument(
        "-P",
        "--pipeline",
        type=int,
        default=32,
        help="Number of in-flight PING per round trip per client.",
    )
    p.add_argument(
        "-d", "--duration", type=float, default=10.0, help="Benchmark duration in seconds."
    )
    p.add_argument(
        "-w", "--warmup", type=float, default=2.0, help="Warmup time in seconds."
    )
    p.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Progress report interval in seconds.",
    )
    p.add_argument(
        "--report-window",
        type=float,
        default=5.0,
        help="Rolling window in seconds for progress metrics.",
    )
    p.add_argument(
        "--message",
        type=str,
        default=None,
        help="Optional payload for `PING <message>`.",
    )
    p.add_argument(
        "--io-timeout",
        type=float,
        default=5.0,
        help="Socket read/write timeout in seconds.",
    )
    p.add_argument(
        "--reconnect-delay",
        type=float,
        default=0.05,
        help="Delay before reconnect after I/O error in seconds.",
    )
    p.add_argument(
        "--no-verify",
        action="store_true",
        help="Do not verify server reply payload.",
    )

    args = p.parse_args()
    if args.clients <= 0:
        p.error("--clients must be > 0")
    if args.pipeline <= 0:
        p.error("--pipeline must be > 0")
    if args.duration <= 0:
        p.error("--duration must be > 0")
    if args.warmup < 0:
        p.error("--warmup must be >= 0")
    if args.interval <= 0:
        p.error("--interval must be > 0")
    if args.report_window <= 0:
        p.error("--report-window must be > 0")
    if args.io_timeout <= 0:
        p.error("--io-timeout must be > 0")
    if args.reconnect_delay < 0:
        p.error("--reconnect-delay must be >= 0")
    return args


async def run(args: argparse.Namespace) -> None:
    if args.message is None:
        request, expected_reply, command_text = await probe_ping_command(
            args.host, args.port, args.io_timeout
        )
    else:
        request, expected_reply = build_ping_request(args.message)
        command_text = f"PING <{len(args.message.encode('utf-8'))} bytes>"

    verify = not args.no_verify

    state = BenchmarkState()

    print(
        f"Target={args.host}:{args.port}  clients={args.clients}  "
        f"pipeline={args.pipeline}  verify={verify}"
    )
    print(f"Command={command_text}")

    worker_tasks = [
        asyncio.create_task(
            worker(
                idx=i,
                state=state,
                host=args.host,
                port=args.port,
                request=request,
                expected_reply=expected_reply,
                pipeline=args.pipeline,
                verify=verify,
                io_timeout=args.io_timeout,
                reconnect_delay=args.reconnect_delay,
            )
        )
        for i in range(args.clients)
    ]
    reporter_task = asyncio.create_task(
        reporter(state, args.interval, args.report_window)
    )
    control_task = asyncio.create_task(control(state, args.warmup, args.duration))

    try:
        await control_task
    finally:
        state.stop_event.set()
        for t in worker_tasks:
            t.cancel()
        reporter_task.cancel()
        await asyncio.gather(*worker_tasks, return_exceptions=True)
        await asyncio.gather(reporter_task, return_exceptions=True)

    summarize(state)


def main() -> None:
    args = parse_args()
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        print("\nInterrupted.")


if __name__ == "__main__":
    main()
