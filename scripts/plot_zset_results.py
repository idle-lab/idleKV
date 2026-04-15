#!/usr/bin/env python3
"""
Summarize ZSet benchmark CSVs and render comparison charts.
"""

from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from statistics import median
from typing import Iterable

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


OPS_ORDER = [
    "zadd_insert",
    "zadd_update",
    "zrange_head",
    "zrange_mid",
    "zrange_deep",
    "zrange_head_withscores",
]
WORKLOAD_ORDER = ["single", "multi"]
SERVER_ORDER = ["redis", "idlekv"]
SERVER_COLORS = {
    "redis": "#d62728",
    "idlekv": "#1f77b4",
}
PIPELINE_LINESTYLES = {
    1: "-",
    16: "--",
    64: ":",
}


@dataclass(frozen=True)
class BenchSummaryKey:
    server: str
    workload: str
    op: str
    clients: int
    pipeline: int
    withscores: bool
    key_count: int
    members_per_key: int
    total_members: int
    returned_members: int


@dataclass(frozen=True)
class MemorySummaryKey:
    server: str
    workload: str
    key_count: int
    members_per_key: int
    total_members: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Aggregate benchmark CSVs and generate ZSet comparison charts."
    )
    parser.add_argument("--bench-csv", required=True, help="Raw bench CSV path")
    parser.add_argument("--memory-csv", required=True, help="Raw memory CSV path")
    parser.add_argument(
        "--out-dir",
        required=True,
        help="Output directory. Summary CSVs go under summary/, charts under charts/",
    )
    return parser.parse_args()


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(path)
    if path.stat().st_size == 0:
        return []

    with path.open(newline="") as fh:
        return list(csv.DictReader(fh))


def parse_int(raw: str) -> int:
    return int(raw) if raw else 0


def parse_float(raw: str) -> float:
    return float(raw) if raw else 0.0


def load_bench_rows(path: Path) -> list[dict[str, object]]:
    rows = []
    for raw in read_csv(path):
        if raw.get("op") == "memory":
            continue
        rows.append(
            {
                "server": raw["server"],
                "workload": raw["workload"],
                "op": raw["op"],
                "clients": parse_int(raw["clients"]),
                "pipeline": parse_int(raw["pipeline"]),
                "withscores": raw["withscores"].lower() == "true",
                "key_count": parse_int(raw["key_count"]),
                "members_per_key": parse_int(raw["members_per_key"]),
                "total_members": parse_int(raw["total_members"]),
                "returned_members": parse_int(raw["returned_members"]),
                "run": raw["run"],
                "ops_per_sec": parse_float(raw["ops_per_sec"]),
                "elements_per_sec": parse_float(raw["elements_per_sec"]),
                "avg_us": parse_float(raw["avg_us"]),
                "p50_us": parse_float(raw["p50_us"]),
                "p95_us": parse_float(raw["p95_us"]),
                "p99_us": parse_float(raw["p99_us"]),
            }
        )
    return rows


def load_memory_rows(path: Path) -> list[dict[str, object]]:
    rows = []
    for raw in read_csv(path):
        if raw.get("op") != "memory":
            continue
        rows.append(
            {
                "server": raw["server"],
                "workload": raw["workload"],
                "key_count": parse_int(raw["key_count"]),
                "members_per_key": parse_int(raw["members_per_key"]),
                "total_members": parse_int(raw["total_members"]),
                "run": raw["run"],
                "used_memory_baseline": parse_int(raw["used_memory_baseline"]),
                "used_memory_after": parse_int(raw["used_memory_after"]),
                "used_memory_delta": parse_int(raw["used_memory_delta"]),
                "bytes_per_member": parse_float(raw["bytes_per_member"]),
                "rss_baseline": parse_int(raw["rss_baseline"]),
                "rss_after": parse_int(raw["rss_after"]),
            }
        )
    return rows


def summarize_bench(rows: Iterable[dict[str, object]]) -> list[dict[str, object]]:
    groups: dict[BenchSummaryKey, list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        key = BenchSummaryKey(
            server=str(row["server"]),
            workload=str(row["workload"]),
            op=str(row["op"]),
            clients=int(row["clients"]),
            pipeline=int(row["pipeline"]),
            withscores=bool(row["withscores"]),
            key_count=int(row["key_count"]),
            members_per_key=int(row["members_per_key"]),
            total_members=int(row["total_members"]),
            returned_members=int(row["returned_members"]),
        )
        groups[key].append(row)

    summaries: list[dict[str, object]] = []
    for key, group in groups.items():
        summaries.append(
            {
                "server": key.server,
                "workload": key.workload,
                "op": key.op,
                "clients": key.clients,
                "pipeline": key.pipeline,
                "withscores": key.withscores,
                "key_count": key.key_count,
                "members_per_key": key.members_per_key,
                "total_members": key.total_members,
                "returned_members": key.returned_members,
                "sample_count": len(group),
                "ops_per_sec": median(float(row["ops_per_sec"]) for row in group),
                "elements_per_sec": median(
                    float(row["elements_per_sec"]) for row in group
                ),
                "avg_us": median(float(row["avg_us"]) for row in group),
                "p50_us": median(float(row["p50_us"]) for row in group),
                "p95_us": median(float(row["p95_us"]) for row in group),
                "p99_us": median(float(row["p99_us"]) for row in group),
            }
        )

    return sorted(
        summaries,
        key=lambda row: (
            workload_rank(str(row["workload"])),
            op_rank(str(row["op"])),
            int(row["clients"]),
            int(row["pipeline"]),
            server_rank(str(row["server"])),
        ),
    )


def summarize_memory(rows: Iterable[dict[str, object]]) -> list[dict[str, object]]:
    groups: dict[MemorySummaryKey, list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        key = MemorySummaryKey(
            server=str(row["server"]),
            workload=str(row["workload"]),
            key_count=int(row["key_count"]),
            members_per_key=int(row["members_per_key"]),
            total_members=int(row["total_members"]),
        )
        groups[key].append(row)

    summaries: list[dict[str, object]] = []
    for key, group in groups.items():
        summaries.append(
            {
                "server": key.server,
                "workload": key.workload,
                "key_count": key.key_count,
                "members_per_key": key.members_per_key,
                "total_members": key.total_members,
                "sample_count": len(group),
                "used_memory_baseline": median(
                    int(row["used_memory_baseline"]) for row in group
                ),
                "used_memory_after": median(int(row["used_memory_after"]) for row in group),
                "used_memory_delta": median(int(row["used_memory_delta"]) for row in group),
                "bytes_per_member": median(
                    float(row["bytes_per_member"]) for row in group
                ),
                "rss_baseline": median(int(row["rss_baseline"]) for row in group),
                "rss_after": median(int(row["rss_after"]) for row in group),
            }
        )

    return sorted(
        summaries,
        key=lambda row: (
            workload_rank(str(row["workload"])),
            server_rank(str(row["server"])),
        ),
    )


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("")
        return

    header = list(rows[0].keys())
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=header)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def workload_rank(workload: str) -> int:
    try:
        return WORKLOAD_ORDER.index(workload)
    except ValueError:
        return len(WORKLOAD_ORDER)


def op_rank(op: str) -> int:
    try:
        return OPS_ORDER.index(op)
    except ValueError:
        return len(OPS_ORDER)


def server_rank(server: str) -> int:
    try:
        return SERVER_ORDER.index(server)
    except ValueError:
        return len(SERVER_ORDER)


def plot_memory_charts(rows: list[dict[str, object]], charts_dir: Path) -> None:
    if not rows:
        return

    charts_dir.mkdir(parents=True, exist_ok=True)
    workloads = [
        workload
        for workload in WORKLOAD_ORDER
        if any(str(row["workload"]) == workload for row in rows)
    ]
    servers = sorted({str(row["server"]) for row in rows}, key=server_rank)
    if not workloads or not servers:
        return

    fig, axes = plt.subplots(1, 2, figsize=(14, 5), constrained_layout=True)
    metrics = [
        ("bytes_per_member", "Bytes per member", 1.0),
        ("used_memory_delta", "Used memory delta (MiB)", 1024.0 * 1024.0),
    ]

    x_positions = list(range(len(workloads)))
    bar_width = 0.8 / max(len(servers), 1)

    for ax, (metric, ylabel, divisor) in zip(axes, metrics, strict=True):
        for idx, server in enumerate(servers):
            heights = []
            for workload in workloads:
                match = next(
                    (
                        row
                        for row in rows
                        if str(row["server"]) == server
                        and str(row["workload"]) == workload
                    ),
                    None,
                )
                value = float(match[metric]) / divisor if match is not None else 0.0
                heights.append(value)

            offsets = [
                x + (idx - (len(servers) - 1) / 2.0) * bar_width for x in x_positions
            ]
            ax.bar(
                offsets,
                heights,
                width=bar_width,
                label=server,
                color=SERVER_COLORS.get(server),
            )

        ax.set_xticks(x_positions, workloads)
        ax.set_ylabel(ylabel)
        ax.grid(True, axis="y", linestyle=":", alpha=0.4)
        ax.set_title(ylabel)

    axes[0].legend()
    fig.suptitle("ZSet Memory Comparison")
    fig.savefig(charts_dir / "memory_overview.png", dpi=180)
    plt.close(fig)


def plot_bench_metric(
    rows: list[dict[str, object]], workload: str, metric: str, ylabel: str, path: Path
) -> None:
    workload_rows = [row for row in rows if str(row["workload"]) == workload]
    if not workload_rows:
        return

    fig, axes = plt.subplots(2, 3, figsize=(18, 10), sharex=True)
    axes_flat = axes.flatten()
    legend_handles = {}
    client_ticks = sorted({int(row["clients"]) for row in workload_rows})

    for ax, op in zip(axes_flat, OPS_ORDER, strict=True):
        op_rows = [row for row in workload_rows if str(row["op"]) == op]
        if not op_rows:
            ax.set_visible(False)
            continue

        servers = sorted({str(row["server"]) for row in op_rows}, key=server_rank)
        pipelines = sorted({int(row["pipeline"]) for row in op_rows})
        for server in servers:
            for pipeline in pipelines:
                points = sorted(
                    (
                        row
                        for row in op_rows
                        if str(row["server"]) == server
                        and int(row["pipeline"]) == pipeline
                    ),
                    key=lambda row: int(row["clients"]),
                )
                if not points:
                    continue

                x_vals = [int(row["clients"]) for row in points]
                y_vals = [float(row[metric]) for row in points]
                label = f"{server} / p={pipeline}"
                (line,) = ax.plot(
                    x_vals,
                    y_vals,
                    marker="o",
                    linewidth=2,
                    linestyle=PIPELINE_LINESTYLES.get(pipeline, "-."),
                    color=SERVER_COLORS.get(server),
                    label=label,
                )
                legend_handles[label] = line

        ax.set_title(op)
        ax.set_xticks(client_ticks)
        ax.grid(True, linestyle=":", alpha=0.4)
        ax.set_xlabel("Clients")
        ax.set_ylabel(ylabel)

    handles = [legend_handles[label] for label in sorted(legend_handles)]
    labels = sorted(legend_handles)
    if handles:
        fig.legend(
            handles,
            labels,
            loc="lower center",
            ncol=min(3, len(handles)),
            bbox_to_anchor=(0.5, 0.01),
        )

    fig.suptitle(f"{workload} workload: {ylabel}")
    fig.tight_layout(rect=(0, 0.06, 1, 0.96))
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=180)
    plt.close(fig)


def main() -> None:
    args = parse_args()

    out_dir = Path(args.out_dir)
    summary_dir = out_dir / "summary"
    charts_dir = out_dir / "charts"
    summary_dir.mkdir(parents=True, exist_ok=True)
    charts_dir.mkdir(parents=True, exist_ok=True)

    bench_rows = load_bench_rows(Path(args.bench_csv))
    memory_rows = load_memory_rows(Path(args.memory_csv))

    bench_summary = summarize_bench(bench_rows)
    memory_summary = summarize_memory(memory_rows)

    write_csv(summary_dir / "bench_medians.csv", bench_summary)
    write_csv(summary_dir / "memory_medians.csv", memory_summary)

    plot_memory_charts(memory_summary, charts_dir)
    for workload in WORKLOAD_ORDER:
        plot_bench_metric(
            bench_summary,
            workload,
            "ops_per_sec",
            "Ops per second",
            charts_dir / f"{workload}_ops_per_sec.png",
        )
        plot_bench_metric(
            bench_summary,
            workload,
            "p99_us",
            "p99 latency (us)",
            charts_dir / f"{workload}_p99_us.png",
        )


if __name__ == "__main__":
    main()
