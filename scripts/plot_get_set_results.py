#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib")

import matplotlib.pyplot as plt
import pandas as pd


SECTIONS = ("Sets", "Gets", "Totals")
RATIO_ORDER = ["9:1", "1:1", "1:9"]
SERVER_ORDER = ["redis", "idlekv"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Aggregate memtier Get/Set benchmark results and render comparison charts."
    )
    parser.add_argument("--input-dir", required=True, help="Directory containing memtier JSON files")
    parser.add_argument("--summary-dir", required=True, help="Directory for summary CSV outputs")
    parser.add_argument("--charts-dir", required=True, help="Directory for chart outputs")
    return parser.parse_args()


def parse_result(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)

    filename = path.stem
    parts = filename.split("-")
    server = parts[0]
    ratio = parts[-2] + ":" + parts[-1]
    stats = payload["ALL STATS"]

    rows = []
    for section in SECTIONS:
        section_stats = stats[section]
        percentiles = section_stats.get("Percentile Latencies", {})
        count = section_stats.get("Count", 0)
        accumulated_latency = section_stats.get("Accumulated Latency", 0.0)
        average_latency = section_stats.get("Average Latency", 0.0)
        if count and accumulated_latency:
            average_latency = accumulated_latency / count
        rows.append(
            {
                "server": server,
                "ratio": ratio,
                "section": section,
                "count": count,
                "ops_sec": section_stats.get("Ops/sec", 0.0),
                "hits_sec": section_stats.get("Hits/sec", 0.0),
                "misses_sec": section_stats.get("Misses/sec", 0.0),
                "avg_latency_ms": average_latency,
                "p50_latency_ms": percentiles.get("p50.00", 0.0),
                "p99_latency_ms": percentiles.get("p99.00", 0.0),
                "p999_latency_ms": percentiles.get("p99.90", 0.0),
                "kb_sec": section_stats.get("KB/sec", 0.0),
                "connection_errors": section_stats.get("Connection Errors", 0),
                "accumulated_latency_ms": accumulated_latency,
            }
        )
    return rows


def annotate_bars(ax: plt.Axes, fmt: str) -> None:
    for patch in ax.patches:
        height = patch.get_height()
        if height <= 0:
            continue
        ax.annotate(
            fmt.format(height),
            (patch.get_x() + patch.get_width() / 2.0, height),
            ha="center",
            va="bottom",
            fontsize=8,
            xytext=(0, 3),
            textcoords="offset points",
        )


def grouped_bar(
    df: pd.DataFrame,
    value_col: str,
    title: str,
    ylabel: str,
    out_path: Path,
    fmt: str = "{:,.2f}",
) -> None:
    pivot = (
        df.pivot(index="ratio", columns="server", values=value_col)
        .reindex(RATIO_ORDER)
        .reindex(columns=SERVER_ORDER)
    )
    ax = pivot.plot(kind="bar", figsize=(9, 5), rot=0)
    ax.set_title(title)
    ax.set_xlabel("Read:Write Ratio")
    ax.set_ylabel(ylabel)
    ax.grid(axis="y", alpha=0.3)
    annotate_bars(ax, fmt)
    plt.tight_layout()
    plt.savefig(out_path, dpi=180)
    plt.close()


def split_ops_chart(df: pd.DataFrame, out_path: Path) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(14, 5), sharey=False)
    for ax, section in zip(axes, ("Gets", "Sets")):
        pivot = (
            df[df["section"] == section]
            .pivot(index="ratio", columns="server", values="ops_sec")
            .reindex(RATIO_ORDER)
            .reindex(columns=SERVER_ORDER)
        )
        pivot.plot(kind="bar", ax=ax, rot=0)
        ax.set_title(f"{section} Ops/sec")
        ax.set_xlabel("Read:Write Ratio")
        ax.set_ylabel("Ops/sec")
        ax.grid(axis="y", alpha=0.3)
        annotate_bars(ax, "{:,.0f}")
    plt.tight_layout()
    plt.savefig(out_path, dpi=180)
    plt.close()


def main() -> None:
    args = parse_args()
    input_dir = Path(args.input_dir)
    summary_dir = Path(args.summary_dir)
    charts_dir = Path(args.charts_dir)
    summary_dir.mkdir(parents=True, exist_ok=True)
    charts_dir.mkdir(parents=True, exist_ok=True)

    rows: list[dict] = []
    for path in sorted(input_dir.glob("*.json")):
        rows.extend(parse_result(path))

    if not rows:
        raise SystemExit(f"no JSON result files found in {input_dir}")

    df = pd.DataFrame(rows)
    df = df[df["ratio"].isin(RATIO_ORDER)].copy()
    df["ratio"] = pd.Categorical(df["ratio"], categories=RATIO_ORDER, ordered=True)
    df["server"] = pd.Categorical(df["server"], categories=SERVER_ORDER, ordered=True)
    df["section"] = pd.Categorical(df["section"], categories=list(SECTIONS), ordered=True)
    df = df.sort_values(["section", "ratio", "server"]).reset_index(drop=True)
    df.to_csv(summary_dir / "summary_long.csv", index=False)

    totals = df[df["section"] == "Totals"].copy()
    totals["ops_speedup_vs_redis"] = (
        totals.pivot(index="ratio", columns="server", values="ops_sec")["idlekv"]
        / totals.pivot(index="ratio", columns="server", values="ops_sec")["redis"]
    ).reindex(totals["ratio"]).to_numpy()
    totals["latency_improvement_vs_redis"] = (
        totals.pivot(index="ratio", columns="server", values="avg_latency_ms")["redis"]
        / totals.pivot(index="ratio", columns="server", values="avg_latency_ms")["idlekv"]
    ).reindex(totals["ratio"]).to_numpy()
    totals.to_csv(summary_dir / "totals_summary.csv", index=False)

    grouped_bar(
        totals,
        "ops_sec",
        "Total Throughput Comparison",
        "Ops/sec",
        charts_dir / "total_ops_sec.png",
    )
    grouped_bar(
        totals,
        "avg_latency_ms",
        "Average Latency Comparison",
        "Latency (ms)",
        charts_dir / "total_avg_latency_ms.png",
    )
    grouped_bar(
        totals,
        "p99_latency_ms",
        "p99 Latency Comparison",
        "Latency (ms)",
        charts_dir / "total_p99_latency_ms.png",
    )
    split_ops_chart(df, charts_dir / "get_set_ops_split.png")

    speedup = (
        totals.pivot(index="ratio", columns="server", values="ops_sec")["idlekv"]
        / totals.pivot(index="ratio", columns="server", values="ops_sec")["redis"]
    ).reindex(RATIO_ORDER)
    latency_gain = (
        totals.pivot(index="ratio", columns="server", values="avg_latency_ms")["redis"]
        / totals.pivot(index="ratio", columns="server", values="avg_latency_ms")["idlekv"]
    ).reindex(RATIO_ORDER)

    totals_pivot = totals.pivot(index="ratio", columns="server")
    comparison = pd.DataFrame(
        {
            "ratio": RATIO_ORDER,
            "redis_ops_sec": totals_pivot["ops_sec"]["redis"].reindex(RATIO_ORDER).values,
            "idlekv_ops_sec": totals_pivot["ops_sec"]["idlekv"].reindex(RATIO_ORDER).values,
            "idlekv_vs_redis_ops_speedup": speedup.reindex(RATIO_ORDER).values,
            "redis_avg_latency_ms": totals_pivot["avg_latency_ms"]["redis"].reindex(RATIO_ORDER).values,
            "idlekv_avg_latency_ms": totals_pivot["avg_latency_ms"]["idlekv"].reindex(RATIO_ORDER).values,
            "redis_p99_latency_ms": totals_pivot["p99_latency_ms"]["redis"].reindex(RATIO_ORDER).values,
            "idlekv_p99_latency_ms": totals_pivot["p99_latency_ms"]["idlekv"].reindex(RATIO_ORDER).values,
            "idlekv_vs_redis_latency_gain": latency_gain.reindex(RATIO_ORDER).values,
        }
    )
    comparison.to_csv(summary_dir / "comparison_summary.csv", index=False)

    fig, ax = plt.subplots(figsize=(9, 5))
    ax.plot(RATIO_ORDER, speedup.values, marker="o", linewidth=2, label="Throughput Speedup")
    ax.plot(RATIO_ORDER, latency_gain.values, marker="s", linewidth=2, label="Latency Improvement")
    ax.set_title("IdleKV vs Redis Relative Gain")
    ax.set_xlabel("Read:Write Ratio")
    ax.set_ylabel("x times")
    ax.grid(alpha=0.3)
    ax.legend()
    plt.tight_layout()
    plt.savefig(charts_dir / "idlekv_vs_redis_gain.png", dpi=180)
    plt.close()


if __name__ == "__main__":
    main()
