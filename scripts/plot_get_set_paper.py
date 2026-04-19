#!/usr/bin/env python3

from __future__ import annotations

import argparse
import os
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib")

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.patches import Patch
from matplotlib.ticker import FuncFormatter


RATIO_ORDER = ["9:1", "1:1", "1:9"]
SERVER_ORDER = ["redis", "idlekv"]
SERVER_LABELS = {"redis": "Redis", "idlekv": "IdleKV"}
SLIDE_BG = "#F7F7F7"
SLIDE_TEXT = "#525252"
SLIDE_TEXT_SOFT = "#727272"
SLIDE_GRID = "#C8C8C8"
SLIDE_REDIS = "#C6C6C6"
SLIDE_IDLEKV = "#9E1B32"
SLIDE_TEAL = "#2C858B"
SLIDE_TEAL_DARK = "#154E56"
SLIDE_RED_SOFT = "#D89CA8"
SLIDE_TEAL_SOFT = "#98C8CB"
SERVER_COLORS = {
    "redis": SLIDE_REDIS,
    "idlekv": SLIDE_IDLEKV,
}
SERVER_HATCHES = {
    "redis": "///",
    "idlekv": "",
}
LINE_COLORS = {
    "throughput": SLIDE_IDLEKV,
    "latency": SLIDE_TEAL,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Render publication-ready Get/Set benchmark figures."
    )
    parser.add_argument(
        "--summary-dir",
        required=True,
        help="Directory containing summary_long.csv and comparison_summary.csv",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Directory to write publication-ready figures",
    )
    return parser.parse_args()


def apply_publication_style() -> None:
    plt.rcParams.update(
        {
            "figure.dpi": 180,
            "savefig.dpi": 320,
            "savefig.bbox": "tight",
            "savefig.pad_inches": 0.02,
            "figure.facecolor": SLIDE_BG,
            "savefig.facecolor": SLIDE_BG,
            "axes.facecolor": SLIDE_BG,
            "font.family": "serif",
            "font.serif": [
                "Times New Roman",
                "Noto Serif CJK SC",
                "Source Han Serif SC",
                "DejaVu Serif",
            ],
            "axes.titlesize": 12,
            "axes.titleweight": "semibold",
            "axes.labelsize": 10.5,
            "xtick.labelsize": 9.5,
            "ytick.labelsize": 9.5,
            "legend.fontsize": 9.5,
            "axes.linewidth": 0.8,
            "grid.linewidth": 0.6,
            "pdf.fonttype": 42,
            "ps.fonttype": 42,
        }
    )


def load_tables(summary_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    summary_long = pd.read_csv(summary_dir / "summary_long.csv")
    comparison = pd.read_csv(summary_dir / "comparison_summary.csv")
    totals = pd.read_csv(summary_dir / "totals_summary.csv")

    for frame in (summary_long, comparison, totals):
        if "ratio" in frame.columns:
            frame["ratio"] = pd.Categorical(frame["ratio"], categories=RATIO_ORDER, ordered=True)
            frame.sort_values("ratio", inplace=True)

    return summary_long, comparison, totals


def format_ops_tick(value: float, _pos: float) -> str:
    return f"{value / 1e6:.1f}M"


def format_ms_tick(value: float, _pos: float) -> str:
    return f"{value:.1f}"


def format_compact_ops(value: float) -> str:
    return f"{value / 1e6:.2f}M"


def format_compact_ms(value: float) -> str:
    return f"{value:.2f}"


def format_compact_ratio(value: float) -> str:
    return f"{value:.2f}x"


def style_axis(ax: plt.Axes) -> None:
    ax.set_facecolor(SLIDE_BG)
    ax.grid(axis="y", color=SLIDE_GRID, alpha=0.8)
    ax.set_axisbelow(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color(SLIDE_TEXT)
    ax.spines["bottom"].set_color(SLIDE_TEXT)
    ax.tick_params(colors=SLIDE_TEXT, labelcolor=SLIDE_TEXT)
    ax.title.set_color(SLIDE_TEXT)
    ax.xaxis.label.set_color(SLIDE_TEXT)
    ax.yaxis.label.set_color(SLIDE_TEXT)


def add_panel_label(ax: plt.Axes, label: str) -> None:
    ax.text(
        -0.12,
        1.03,
        label,
        transform=ax.transAxes,
        fontsize=11,
        fontweight="bold",
        va="bottom",
        ha="left",
        color=SLIDE_IDLEKV,
    )


def make_bar_legend() -> list[Patch]:
    return [
        Patch(
            facecolor=SERVER_COLORS[server],
            edgecolor=SLIDE_TEXT,
            linewidth=0.8,
            hatch=SERVER_HATCHES[server],
            label=SERVER_LABELS[server],
        )
        for server in SERVER_ORDER
    ]


def annotate_bars(ax: plt.Axes, formatter, y_pad_fraction: float = 0.015) -> None:
    ymax = ax.get_ylim()[1]
    pad = ymax * y_pad_fraction
    for container in ax.containers:
        for bar in container:
            height = bar.get_height()
            if height <= 0:
                continue
            ax.text(
                bar.get_x() + bar.get_width() / 2.0,
                height + pad,
                formatter(height),
                ha="center",
                va="bottom",
                fontsize=8.5,
                color=SLIDE_TEXT,
            )


def plot_grouped_metric(
    ax: plt.Axes,
    metric_df: pd.DataFrame,
    value_col: str,
    title: str,
    ylabel: str,
    tick_formatter,
    bar_formatter,
    annotate: bool = True,
) -> None:
    pivot = (
        metric_df.pivot(index="ratio", columns="server", values=value_col)
        .reindex(RATIO_ORDER)
        .reindex(columns=SERVER_ORDER)
    )
    x = np.arange(len(RATIO_ORDER))
    width = 0.34
    max_value = float(np.nanmax(pivot.to_numpy()))
    bar_containers = []

    for idx, server in enumerate(SERVER_ORDER):
        positions = x + (idx - 0.5) * width
        bars = ax.bar(
            positions,
            pivot[server].values,
            width=width,
            color=SERVER_COLORS[server],
            edgecolor=SLIDE_TEXT,
            linewidth=0.8,
            hatch=SERVER_HATCHES[server],
            label=SERVER_LABELS[server],
            zorder=3,
        )
        bar_containers.append((server, bars))

    upper = max_value * 1.16 if max_value > 0 else 1.0
    ax.set_ylim(0, upper)

    if annotate:
        for server, bars in bar_containers:
            ax.bar_label(
                bars,
                labels=[bar_formatter(v) for v in pivot[server].values],
                padding=3,
                fontsize=8.5,
                color=SLIDE_TEXT,
            )

    ax.set_xticks(x)
    ax.set_xticklabels(RATIO_ORDER)
    ax.set_title(title)
    ax.set_xlabel("Read:Write ratio")
    ax.set_ylabel(ylabel)
    ax.yaxis.set_major_formatter(FuncFormatter(tick_formatter))
    ax.margins(x=0.05)
    style_axis(ax)


def plot_relative_gain(
    ax: plt.Axes,
    comparison: pd.DataFrame,
    *,
    title: str = "Relative improvement over Redis",
    legend_loc: str = "upper right",
    legend_ncol: int = 1,
    show_legend: bool = True,
    label_offset: float = 0.04,
) -> None:
    x = np.arange(len(RATIO_ORDER))
    throughput = comparison["idlekv_vs_redis_ops_speedup"].to_numpy()
    mean_latency = comparison["idlekv_vs_redis_latency_gain"].to_numpy()

    ax.plot(
        x,
        throughput,
        marker="o",
        markersize=5.5,
        linewidth=2.0,
        color=LINE_COLORS["throughput"],
        label="Throughput gain",
        zorder=3,
    )
    ax.plot(
        x,
        mean_latency,
        marker="s",
        markersize=5.2,
        linewidth=2.0,
        color=LINE_COLORS["latency"],
        label="Mean latency reduction",
        zorder=3,
    )

    for xs, ys, formatter in (
        (x, throughput, format_compact_ratio),
        (x, mean_latency, format_compact_ratio),
    ):
        for xi, yi in zip(xs, ys, strict=True):
            ax.text(
                xi,
                yi + label_offset,
                formatter(yi),
                ha="center",
                va="bottom",
                fontsize=8.5,
                color=SLIDE_TEXT,
            )

    ax.axhline(1.0, color=SLIDE_TEXT_SOFT, linestyle="--", linewidth=1.0, alpha=0.9)
    ax.set_xticks(x)
    ax.set_xticklabels(RATIO_ORDER)
    ax.set_xlabel("Read:Write ratio")
    ax.set_ylabel("Relative gain (x)")
    ax.set_title(title)
    ax.set_ylim(0.95, max(float(throughput.max()), float(mean_latency.max())) + 0.22)
    style_axis(ax)
    if show_legend:
        ax.legend(loc=legend_loc, ncol=legend_ncol, frameon=False)


def save_figure(fig: plt.Figure, output_dir: Path, stem: str) -> None:
    fig.savefig(output_dir / f"{stem}.png")
    fig.savefig(output_dir / f"{stem}.pdf")
    plt.close(fig)


def build_overview_figure(totals: pd.DataFrame, comparison: pd.DataFrame, output_dir: Path) -> None:
    fig = plt.figure(figsize=(11.2, 7.8), constrained_layout=True)
    grid = fig.add_gridspec(3, 2, height_ratios=[0.12, 1.0, 1.0])
    legend_ax = fig.add_subplot(grid[0, :])
    legend_ax.axis("off")
    axes = np.array(
        [
            [fig.add_subplot(grid[1, 0]), fig.add_subplot(grid[1, 1])],
            [fig.add_subplot(grid[2, 0]), fig.add_subplot(grid[2, 1])],
        ]
    )

    plot_grouped_metric(
        axes[0, 0],
        totals,
        "ops_sec",
        "Overall throughput",
        "Operations per second",
        format_ops_tick,
        format_compact_ops,
    )
    add_panel_label(axes[0, 0], "(a)")

    plot_grouped_metric(
        axes[0, 1],
        totals,
        "avg_latency_ms",
        "Mean latency",
        "Latency (ms)",
        format_ms_tick,
        format_compact_ms,
    )
    add_panel_label(axes[0, 1], "(b)")

    plot_grouped_metric(
        axes[1, 0],
        totals,
        "p99_latency_ms",
        "P99 latency",
        "Latency (ms)",
        format_ms_tick,
        format_compact_ms,
    )
    add_panel_label(axes[1, 0], "(c)")

    plot_relative_gain(axes[1, 1], comparison)
    add_panel_label(axes[1, 1], "(d)")

    legend_ax.legend(
        handles=make_bar_legend(),
        loc="center",
        ncol=2,
        frameon=False,
        handletextpad=0.6,
        columnspacing=1.4,
    )
    save_figure(fig, output_dir, "get_set_overview")


def build_breakdown_figure(summary_long: pd.DataFrame, output_dir: Path) -> None:
    fig = plt.figure(figsize=(10.8, 4.9), constrained_layout=True)
    grid = fig.add_gridspec(2, 2, height_ratios=[0.14, 1.0])
    legend_ax = fig.add_subplot(grid[0, :])
    legend_ax.axis("off")
    axes = [fig.add_subplot(grid[1, 0]), fig.add_subplot(grid[1, 1])]
    sections = [("Gets", "GET throughput"), ("Sets", "SET throughput")]

    for idx, (section, title) in enumerate(sections):
        section_df = summary_long[summary_long["section"] == section].copy()
        plot_grouped_metric(
            axes[idx],
            section_df,
            "ops_sec",
            title,
            "Operations per second",
            format_ops_tick,
            format_compact_ops,
        )
        add_panel_label(axes[idx], f"({chr(ord('a') + idx)})")

    legend_ax.legend(
        handles=make_bar_legend(),
        loc="center",
        ncol=2,
        frameon=False,
        handletextpad=0.6,
        columnspacing=1.4,
    )
    save_figure(fig, output_dir, "get_set_breakdown")


def build_single_metric_figures(totals: pd.DataFrame, comparison: pd.DataFrame, output_dir: Path) -> None:
    single_specs = [
        (
            "throughput_total",
            "ops_sec",
            "Overall throughput",
            "Operations per second",
            format_ops_tick,
            format_compact_ops,
        ),
        (
            "latency_mean",
            "avg_latency_ms",
            "Mean latency",
            "Latency (ms)",
            format_ms_tick,
            format_compact_ms,
        ),
        (
            "latency_p99",
            "p99_latency_ms",
            "P99 latency",
            "Latency (ms)",
            format_ms_tick,
            format_compact_ms,
        ),
    ]

    for stem, value_col, title, ylabel, tick_formatter, bar_formatter in single_specs:
        fig = plt.figure(figsize=(5.8, 4.6), constrained_layout=True)
        grid = fig.add_gridspec(2, 1, height_ratios=[0.16, 1.0])
        legend_ax = fig.add_subplot(grid[0, 0])
        legend_ax.axis("off")
        ax = fig.add_subplot(grid[1, 0])
        plot_grouped_metric(
            ax,
            totals,
            value_col,
            title,
            ylabel,
            tick_formatter,
            bar_formatter,
        )
        legend_ax.legend(
            handles=make_bar_legend(),
            loc="center",
            ncol=2,
            frameon=False,
            handletextpad=0.6,
            columnspacing=1.4,
        )
        save_figure(fig, output_dir, stem)

    fig, ax = plt.subplots(figsize=(5.8, 4.2), constrained_layout=True)
    plot_relative_gain(ax, comparison)
    save_figure(fig, output_dir, "relative_gain")


def build_intro_relative_gain_figure(comparison: pd.DataFrame, output_dir: Path) -> None:
    throughput = comparison["idlekv_vs_redis_ops_speedup"].to_numpy()
    mean_latency = comparison["idlekv_vs_redis_latency_gain"].to_numpy()

    fig = plt.figure(figsize=(8.8, 5.3), constrained_layout=True)
    grid = fig.add_gridspec(3, 1, height_ratios=[0.18, 0.12, 1.0])
    title_ax = fig.add_subplot(grid[0, 0])
    legend_ax = fig.add_subplot(grid[1, 0])
    ax = fig.add_subplot(grid[2, 0])

    title_ax.axis("off")
    legend_ax.axis("off")

    title_ax.text(
        0.5,
        0.78,
        "Relative improvement over Redis",
        ha="center",
        va="center",
        fontsize=15,
        fontweight="bold",
        color=SLIDE_TEXT,
    )
    title_ax.text(
        0.5,
        0.2,
        (
            f"Throughput gain: {throughput.min():.2f}x-{throughput.max():.2f}x    "
            f"Mean latency reduction: {mean_latency.min():.2f}x-{mean_latency.max():.2f}x"
        ),
        ha="center",
        va="center",
        fontsize=10,
        color=SLIDE_TEXT_SOFT,
    )

    plot_relative_gain(
        ax,
        comparison,
        title="",
        show_legend=False,
        label_offset=0.05,
    )
    ax.fill_between(np.arange(len(RATIO_ORDER)), 1.0, throughput, color=SLIDE_RED_SOFT, alpha=0.26)
    ax.fill_between(
        np.arange(len(RATIO_ORDER)),
        1.0,
        mean_latency,
        color=SLIDE_TEAL_SOFT,
        alpha=0.28,
    )
    legend_ax.legend(
        handles=ax.get_legend_handles_labels()[0],
        labels=ax.get_legend_handles_labels()[1],
        loc="center",
        ncol=2,
        frameon=False,
        handletextpad=0.8,
        columnspacing=1.8,
    )
    save_figure(fig, output_dir, "relative_gain_intro")


def main() -> None:
    args = parse_args()
    summary_dir = Path(args.summary_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    apply_publication_style()
    summary_long, comparison, totals = load_tables(summary_dir)
    build_overview_figure(totals[totals["section"] == "Totals"].copy(), comparison, output_dir)
    build_breakdown_figure(summary_long, output_dir)
    build_single_metric_figures(totals[totals["section"] == "Totals"].copy(), comparison, output_dir)
    build_intro_relative_gain_figure(comparison, output_dir)


if __name__ == "__main__":
    main()
