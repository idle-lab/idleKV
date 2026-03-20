#!/usr/bin/env python3

import argparse
import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

OPS_ORDER = [
    "InsertUnique",
    "LookupHit",
    "LookupMiss",
    "UpsertHit",
    "EraseHit",
    "MixedReadWrite",
]
INDEX_ORDER = ["art", "std::unordered_map", "absl::flat_hash_map"]
DATASET_ORDER = ["shared_prefix", "wide_fanout", "mixed"]
STEADY_STATE_OPS = ["LookupHit", "MixedReadWrite"]
COLORS = {
    "art": "#1f77b4",
    "std::unordered_map": "#ff7f0e",
    "absl::flat_hash_map": "#2ca02c",
}


def ordered_present(values, preferred_order):
    present = set(values)
    ordered = [value for value in preferred_order if value in present]
    extras = sorted(present - set(preferred_order))
    return ordered + extras


def load_reports(path: Path):
    numeric_float_fields = {
        "seconds",
        "qps",
        "throughput_mib_per_sec",
        "hit_rate",
        "miss_rate",
        "latency_avg_ns",
        "bytes_per_key",
        "amplification",
        "alloc_calls_per_op",
    }
    numeric_int_fields = {
        "op_count",
        "latency_p50_ns",
        "latency_p95_ns",
        "latency_p99_ns",
        "latency_p999_ns",
        "latency_max_ns",
        "start_live_bytes",
        "end_live_bytes",
        "peak_live_bytes",
        "phase_allocated_bytes",
        "phase_alloc_calls",
        "phase_dealloc_calls",
        "rss_before_setup_bytes",
        "rss_after_setup_bytes",
        "rss_after_ops_bytes",
        "reference_payload_bytes",
        "resident_key_count",
    }

    reports = []
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            for field in numeric_float_fields:
                row[field] = float(row[field])
            for field in numeric_int_fields:
                row[field] = int(row[field])
            reports.append(row)
    return reports


def plot_metric_by_operation(reports, metric_key, ylabel, title, output_path, log_scale=False):
    datasets = ordered_present((row["dataset"] for row in reports), DATASET_ORDER)
    indexes = ordered_present((row["index"] for row in reports), INDEX_ORDER)
    figure, axes = plt.subplots(1, len(datasets), figsize=(6 * len(datasets), 5), squeeze=False)

    for axis, dataset in zip(axes[0], datasets):
        subset = [row for row in reports if row["dataset"] == dataset]
        operations = ordered_present((row["operation"] for row in subset), OPS_ORDER)
        width = 0.8 / max(len(indexes), 1)
        centers = list(range(len(operations)))

        for index_pos, index_name in enumerate(indexes):
            values = []
            for operation in operations:
                match = next(
                    row
                    for row in subset
                    if row["index"] == index_name and row["operation"] == operation
                )
                values.append(match[metric_key])

            offsets = [
                center + (index_pos - (len(indexes) - 1) / 2.0) * width for center in centers
            ]
            axis.bar(
                offsets,
                values,
                width=width,
                label=index_name,
                color=COLORS.get(index_name),
                edgecolor="black",
                linewidth=0.5,
            )

        axis.set_title(dataset)
        axis.set_xticks(centers)
        axis.set_xticklabels(operations, rotation=30, ha="right")
        axis.set_ylabel(ylabel)
        axis.grid(axis="y", linestyle="--", alpha=0.4)
        if log_scale:
            axis.set_yscale("log")

    handles, labels = axes[0][0].get_legend_handles_labels()
    figure.legend(handles, labels, loc="upper center", ncol=len(indexes))
    figure.suptitle(title)
    figure.tight_layout(rect=(0, 0, 1, 0.92))
    figure.savefig(output_path, dpi=180)
    plt.close(figure)


def plot_steady_state_metric(reports, metric_key, ylabel, title, output_path, log_scale=False):
    filtered = [row for row in reports if row["operation"] in STEADY_STATE_OPS]
    datasets = ordered_present((row["dataset"] for row in filtered), DATASET_ORDER)
    indexes = ordered_present((row["index"] for row in filtered), INDEX_ORDER)

    categories = []
    for dataset in datasets:
        for operation in STEADY_STATE_OPS:
            if any(
                row["dataset"] == dataset and row["operation"] == operation for row in filtered
            ):
                categories.append((dataset, operation))

    figure, axis = plt.subplots(figsize=(max(8, len(categories) * 1.8), 5))
    width = 0.8 / max(len(indexes), 1)
    centers = list(range(len(categories)))

    for index_pos, index_name in enumerate(indexes):
        values = []
        for dataset, operation in categories:
            match = next(
                row
                for row in filtered
                if row["index"] == index_name
                and row["dataset"] == dataset
                and row["operation"] == operation
            )
            values.append(match[metric_key])

        offsets = [center + (index_pos - (len(indexes) - 1) / 2.0) * width for center in centers]
        axis.bar(
            offsets,
            values,
            width=width,
            label=index_name,
            color=COLORS.get(index_name),
            edgecolor="black",
            linewidth=0.5,
        )

    axis.set_xticks(centers)
    axis.set_xticklabels([f"{dataset}\n{operation}" for dataset, operation in categories])
    axis.set_ylabel(ylabel)
    axis.grid(axis="y", linestyle="--", alpha=0.4)
    axis.set_title(title)
    if log_scale:
        axis.set_yscale("log")
    axis.legend(loc="upper center", ncol=len(indexes))
    figure.tight_layout()
    figure.savefig(output_path, dpi=180)
    plt.close(figure)


def main():
    parser = argparse.ArgumentParser(description="Plot index benchmark CSV results.")
    parser.add_argument("--input", required=True, help="CSV file produced by index_bench --csv-out")
    parser.add_argument("--output-dir", required=True, help="Directory for generated PNG charts")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    reports = load_reports(input_path)
    if not reports:
        raise SystemExit("no benchmark rows found in CSV")

    plot_metric_by_operation(
        reports,
        "qps",
        "QPS (ops/s)",
        "Index Throughput Comparison",
        output_dir / "qps_by_operation.png",
    )
    plot_metric_by_operation(
        reports,
        "latency_p99_ns",
        "p99 latency (ns)",
        "Index p99 Latency Comparison",
        output_dir / "p99_latency_by_operation.png",
    )
    plot_steady_state_metric(
        reports,
        "bytes_per_key",
        "Bytes per key",
        "Steady-state Memory Cost",
        output_dir / "bytes_per_key.png",
    )
    plot_steady_state_metric(
        reports,
        "amplification",
        "Memory amplification",
        "Steady-state Memory Amplification",
        output_dir / "amplification.png",
    )
    plot_steady_state_metric(
        reports,
        "alloc_calls_per_op",
        "Allocation calls per op",
        "Steady-state Allocation Pressure",
        output_dir / "alloc_calls_per_op.png",
    )

    print(f"charts written to: {output_dir}")


if __name__ == "__main__":
    main()
