#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any


SCRIPT_PATH = Path(__file__).resolve()
REPO_ROOT = SCRIPT_PATH.parent.parent
ELLE_DIR = REPO_ROOT / "scripts" / "elle"
DEFAULT_DOCKER_IMAGE = os.environ.get("ELLE_CLOJURE_IMAGE", "clojure:temurin-21-tools-deps")
DEFAULT_MAVEN_REPO = os.environ.get("ELLE_MAVEN_REPO", "/tmp/idlekv-m2")
HEADLESS_JAVA_OPTION = "-Djava.awt.headless=true"
DELETE_TOKEN_PREFIX = "__idlekv_deleted__:"

READ_TYPES = {"r", "read", "get", "mget"}
DELETE_TYPES = {"del", "delete"}
DIRECT_WRITE_TYPES = {"w", "write", "set", "mset"}
WRITE_TYPES = DIRECT_WRITE_TYPES | DELETE_TYPES
STATUS_TYPES = {"ok", "fail", "info"}


class Keyword(str):
    pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert idleKV transaction history JSONL to Elle history and run the rw-register checker."
    )
    parser.add_argument("history", type=Path, help="Input JSONL history file")
    parser.add_argument(
        "--runner",
        choices=("auto", "local", "maven", "docker"),
        default="auto",
        help="How to run the Clojure checker",
    )
    parser.add_argument(
        "--model",
        default="strict-serializable",
        help="Elle consistency model to check, default: strict-serializable",
    )
    parser.add_argument(
        "--docker-image",
        default=DEFAULT_DOCKER_IMAGE,
        help=f"Docker image used by --runner=docker, default: {DEFAULT_DOCKER_IMAGE}",
    )
    parser.add_argument(
        "--edn-out",
        type=Path,
        default=None,
        help="Optional output path for the generated EDN history",
    )
    parser.add_argument(
        "--graphs-dir",
        type=Path,
        default=None,
        help="Optional directory where Elle will write anomaly reports and graphs",
    )
    return parser.parse_args()


def fail(msg: str) -> None:
    print(f"error: {msg}", file=sys.stderr)
    raise SystemExit(1)


def as_int(value: Any, field: str, line_no: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        fail(f"line {line_no}: field {field!r} must be an integer")
    return value


def normalize_status(value: Any, line_no: int) -> str:
    if not isinstance(value, str):
        fail(f"line {line_no}: field 'status' must be a string")
    status = value.lower()
    if status not in STATUS_TYPES:
        fail(f"line {line_no}: unsupported status {value!r}, expected one of {sorted(STATUS_TYPES)}")
    return status


def txn_label(record: dict[str, Any], line_no: int) -> str:
    label = record.get("txn_id")
    if isinstance(label, str) and label:
        return label
    label = record.get("txn")
    if isinstance(label, str) and label:
        return label
    return f"line-{line_no}-p{record['process']}"


def delete_token(record: dict[str, Any], line_no: int, op_index: int) -> str:
    return f"{DELETE_TOKEN_PREFIX}{txn_label(record, line_no)}:{op_index}"


def is_delete_token(value: Any) -> bool:
    return isinstance(value, str) and value.startswith(DELETE_TOKEN_PREFIX)


def normalize_mop(
    record: dict[str, Any], mop: Any, line_no: int, op_index: int
) -> dict[str, Any]:
    if not isinstance(mop, dict):
        fail(f"line {line_no}: ops[{op_index}] must be an object")

    op_type = mop.get("type")
    if not isinstance(op_type, str):
        fail(f"line {line_no}: ops[{op_index}].type must be a string")

    op_type = op_type.lower()
    if op_type in READ_TYPES:
        func = Keyword("r")
        value = mop.get("value")
    elif op_type in DELETE_TYPES:
        func = Keyword("w")
        value = mop.get("value")
        if value is None:
            value = delete_token(record, line_no, op_index)
    elif op_type in DIRECT_WRITE_TYPES:
        func = Keyword("w")
        value = mop.get("value")
        # Older histories encoded DEL as a plain write of nil. Treat those as
        # delete-like writes so Elle can still distinguish individual deletes.
        if value is None:
            value = delete_token(record, line_no, op_index)
    else:
        fail(
            f"line {line_no}: unsupported ops[{op_index}].type {mop['type']!r}, "
            f"expected one of {sorted(READ_TYPES | WRITE_TYPES)}"
        )

    if "key" not in mop:
        fail(f"line {line_no}: ops[{op_index}] is missing 'key'")

    return {
        "f": func,
        "key": mop["key"],
        "value": value,
    }


def encode_edn(value: Any) -> str:
    if value is None:
        return "nil"
    if isinstance(value, Keyword):
        return f":{value}"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return json.dumps(value)
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, list):
        return "[" + " ".join(encode_edn(item) for item in value) + "]"
    if isinstance(value, tuple):
        return "[" + " ".join(encode_edn(item) for item in value) + "]"
    if isinstance(value, dict):
        items = []
        for key, item in value.items():
            items.append(f"{encode_edn(key)} {encode_edn(item)}")
        return "{" + " ".join(items) + "}"
    fail(f"unsupported EDN value type: {type(value).__name__}")
    raise AssertionError("unreachable")


def encode_event(event: dict[str, Any]) -> str:
    parts = []
    for key, value in event.items():
        parts.append(f":{key} {encode_edn(value)}")
    return "{" + " ".join(parts) + "}"


def normalize_record(record: Any, line_no: int) -> dict[str, Any]:
    if not isinstance(record, dict):
        fail(f"line {line_no}: each JSONL line must be an object")

    if "process" not in record:
        fail(f"line {line_no}: missing 'process'")
    if "start_ns" not in record:
        fail(f"line {line_no}: missing 'start_ns'")
    if "end_ns" not in record:
        fail(f"line {line_no}: missing 'end_ns'")
    if "status" not in record:
        fail(f"line {line_no}: missing 'status'")
    if "ops" not in record:
        fail(f"line {line_no}: missing 'ops'")

    process = record["process"]
    start_ns = as_int(record["start_ns"], "start_ns", line_no)
    end_ns = as_int(record["end_ns"], "end_ns", line_no)
    if end_ns < start_ns:
        fail(f"line {line_no}: end_ns must be >= start_ns")

    status = normalize_status(record["status"], line_no)
    ops_raw = record["ops"]
    if not isinstance(ops_raw, list):
        fail(f"line {line_no}: field 'ops' must be a list")

    ops = [normalize_mop(record, mop, line_no, idx) for idx, mop in enumerate(ops_raw)]

    normalized = {
        "process": process,
        "start_ns": start_ns,
        "end_ns": end_ns,
        "status": status,
        "ops": ops,
        "_order": line_no,
    }

    if "txn_id" in record:
        normalized["txn-id"] = record["txn_id"]
    elif "txn" in record:
        normalized["txn-id"] = record["txn"]

    return normalized


def resolve_absent_reads(records: list[dict[str, Any]]) -> None:
    completed_writes: list[tuple[int, int, int, str, Any]] = []
    for record in records:
        if record["status"] != "ok":
            continue
        for op_index, op in enumerate(record["ops"]):
            if op["f"] == Keyword("w"):
                completed_writes.append(
                    (record["end_ns"], record["_order"], op_index, op["key"], op["value"])
                )

    completed_writes.sort()

    latest_completed_write: dict[str, Any] = {}
    write_index = 0
    for record in sorted(records, key=lambda item: (item["start_ns"], item["_order"])):
        while write_index < len(completed_writes):
            end_ns, _, _, key, value = completed_writes[write_index]
            if end_ns >= record["start_ns"]:
                break
            latest_completed_write[key] = value
            write_index += 1

        if record["status"] != "ok":
            continue

        for op in record["ops"]:
            if op["f"] != Keyword("r") or op["value"] is not None:
                continue

            latest_value = latest_completed_write.get(op["key"])
            if is_delete_token(latest_value):
                op["value"] = latest_value


def record_to_events(record: dict[str, Any]) -> list[dict[str, Any]]:
    base = {
        "process": record["process"],
        "f": Keyword("txn"),
        "value": [[op["f"], op["key"], op["value"]] for op in record["ops"]],
    }

    if "txn-id" in record:
        base["txn-id"] = record["txn-id"]

    return [
        {**base, "type": Keyword("invoke"), "time": record["start_ns"], "_order": record["_order"] * 2},
        {**base, "type": Keyword(record["status"]), "time": record["end_ns"], "_order": record["_order"] * 2 + 1},
    ]


def load_history(history_path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with history_path.open("r", encoding="utf-8") as fh:
        for line_no, raw_line in enumerate(fh, start=1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                fail(f"line {line_no}: invalid JSON: {exc}")
            records.append(normalize_record(record, line_no))

    if not records:
        fail("history file is empty")

    resolve_absent_reads(records)

    events: list[dict[str, Any]] = []
    for record in records:
        events.extend(record_to_events(record))

    def event_sort_key(event: dict[str, Any]) -> tuple[int, int, int]:
        return (
            event["time"],
            0 if event["type"] == Keyword("invoke") else 1,
            event["_order"],
        )

    events.sort(key=event_sort_key)
    for event in events:
        del event["_order"]
    return events


def write_edn_history(events: list[dict[str, Any]], edn_path: Path) -> None:
    edn_path.parent.mkdir(parents=True, exist_ok=True)
    with edn_path.open("w", encoding="utf-8") as fh:
        fh.write("[\n")
        for index, event in enumerate(events):
            suffix = "," if False else ""
            fh.write(f"  {encode_event(event)}{suffix}\n")
        fh.write("]\n")


def resolve_runner(requested: str) -> str:
    if requested != "auto":
        return requested
    if shutil.which("clojure"):
        return "local"
    if shutil.which("mvn"):
        return "maven"
    if shutil.which("docker"):
        return "docker"
    fail("none of 'clojure', 'mvn', or 'docker' is available")
    raise AssertionError("unreachable")


def containerize_path(path: Path, mounts: dict[Path, str]) -> str:
    path = path.resolve()
    if path.is_relative_to(REPO_ROOT):
        return str(Path("/workspace") / path.relative_to(REPO_ROOT))
    tmp_dir = Path("/tmp")
    if path.is_relative_to(tmp_dir):
        mounts.setdefault(tmp_dir, "/tmp")
        return str(Path("/tmp") / path.relative_to(tmp_dir))

    parent = path.parent
    if parent not in mounts:
        mounts[parent] = f"/host-mount-{len(mounts)}"
    return str(Path(mounts[parent]) / path.name)


def build_checker_command(
    runner: str, model: str, edn_path: Path, graphs_dir: Path | None, docker_image: str
) -> tuple[list[str], Path]:
    if runner == "local":
        cmd = ["clojure", "-M", "-m", "idlekv.elle-check", str(edn_path), model]
        if graphs_dir:
            cmd.append(str(graphs_dir))
        return cmd, ELLE_DIR

    if runner == "maven":
        args = ["-m", "idlekv.elle-check", str(edn_path), model]
        if graphs_dir:
            args.append(str(graphs_dir))
        cmd = [
            "mvn",
            "-q",
            f"-Dmaven.repo.local={DEFAULT_MAVEN_REPO}",
            "-f",
            str(ELLE_DIR / "pom.xml"),
            f"-Dexec.mainClass=clojure.main",
            f"-Dexec.args={shlex.join(args)}",
            "org.codehaus.mojo:exec-maven-plugin:3.5.0:java",
        ]
        return cmd, REPO_ROOT

    mounts: dict[Path, str] = {REPO_ROOT: "/workspace"}
    edn_arg = containerize_path(edn_path, mounts)
    cmd = [
        "docker",
        "run",
        "--rm",
        "-e",
        f"JAVA_TOOL_OPTIONS={HEADLESS_JAVA_OPTION}",
    ]
    if graphs_dir is not None:
        graphs_arg = containerize_path(graphs_dir, mounts)
    else:
        graphs_arg = None

    for host_path, container_path in mounts.items():
        cmd.extend(["-v", f"{host_path}:{container_path}"])

    cmd.extend(
        [
            "-w",
            "/workspace/scripts/elle",
            docker_image,
            "clojure",
            "-M",
            "-m",
            "idlekv.elle-check",
            edn_arg,
            model,
        ]
    )
    if graphs_arg is not None:
        cmd.append(graphs_arg)

    return cmd, REPO_ROOT


def main() -> int:
    args = parse_args()
    history_path = args.history.resolve()
    if not history_path.is_file():
        fail(f"history file does not exist: {history_path}")

    events = load_history(history_path)

    temp_dir_obj = None
    if args.edn_out is not None:
        edn_path = args.edn_out.resolve()
    else:
        temp_dir_obj = tempfile.TemporaryDirectory(prefix="idlekv-elle-", dir="/tmp")
        edn_path = Path(temp_dir_obj.name) / f"{history_path.stem}.edn"

    graphs_dir = args.graphs_dir.resolve() if args.graphs_dir is not None else None
    if graphs_dir is not None:
        graphs_dir.mkdir(parents=True, exist_ok=True)

    write_edn_history(events, edn_path)
    print(f"EDN history written to {edn_path}", file=sys.stderr)

    runner = resolve_runner(args.runner)
    cmd, cwd = build_checker_command(runner, args.model, edn_path, graphs_dir, args.docker_image)
    print(f"Running Elle via {runner}: {' '.join(cmd)}", file=sys.stderr)

    env = os.environ.copy()
    current_java_opts = env.get("JAVA_TOOL_OPTIONS", "").strip()
    if HEADLESS_JAVA_OPTION not in current_java_opts.split():
        env["JAVA_TOOL_OPTIONS"] = (
            f"{current_java_opts} {HEADLESS_JAVA_OPTION}".strip()
        )

    completed = subprocess.run(cmd, cwd=cwd, check=False, env=env)

    if temp_dir_obj is not None:
        temp_dir_obj.cleanup()

    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
