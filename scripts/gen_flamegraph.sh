#!/usr/bin/env bash

set -euo pipefail

FLAMEGRAPH_DIR_DEFAULT="/home/idle/code/FlameGraph"
OUT_DIR_DEFAULT="flamegraph-out"
FREQ_DEFAULT="99"
DURATION_DEFAULT="30"
CALL_GRAPH_DEFAULT="dwarf,16384"
TITLE_DEFAULT="idleKV Flame Graph"

usage() {
  cat <<'EOF'
Usage:
  scripts/gen_flamegraph.sh [options] -- <command> [args...]
  scripts/gen_flamegraph.sh [options] --pid <pid>

Options:
  --pid <pid>               Attach to an existing process instead of launching one.
  -d, --duration <seconds>  Sampling duration when using --pid. Default: 30
  -f, --freq <hz>           perf sampling frequency. Default: 99
  -c, --call-graph <mode>   perf call graph mode. Default: dwarf,16384
  -o, --out-dir <dir>       Output directory. Default: flamegraph-out/<timestamp>
  -t, --title <title>       Flame graph title. Default: "idleKV Flame Graph"
  --flamegraph-dir <dir>    FlameGraph repo path. Default: /home/idle/code/FlameGraph
  -h, --help                Show this help.

Examples:
  scripts/gen_flamegraph.sh -- ./build/src/idlekv
  scripts/gen_flamegraph.sh --pid 12345 -d 15

Outputs:
  <out-dir>/perf.data
  <out-dir>/out.perf
  <out-dir>/out.folded
  <out-dir>/flamegraph.svg
EOF
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

PID=""
DURATION="$DURATION_DEFAULT"
FREQ="$FREQ_DEFAULT"
CALL_GRAPH="$CALL_GRAPH_DEFAULT"
TITLE="$TITLE_DEFAULT"
FLAMEGRAPH_DIR="$FLAMEGRAPH_DIR_DEFAULT"
OUT_DIR=""
CMD=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pid)
      PID="${2:-}"
      shift 2
      ;;
    -d|--duration)
      DURATION="${2:-}"
      shift 2
      ;;
    -f|--freq)
      FREQ="${2:-}"
      shift 2
      ;;
    -c|--call-graph)
      CALL_GRAPH="${2:-}"
      shift 2
      ;;
    -o|--out-dir)
      OUT_DIR="${2:-}"
      shift 2
      ;;
    -t|--title)
      TITLE="${2:-}"
      shift 2
      ;;
    --flamegraph-dir)
      FLAMEGRAPH_DIR="${2:-}"
      shift 2
      ;;
    --)
      shift
      CMD=("$@")
      break
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

require_cmd perf
require_cmd perl

STACK_COLLAPSE="$FLAMEGRAPH_DIR/stackcollapse-perf.pl"
FLAMEGRAPH_PL="$FLAMEGRAPH_DIR/flamegraph.pl"

if [[ ! -x "$STACK_COLLAPSE" ]]; then
  echo "stackcollapse-perf.pl not found or not executable: $STACK_COLLAPSE" >&2
  exit 1
fi

if [[ ! -x "$FLAMEGRAPH_PL" ]]; then
  echo "flamegraph.pl not found or not executable: $FLAMEGRAPH_PL" >&2
  exit 1
fi

if [[ -z "$PID" && ${#CMD[@]} -eq 0 ]]; then
  echo "either provide --pid <pid> or a command after --" >&2
  usage
  exit 1
fi

if [[ -n "$PID" && ${#CMD[@]} -gt 0 ]]; then
  echo "--pid and command mode are mutually exclusive" >&2
  exit 1
fi

if [[ -z "$OUT_DIR" ]]; then
  OUT_DIR="$OUT_DIR_DEFAULT/$(date +%Y%m%d-%H%M%S)"
fi

mkdir -p "$OUT_DIR"

PERF_DATA="$OUT_DIR/perf.data"
PERF_TXT="$OUT_DIR/out.perf"
FOLDED="$OUT_DIR/out.folded"
SVG="$OUT_DIR/flamegraph.svg"

echo "output dir: $OUT_DIR"
echo "perf data:  $PERF_DATA"

if [[ -n "$PID" ]]; then
  echo "profiling pid $PID for ${DURATION}s"
  perf record \
    --freq "$FREQ" \
    --call-graph "$CALL_GRAPH" \
    --pid "$PID" \
    --output "$PERF_DATA" \
    -- sleep "$DURATION"
else
  echo "profiling command: ${CMD[*]}"
  perf record \
    --freq "$FREQ" \
    --call-graph "$CALL_GRAPH" \
    --output "$PERF_DATA" \
    -- "${CMD[@]}"
fi

perf script --input "$PERF_DATA" > "$PERF_TXT"
perl "$STACK_COLLAPSE" "$PERF_TXT" > "$FOLDED"
perl "$FLAMEGRAPH_PL" --title "$TITLE" "$FOLDED" > "$SVG"

echo "flame graph written to: $SVG"
