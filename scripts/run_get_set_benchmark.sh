#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "${SCRIPT_DIR}/.." && pwd)

RUN_NAME=${RUN_NAME:-$(date +%Y%m%d-%H%M%S)}
OUT_ROOT=${OUT_ROOT:-"${REPO_ROOT}/out"}
RUN_DIR="${OUT_ROOT}/get_set_benchmark/${RUN_NAME}"
RAW_DIR="${RUN_DIR}/raw"
LOG_DIR="${RUN_DIR}/logs"
SUMMARY_DIR="${RUN_DIR}/summary"
CHART_DIR="${RUN_DIR}/charts"

RATIOS=("9:1" "1:1" "1:9")
CLIENTS=${CLIENTS:-64}
THREADS=${THREADS:-4}
PIPELINE=${PIPELINE:-64}
REQUESTS=${REQUESTS:-500000}
KEY_MIN=${KEY_MIN:-1}
KEY_MAX=${KEY_MAX:-1000000}

REDIS_HOST=${REDIS_HOST:-127.0.0.1}
REDIS_PORT=${REDIS_PORT:-6379}
IDLEKV_HOST=${IDLEKV_HOST:-127.0.0.1}
IDLEKV_PORT=${IDLEKV_PORT:-4396}
IDLEKV_BIN=${IDLEKV_BIN:-"${REPO_ROOT}/build/src/idlekv"}

SERVER_PID=""
SERVER_NAME=""

usage() {
  cat <<'EOF'
Usage: scripts/run_get_set_benchmark.sh [options]

Run Get/Set memtier benchmarks for Redis and IdleKV sequentially, then aggregate
results and render comparison charts.

Options:
  --run-name NAME        Output subdirectory under out/get_set_benchmark
  --out-root DIR         Output root directory (default: out)
  --clients N            memtier --clients
  --threads N            memtier --threads
  --pipeline N           memtier --pipeline
  --requests N           memtier --requests
  --redis-port PORT      Redis port (default: 6379)
  --idlekv-port PORT     IdleKV port (default: 4396)
  --idlekv-bin PATH      IdleKV binary path
  -h, --help             Show this help
EOF
}

require_cmd() {
  local cmd=$1
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    echo "error: missing required command: ${cmd}" >&2
    exit 1
  fi
}

wait_for_redis_protocol() {
  local host=$1
  local port=$2
  local timeout_secs=${3:-30}

  if python3 - "${host}" "${port}" "${timeout_secs}" <<'PY'
import socket
import sys
import time

host = sys.argv[1]
port = int(sys.argv[2])
deadline = time.time() + float(sys.argv[3])
request = b"*1\r\n$4\r\nPING\r\n"

while time.time() < deadline:
    try:
        with socket.create_connection((host, port), timeout=0.5) as sock:
            sock.sendall(request)
            if b"PONG" in sock.recv(128):
                sys.exit(0)
    except OSError:
        time.sleep(0.2)

sys.exit(1)
PY
  then
    return 0
  fi

  echo "error: service on ${host}:${port} did not become ready within ${timeout_secs}s" >&2
  return 1
}

stop_server() {
  if [[ -z "${SERVER_PID}" ]]; then
    return 0
  fi

  if kill -0 "${SERVER_PID}" >/dev/null 2>&1; then
    kill -TERM "${SERVER_PID}" >/dev/null 2>&1 || true
    for _ in $(seq 1 50); do
      if ! kill -0 "${SERVER_PID}" >/dev/null 2>&1; then
        break
      fi
      sleep 0.2
    done
    if kill -0 "${SERVER_PID}" >/dev/null 2>&1; then
      kill -KILL "${SERVER_PID}" >/dev/null 2>&1 || true
    fi
    wait "${SERVER_PID}" >/dev/null 2>&1 || true
  fi

  SERVER_PID=""
  SERVER_NAME=""
}

cleanup() {
  stop_server
}

trap cleanup EXIT INT TERM

start_server() {
  local server=$1
  local server_log="${LOG_DIR}/${server}.server.log"

  stop_server

  case "${server}" in
    redis)
      SERVER_NAME=redis
      redis-server \
        --bind "${REDIS_HOST}" \
        --port "${REDIS_PORT}" \
        --save "" \
        --appendonly no \
        >"${server_log}" 2>&1 &
      SERVER_PID=$!
      wait_for_redis_protocol "${REDIS_HOST}" "${REDIS_PORT}" 30
      ;;
    idlekv)
      SERVER_NAME=idlekv
      "${IDLEKV_BIN}" \
        --ip "${IDLEKV_HOST}" \
        --port "${IDLEKV_PORT}" \
        --metrics-port 0 \
        >"${server_log}" 2>&1 &
      SERVER_PID=$!
      wait_for_redis_protocol "${IDLEKV_HOST}" "${IDLEKV_PORT}" 30
      ;;
    *)
      echo "error: unsupported server ${server}" >&2
      exit 1
      ;;
  esac
}

run_case() {
  local server=$1
  local ratio=$2
  local host port ratio_slug txt_out json_out

  case "${server}" in
    redis)
      host=${REDIS_HOST}
      port=${REDIS_PORT}
      ;;
    idlekv)
      host=${IDLEKV_HOST}
      port=${IDLEKV_PORT}
      ;;
    *)
      echo "error: unsupported server ${server}" >&2
      exit 1
      ;;
  esac

  ratio_slug=${ratio/:/-}
  txt_out="${RAW_DIR}/${server}-get-set-${ratio_slug}.txt"
  json_out="${RAW_DIR}/${server}-get-set-${ratio_slug}.json"

  echo "==> ${server} ratio=${ratio}"
  memtier_benchmark \
    --server="${host}" \
    --port="${port}" \
    --ratio="${ratio}" \
    --threads="${THREADS}" \
    --clients="${CLIENTS}" \
    --pipeline="${PIPELINE}" \
    --key-minimum="${KEY_MIN}" \
    --key-maximum="${KEY_MAX}" \
    --requests="${REQUESTS}" \
    --json-out-file="${json_out}" \
    --out-file="${txt_out}"
}

write_metadata() {
  cat >"${RUN_DIR}/metadata.env" <<EOF
RUN_NAME=${RUN_NAME}
RUN_DIR=${RUN_DIR}
RATIOS=${RATIOS[*]}
CLIENTS=${CLIENTS}
THREADS=${THREADS}
PIPELINE=${PIPELINE}
REQUESTS=${REQUESTS}
KEY_MIN=${KEY_MIN}
KEY_MAX=${KEY_MAX}
REDIS_HOST=${REDIS_HOST}
REDIS_PORT=${REDIS_PORT}
IDLEKV_HOST=${IDLEKV_HOST}
IDLEKV_PORT=${IDLEKV_PORT}
IDLEKV_BIN=${IDLEKV_BIN}
EOF
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --run-name)
        RUN_NAME=$2
        RUN_DIR="${OUT_ROOT}/get_set_benchmark/${RUN_NAME}"
        RAW_DIR="${RUN_DIR}/raw"
        LOG_DIR="${RUN_DIR}/logs"
        SUMMARY_DIR="${RUN_DIR}/summary"
        CHART_DIR="${RUN_DIR}/charts"
        shift 2
        ;;
      --out-root)
        OUT_ROOT=$2
        RUN_DIR="${OUT_ROOT}/get_set_benchmark/${RUN_NAME}"
        RAW_DIR="${RUN_DIR}/raw"
        LOG_DIR="${RUN_DIR}/logs"
        SUMMARY_DIR="${RUN_DIR}/summary"
        CHART_DIR="${RUN_DIR}/charts"
        shift 2
        ;;
      --clients)
        CLIENTS=$2
        shift 2
        ;;
      --threads)
        THREADS=$2
        shift 2
        ;;
      --pipeline)
        PIPELINE=$2
        shift 2
        ;;
      --requests)
        REQUESTS=$2
        shift 2
        ;;
      --redis-port)
        REDIS_PORT=$2
        shift 2
        ;;
      --idlekv-port)
        IDLEKV_PORT=$2
        shift 2
        ;;
      --idlekv-bin)
        IDLEKV_BIN=$2
        shift 2
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        echo "error: unknown option: $1" >&2
        usage
        exit 1
        ;;
    esac
  done
}

main() {
  parse_args "$@"

  require_cmd memtier_benchmark
  require_cmd redis-server
  require_cmd python3

  mkdir -p "${RAW_DIR}" "${LOG_DIR}" "${SUMMARY_DIR}" "${CHART_DIR}"
  write_metadata

  start_server redis
  for ratio in "${RATIOS[@]}"; do
    run_case redis "${ratio}"
  done
  stop_server

  start_server idlekv
  for ratio in "${RATIOS[@]}"; do
    run_case idlekv "${ratio}"
  done
  stop_server

  python3 "${REPO_ROOT}/scripts/plot_get_set_results.py" \
    --input-dir "${RAW_DIR}" \
    --summary-dir "${SUMMARY_DIR}" \
    --charts-dir "${CHART_DIR}"

  echo "Benchmark completed."
  echo "Run dir: ${RUN_DIR}"
}

main "$@"
