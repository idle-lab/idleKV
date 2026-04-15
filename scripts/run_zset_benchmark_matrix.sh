#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "${SCRIPT_DIR}/.." && pwd)

SERVERS=(redis idlekv)
WORKLOADS=(single multi)
OPS=(zadd_insert zadd_update zrange_head zrange_mid zrange_deep zrange_head_withscores)
CLIENTS=(1 8 32 64)
PIPELINES=(1 16 64)

RUN_NAME=${RUN_NAME:-$(date +%Y%m%d-%H%M%S)}
OUT_ROOT=${OUT_ROOT:-"${REPO_ROOT}/out"}
RUN_DIR="${OUT_ROOT}/zset_benchmark/${RUN_NAME}"
RAW_DIR="${RUN_DIR}/raw"
LOG_DIR="${RUN_DIR}/logs"
SUMMARY_DIR="${RUN_DIR}/summary"
CHART_DIR="${RUN_DIR}/charts"
BIN_DIR="${RUN_DIR}/bin"

BENCH_CLIENT_SOURCE="${REPO_ROOT}/scripts/resp_zset_bench.go"
BENCH_CLIENT_BIN="${BIN_DIR}/resp_zset_bench"
PLOT_SCRIPT="${REPO_ROOT}/scripts/plot_zset_results.py"

IDLEKV_BIN=${IDLEKV_BIN:-"${REPO_ROOT}/build/src/idlekv"}
REDIS_HOST=${REDIS_HOST:-127.0.0.1}
REDIS_PORT=${REDIS_PORT:-6379}
IDLEKV_HOST=${IDLEKV_HOST:-127.0.0.1}
IDLEKV_PORT=${IDLEKV_PORT:-4396}

MEMORY_RUNS=${MEMORY_RUNS:-3}
BENCH_RUNS=${BENCH_RUNS:-3}
BENCH_DURATION=${BENCH_DURATION:-30s}
WARMUP=${WARMUP:-10s}
SETTLE=${SETTLE:-10s}
REPORT_INTERVAL=${REPORT_INTERVAL:-2s}
IO_TIMEOUT=${IO_TIMEOUT:-5s}
RECONNECT_DELAY=${RECONNECT_DELAY:-200ms}
MEMORY_SAMPLES=${MEMORY_SAMPLES:-5}
MEMORY_SAMPLE_GAP=${MEMORY_SAMPLE_GAP:-500ms}
LOAD_CLIENTS=${LOAD_CLIENTS:-8}
LOAD_PIPELINE=${LOAD_PIPELINE:-16}
LOAD_BATCH_MEMBERS=${LOAD_BATCH_MEMBERS:-16}
ERROR_LOG_LIMIT=${ERROR_LOG_LIMIT:-16}
SEED=${SEED:-20260414}

SINGLE_KEY_COUNT=${SINGLE_KEY_COUNT:-1}
SINGLE_MEMBERS_PER_KEY=${SINGLE_MEMBERS_PER_KEY:-1000000}
MULTI_KEY_COUNT=${MULTI_KEY_COUNT:-4096}
MULTI_MEMBERS_PER_KEY=${MULTI_MEMBERS_PER_KEY:-1024}

VERIFY_CORRECTNESS=0
SKIP_PLOT=0
RESUME=0

SERVER_PID=""
SERVER_NAME=""
SERVER_PORT=""
SERVER_HOST=""
SERVER_LOG=""
SKIP_CSV=""

declare -A COMPLETED_MEMORY_CASES=()
declare -A COMPLETED_BENCH_CASES=()
declare -A SKIPPED_CASES=()

usage() {
  cat <<'EOF'
Usage: scripts/run_zset_benchmark_matrix.sh [options]

Run the full Redis vs IdleKV ZSet benchmark matrix sequentially. Each case starts
and stops the target service to avoid data contamination from previous runs.

Options:
  --run-name NAME               Output subdirectory name under out/zset_benchmark
  --out-root DIR                Root output directory (default: out)
  --servers LIST                Comma-separated servers: redis,idlekv
  --workloads LIST              Comma-separated workloads: single,multi
  --ops LIST                    Comma-separated ops
  --clients LIST                Comma-separated client counts
  --pipelines LIST              Comma-separated pipeline depths
  --memory-runs N               Repetitions for --mode memory
  --bench-runs N                Repetitions for --mode bench
  --bench-duration DUR          Benchmark duration, e.g. 30s
  --warmup DUR                  Warmup duration
  --settle DUR                  Settle duration after preload
  --verify-correctness          Enable exact semantic verification in benchmark client
  --resume                      Resume an existing run directory without truncating CSVs
  --skip-plot                   Skip chart generation at the end
  --redis-port PORT             Redis port (default: 6379)
  --idlekv-port PORT            IdleKV port (default: 4396)
  --idlekv-bin PATH             IdleKV binary path (default: build/src/idlekv)
  --single-members N            Members per key for single workload
  --multi-keys N                Key count for multi workload
  --multi-members N             Members per key for multi workload
  -h, --help                    Show this help

Environment overrides are also supported for the same variables, e.g.
BENCH_DURATION=10s MEMORY_RUNS=1.

Default matrix pruning:
  clients=1   -> keep pipeline=64
  clients=8   -> keep pipeline=16,64
  clients>=32 -> keep pipeline=1,16,64
EOF
}

split_csv_to_array() {
  local raw=$1
  local -n out_ref=$2
  IFS=',' read -r -a out_ref <<< "${raw}"
}

require_cmd() {
  local cmd=$1
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    echo "error: required command not found: ${cmd}" >&2
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
            data = sock.recv(128)
            if b"PONG" in data:
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
  SERVER_PORT=""
  SERVER_HOST=""
  SERVER_LOG=""
}

cleanup() {
  stop_server
}

trap cleanup EXIT INT TERM

start_server() {
  local server=$1
  local case_id=$2

  stop_server

  case "${server}" in
    redis)
      SERVER_NAME=redis
      SERVER_HOST=${REDIS_HOST}
      SERVER_PORT=${REDIS_PORT}
      SERVER_LOG="${LOG_DIR}/${case_id}.server.log"
      redis-server \
        --bind "${REDIS_HOST}" \
        --port "${REDIS_PORT}" \
        --save "" \
        --appendonly no \
        >"${SERVER_LOG}" 2>&1 &
      SERVER_PID=$!
      ;;
    idlekv)
      SERVER_NAME=idlekv
      SERVER_HOST=${IDLEKV_HOST}
      SERVER_PORT=${IDLEKV_PORT}
      SERVER_LOG="${LOG_DIR}/${case_id}.server.log"
      "${IDLEKV_BIN}" \
        --ip "${IDLEKV_HOST}" \
        --port "${IDLEKV_PORT}" \
        --metrics-port 0 \
        >"${SERVER_LOG}" 2>&1 &
      SERVER_PID=$!
      ;;
    *)
      echo "error: unsupported server ${server}" >&2
      exit 1
      ;;
  esac

  wait_for_redis_protocol "${SERVER_HOST}" "${SERVER_PORT}" 30
}

key_count_for_workload() {
  local workload=$1
  case "${workload}" in
    single) echo "${SINGLE_KEY_COUNT}" ;;
    multi) echo "${MULTI_KEY_COUNT}" ;;
    *)
      echo "error: unsupported workload ${workload}" >&2
      exit 1
      ;;
  esac
}

members_per_key_for_workload() {
  local workload=$1
  case "${workload}" in
    single) echo "${SINGLE_MEMBERS_PER_KEY}" ;;
    multi) echo "${MULTI_MEMBERS_PER_KEY}" ;;
    *)
      echo "error: unsupported workload ${workload}" >&2
      exit 1
      ;;
  esac
}

build_bench_client() {
  mkdir -p "${BIN_DIR}"
  go build -o "${BENCH_CLIENT_BIN}" "${BENCH_CLIENT_SOURCE}"
}

pipeline_enabled_for_clients() {
  local clients=$1
  local pipeline=$2

  if (( clients <= 1 )); then
    (( pipeline >= 64 ))
    return
  fi

  if (( clients <= 8 )); then
    (( pipeline >= 16 ))
    return
  fi

  return 0
}

range_stop_for_op() {
  local op=$1
  case "${op}" in
    zrange_head|zrange_head_withscores)
      echo 9
      ;;
    zrange_mid)
      echo 1009
      ;;
    zrange_deep)
      echo 100009
      ;;
    *)
      echo -1
      ;;
  esac
}

bench_case_supported() {
  local workload=$1
  local op=$2
  local members_per_key=$3
  local reason_ref=$4
  local stop

  stop=$(range_stop_for_op "${op}")
  if [[ "${stop}" -lt 0 ]]; then
    printf -v "${reason_ref}" '%s' ""
    return 0
  fi

  if (( members_per_key <= stop )); then
    printf -v "${reason_ref}" '%s' \
      "members-per-key=${members_per_key} is too small for ${op} (requires stop=${stop}, so members-per-key must be > ${stop})"
    return 1
  fi

  printf -v "${reason_ref}" '%s' ""
  return 0
}

load_completed_cases() {
  local csv_path=$1
  local assoc_name=$2

  if [[ ! -f "${csv_path}" || ! -s "${csv_path}" ]]; then
    return 0
  fi

  local -n assoc_ref="${assoc_name}"
  while IFS= read -r run_id; do
    [[ -n "${run_id}" ]] || continue
    assoc_ref["${run_id}"]=1
  done < <(awk -F, 'NR > 1 && $11 != "" { print $11 }' "${csv_path}")
}

load_skipped_cases() {
  if [[ ! -f "${SKIP_CSV}" || ! -s "${SKIP_CSV}" ]]; then
    return 0
  fi

  while IFS= read -r run_id; do
    [[ -n "${run_id}" ]] || continue
    SKIPPED_CASES["${run_id}"]=1
  done < <(awk -F, 'NR > 1 && $1 != "" { print $1 }' "${SKIP_CSV}")
}

record_skipped_case() {
  local case_id=$1
  local mode=$2
  local server=$3
  local workload=$4
  local op=$5
  local clients=$6
  local pipeline=$7
  local run_idx=$8
  local reason=$9

  if [[ -n "${SKIPPED_CASES[${case_id}]:-}" ]]; then
    return 0
  fi

  if [[ ! -f "${SKIP_CSV}" || ! -s "${SKIP_CSV}" ]]; then
    cat >"${SKIP_CSV}" <<'EOF'
run_id,mode,server,workload,op,clients,pipeline,run,reason
EOF
  fi

  printf '%s,%s,%s,%s,%s,%s,%s,%s,"%s"\n' \
    "${case_id}" "${mode}" "${server}" "${workload}" "${op}" "${clients}" "${pipeline}" "${run_idx}" \
    "${reason//\"/\"\"}" >>"${SKIP_CSV}"
  SKIPPED_CASES["${case_id}"]=1
}

prepare_run_dir() {
  local had_existing_data=0
  if [[ -d "${RUN_DIR}" ]] && find "${RUN_DIR}" -mindepth 1 -print -quit | grep -q .; then
    had_existing_data=1
  fi

  mkdir -p "${RAW_DIR}" "${LOG_DIR}" "${SUMMARY_DIR}" "${CHART_DIR}" "${BIN_DIR}"
  SKIP_CSV="${SUMMARY_DIR}/skipped_cases.csv"

  if [[ "${RESUME}" -eq 1 ]]; then
    [[ -f "${RAW_DIR}/memory.csv" ]] || : >"${RAW_DIR}/memory.csv"
    [[ -f "${RAW_DIR}/bench.csv" ]] || : >"${RAW_DIR}/bench.csv"
    load_completed_cases "${RAW_DIR}/memory.csv" COMPLETED_MEMORY_CASES
    load_completed_cases "${RAW_DIR}/bench.csv" COMPLETED_BENCH_CASES
    load_skipped_cases
    return 0
  fi

  if [[ "${had_existing_data}" -eq 1 ]]; then
    echo "error: run directory already contains data: ${RUN_DIR}" >&2
    echo "hint: use --resume to continue, or choose a new --run-name" >&2
    exit 1
  fi

  : >"${RAW_DIR}/memory.csv"
  : >"${RAW_DIR}/bench.csv"
}

derive_case_seed() {
  local mode=$1
  local workload=$2
  local op=$3
  local clients=$4
  local pipeline=$5
  local run_idx=$6

  python3 - "${SEED}" "${mode}" "${workload}" "${op}" "${clients}" "${pipeline}" "${run_idx}" <<'PY'
import hashlib
import sys

parts = sys.argv[1:]
raw = "|".join(parts).encode("utf-8")
digest = hashlib.sha256(raw).digest()
print(int.from_bytes(digest[:8], "big") & ((1 << 63) - 1))
PY
}

write_metadata() {
  cat >"${RUN_DIR}/metadata.env" <<EOF
RUN_NAME=${RUN_NAME}
RUN_DIR=${RUN_DIR}
SERVERS=${SERVERS[*]}
WORKLOADS=${WORKLOADS[*]}
OPS=${OPS[*]}
CLIENTS=${CLIENTS[*]}
PIPELINES=${PIPELINES[*]}
PIPELINE_PRUNE_RULE=clients=1:64;clients=8:16,64;clients>=32:1,16,64
MEMORY_RUNS=${MEMORY_RUNS}
BENCH_RUNS=${BENCH_RUNS}
BENCH_DURATION=${BENCH_DURATION}
WARMUP=${WARMUP}
SETTLE=${SETTLE}
REPORT_INTERVAL=${REPORT_INTERVAL}
IO_TIMEOUT=${IO_TIMEOUT}
RECONNECT_DELAY=${RECONNECT_DELAY}
MEMORY_SAMPLES=${MEMORY_SAMPLES}
MEMORY_SAMPLE_GAP=${MEMORY_SAMPLE_GAP}
LOAD_CLIENTS=${LOAD_CLIENTS}
LOAD_PIPELINE=${LOAD_PIPELINE}
LOAD_BATCH_MEMBERS=${LOAD_BATCH_MEMBERS}
ERROR_LOG_LIMIT=${ERROR_LOG_LIMIT}
VERIFY_CORRECTNESS=${VERIFY_CORRECTNESS}
RESUME=${RESUME}
REDIS_HOST=${REDIS_HOST}
REDIS_PORT=${REDIS_PORT}
IDLEKV_HOST=${IDLEKV_HOST}
IDLEKV_PORT=${IDLEKV_PORT}
IDLEKV_BIN=${IDLEKV_BIN}
SINGLE_KEY_COUNT=${SINGLE_KEY_COUNT}
SINGLE_MEMBERS_PER_KEY=${SINGLE_MEMBERS_PER_KEY}
MULTI_KEY_COUNT=${MULTI_KEY_COUNT}
MULTI_MEMBERS_PER_KEY=${MULTI_MEMBERS_PER_KEY}
EOF
}

run_memory_case() {
  local server=$1
  local workload=$2
  local run_idx=$3
  local key_count members_per_key case_id client_log case_seed

  key_count=$(key_count_for_workload "${workload}")
  members_per_key=$(members_per_key_for_workload "${workload}")
  case_id="${server}-${workload}-memory-r${run_idx}"
  client_log="${LOG_DIR}/${case_id}.client.log"
  case_seed=$(derive_case_seed memory "${workload}" memory 0 0 "${run_idx}")

  echo "==> ${case_id}"
  if [[ -n "${COMPLETED_MEMORY_CASES[${case_id}]:-}" ]]; then
    echo "    resume: skip completed case"
    return 0
  fi
  start_server "${server}" "${case_id}"

  local -a cmd=(
    "${BENCH_CLIENT_BIN}"
    --mode memory
    --server "${server}"
    --host "${SERVER_HOST}"
    --port "${SERVER_PORT}"
    --pid "${SERVER_PID}"
    --workload "${workload}"
    --key-count "${key_count}"
    --members-per-key "${members_per_key}"
    --memory-samples "${MEMORY_SAMPLES}"
    --memory-sample-gap "${MEMORY_SAMPLE_GAP}"
    --load-clients "${LOAD_CLIENTS}"
    --load-pipeline "${LOAD_PIPELINE}"
    --load-batch-members "${LOAD_BATCH_MEMBERS}"
    --settle "${SETTLE}"
    --io-timeout "${IO_TIMEOUT}"
    --reconnect-delay "${RECONNECT_DELAY}"
    --error-log-limit "${ERROR_LOG_LIMIT}"
    --seed "${case_seed}"
    --csv-out "${RAW_DIR}/memory.csv"
    --run-id "${case_id}"
  )

  if [[ "${VERIFY_CORRECTNESS}" -eq 1 ]]; then
    cmd+=(--verify-correctness)
  fi

  "${cmd[@]}" >"${client_log}" 2>&1
  COMPLETED_MEMORY_CASES["${case_id}"]=1
  stop_server
}

run_bench_case() {
  local server=$1
  local workload=$2
  local op=$3
  local clients=$4
  local pipeline=$5
  local run_idx=$6
  local key_count members_per_key case_id client_log case_seed

  key_count=$(key_count_for_workload "${workload}")
  members_per_key=$(members_per_key_for_workload "${workload}")
  case_id="${server}-${workload}-${op}-c${clients}-p${pipeline}-r${run_idx}"
  client_log="${LOG_DIR}/${case_id}.client.log"
  case_seed=$(derive_case_seed bench "${workload}" "${op}" "${clients}" "${pipeline}" "${run_idx}")

  echo "==> ${case_id}"
  if [[ -n "${COMPLETED_BENCH_CASES[${case_id}]:-}" ]]; then
    echo "    resume: skip completed case"
    return 0
  fi
  if [[ -n "${SKIPPED_CASES[${case_id}]:-}" ]]; then
    echo "    resume: skip previously recorded incompatible case"
    return 0
  fi

  local skip_reason=""
  if ! bench_case_supported "${workload}" "${op}" "${members_per_key}" skip_reason; then
    echo "    skip: ${skip_reason}"
    record_skipped_case "${case_id}" bench "${server}" "${workload}" "${op}" "${clients}" "${pipeline}" "${run_idx}" "${skip_reason}"
    return 0
  fi

  start_server "${server}" "${case_id}"

  local -a cmd=(
    "${BENCH_CLIENT_BIN}"
    --mode bench
    --server "${server}"
    --host "${SERVER_HOST}"
    --port "${SERVER_PORT}"
    --pid "${SERVER_PID}"
    --workload "${workload}"
    --op "${op}"
    --clients "${clients}"
    --pipeline "${pipeline}"
    --duration "${BENCH_DURATION}"
    --warmup "${WARMUP}"
    --report-interval "${REPORT_INTERVAL}"
    --key-count "${key_count}"
    --members-per-key "${members_per_key}"
    --load-clients "${LOAD_CLIENTS}"
    --load-pipeline "${LOAD_PIPELINE}"
    --load-batch-members "${LOAD_BATCH_MEMBERS}"
    --settle "${SETTLE}"
    --io-timeout "${IO_TIMEOUT}"
    --reconnect-delay "${RECONNECT_DELAY}"
    --error-log-limit "${ERROR_LOG_LIMIT}"
    --seed "${case_seed}"
    --csv-out "${RAW_DIR}/bench.csv"
    --run-id "${case_id}"
  )

  if [[ "${VERIFY_CORRECTNESS}" -eq 1 ]]; then
    cmd+=(--verify-correctness)
  fi

  "${cmd[@]}" >"${client_log}" 2>&1
  COMPLETED_BENCH_CASES["${case_id}"]=1
  stop_server
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --run-name)
        RUN_NAME=$2
        RUN_DIR="${OUT_ROOT}/zset_benchmark/${RUN_NAME}"
        RAW_DIR="${RUN_DIR}/raw"
        LOG_DIR="${RUN_DIR}/logs"
        SUMMARY_DIR="${RUN_DIR}/summary"
        CHART_DIR="${RUN_DIR}/charts"
        BIN_DIR="${RUN_DIR}/bin"
        BENCH_CLIENT_BIN="${BIN_DIR}/resp_zset_bench"
        shift 2
        ;;
      --out-root)
        OUT_ROOT=$2
        RUN_DIR="${OUT_ROOT}/zset_benchmark/${RUN_NAME}"
        RAW_DIR="${RUN_DIR}/raw"
        LOG_DIR="${RUN_DIR}/logs"
        SUMMARY_DIR="${RUN_DIR}/summary"
        CHART_DIR="${RUN_DIR}/charts"
        BIN_DIR="${RUN_DIR}/bin"
        BENCH_CLIENT_BIN="${BIN_DIR}/resp_zset_bench"
        shift 2
        ;;
      --servers)
        split_csv_to_array "$2" SERVERS
        shift 2
        ;;
      --workloads)
        split_csv_to_array "$2" WORKLOADS
        shift 2
        ;;
      --ops)
        split_csv_to_array "$2" OPS
        shift 2
        ;;
      --clients)
        split_csv_to_array "$2" CLIENTS
        shift 2
        ;;
      --pipelines)
        split_csv_to_array "$2" PIPELINES
        shift 2
        ;;
      --memory-runs)
        MEMORY_RUNS=$2
        shift 2
        ;;
      --bench-runs)
        BENCH_RUNS=$2
        shift 2
        ;;
      --bench-duration)
        BENCH_DURATION=$2
        shift 2
        ;;
      --warmup)
        WARMUP=$2
        shift 2
        ;;
      --settle)
        SETTLE=$2
        shift 2
        ;;
      --verify-correctness)
        VERIFY_CORRECTNESS=1
        shift
        ;;
      --resume)
        RESUME=1
        shift
        ;;
      --skip-plot)
        SKIP_PLOT=1
        shift
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
      --single-members)
        SINGLE_MEMBERS_PER_KEY=$2
        shift 2
        ;;
      --multi-keys)
        MULTI_KEY_COUNT=$2
        shift 2
        ;;
      --multi-members)
        MULTI_MEMBERS_PER_KEY=$2
        shift 2
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        echo "error: unknown option: $1" >&2
        usage >&2
        exit 2
        ;;
    esac
  done
}

main() {
  parse_args "$@"

  require_cmd go
  require_cmd python3
  if [[ " ${SERVERS[*]} " == *" redis "* ]]; then
    require_cmd redis-server
  fi
  if [[ " ${SERVERS[*]} " == *" idlekv "* ]]; then
    if [[ ! -x "${IDLEKV_BIN}" ]]; then
      echo "error: IdleKV binary not found or not executable: ${IDLEKV_BIN}" >&2
      exit 1
    fi
  fi

  prepare_run_dir
  build_bench_client
  write_metadata

  for server in "${SERVERS[@]}"; do
    for workload in "${WORKLOADS[@]}"; do
      for run_idx in $(seq 1 "${MEMORY_RUNS}"); do
        run_memory_case "${server}" "${workload}" "${run_idx}"
      done
    done
  done

  for server in "${SERVERS[@]}"; do
    for workload in "${WORKLOADS[@]}"; do
      for op in "${OPS[@]}"; do
        for clients in "${CLIENTS[@]}"; do
          for pipeline in "${PIPELINES[@]}"; do
            if ! pipeline_enabled_for_clients "${clients}" "${pipeline}"; then
              continue
            fi
            for run_idx in $(seq 1 "${BENCH_RUNS}"); do
              run_bench_case "${server}" "${workload}" "${op}" "${clients}" "${pipeline}" "${run_idx}"
            done
          done
        done
      done
    done
  done

  if [[ "${SKIP_PLOT}" -eq 0 ]]; then
    python3 "${PLOT_SCRIPT}" \
      --bench-csv "${RAW_DIR}/bench.csv" \
      --memory-csv "${RAW_DIR}/memory.csv" \
      --out-dir "${RUN_DIR}"
  fi

  cat <<EOF
Benchmark matrix completed.
Run directory : ${RUN_DIR}
Raw bench CSV : ${RAW_DIR}/bench.csv
Raw memory CSV: ${RAW_DIR}/memory.csv
Charts        : ${CHART_DIR}
Summary CSVs  : ${SUMMARY_DIR}
Logs          : ${LOG_DIR}
EOF
}

main "$@"
