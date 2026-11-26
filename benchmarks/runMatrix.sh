#!/usr/bin/env bash
# AegisFS benchmark runner. Measures create/get latency across file sizes under two replication
# fan-out strategies (sequential vs parallel) on a process-local 5-node cluster, and append
# throughput. Emits raw CSV to benchmarks/results/raw.csv. Plain wall-clock timings.
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/devCreds.sh"
BIN="$ROOT/build/aegisfs"
RESULTS="$ROOT/benchmarks/results"
TMP="$(mktemp -d)"
N=5
BASE=9300          # distinct port range from the demo/smoke clusters
REPS=5             # repetitions per data point
SIZES_KB=(1 1024 10240 102400)   # 1 KiB, 1 MiB, 10 MiB, 100 MiB
mkdir -p "$RESULTS"

NODE_PIDS=()
# Stops only the nodes this script started.
stopCluster(){
  [ "${#NODE_PIDS[@]}" -eq 0 ] && return 0
  kill -9 "${NODE_PIDS[@]}" 2>/dev/null
  wait "${NODE_PIDS[@]}" 2>/dev/null
  NODE_PIDS=()
}
cleanup(){ stopCluster; rm -rf "$TMP" "$ROOT/run-bench"; }
trap cleanup EXIT
[ -x "$BIN" ] || { echo "build first"; exit 1; }

nowMs(){ python3 -c 'import time;print(int(time.time()*1000))'; }

startCluster() {
  local parallel="$1"
  stopCluster
  rm -rf "$ROOT/run-bench"; mkdir -p "$ROOT/run-bench"
  local seed="127.0.0.1:$BASE"
  for i in $(seq 0 $((N-1))); do
    local port=$((BASE+i)); local data="$ROOT/run-bench/n$i"; mkdir -p "$data"
    local seeds=(); [ "$i" -ne 0 ] && seeds=(--seeds "$seed")
    "$BIN" serve --data "$data" --host 127.0.0.1 --port "$port" --id "bn$i" \
       --parallel "$parallel" ${seeds[@]+"${seeds[@]}"} > "$ROOT/run-bench/n$i.log" 2>&1 &
    NODE_PIDS+=($!)
  done
  sleep 4
}

echo "mode,op,size_kb,rep,latency_ms,bytes_per_s" > "$RESULTS/raw.csv"

runMatrix() {
  local mode="$1" parallel="$2"
  startCluster "$parallel"
  local E="127.0.0.1:$BASE"
  for kb in "${SIZES_KB[@]}"; do
    local bytes=$((kb*1024))
    head -c "$bytes" /dev/urandom > "$TMP/in_$kb.bin"
    for r in $(seq 1 $REPS); do
      local name="bench/${mode}_${kb}_${r}"
      # CREATE
      local t0 t1 dt bps
      t0=$(nowMs); if ! "$BIN" --entry "$E" create "$TMP/in_$kb.bin" "$name" >/dev/null 2>&1; then
        echo "create failed for $name" >&2; exit 1
      fi; t1=$(nowMs)
      dt=$((t1-t0)); [ "$dt" -eq 0 ] && dt=1; bps=$((bytes*1000/dt))
      echo "$mode,create,$kb,$r,$dt,$bps" >> "$RESULTS/raw.csv"
      # GET
      t0=$(nowMs); if ! "$BIN" --entry "$E" get "$name" "$TMP/out_$kb.bin" >/dev/null 2>&1; then
        echo "get failed for $name" >&2; exit 1
      fi; t1=$(nowMs)
      dt=$((t1-t0)); [ "$dt" -eq 0 ] && dt=1; bps=$((bytes*1000/dt))
      echo "$mode,get,$kb,$r,$dt,$bps" >> "$RESULTS/raw.csv"
    done
    echo "  [$mode] ${kb}KiB done"
  done
}

echo "== sequential replication =="
runMatrix "sequential" 0
echo "== parallel replication =="
runMatrix "parallel" 1

echo "Raw results -> $RESULTS/raw.csv"
python3 "$ROOT/benchmarks/analyze.py" "$RESULTS/raw.csv" "$RESULTS"
python3 "$ROOT/benchmarks/validateResults.py" "$RESULTS/raw.csv"
echo "Done."
