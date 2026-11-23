#!/usr/bin/env bash
# AegisFS benchmark runner. Measures create/get latency across file sizes under two replication
# fan-out strategies (sequential vs parallel) on a process-local 5-node cluster, and append
# throughput. Emits raw CSV to benchmarks/results/raw.csv. Plain wall-clock timings.
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/build/aegisfs"
RESULTS="$ROOT/benchmarks/results"
TMP="$(mktemp -d)"
N=5
BASE=9300          # distinct port range from the demo/smoke clusters
REPS=5             # repetitions per data point
SIZES_KB=(1 1024 10240 102400)   # 1 KiB, 1 MiB, 10 MiB, 100 MiB
mkdir -p "$RESULTS"

cleanup(){ pkill -9 -f "aegisfs serve" 2>/dev/null || true; rm -rf "$TMP" "$ROOT/run-bench"; }
trap cleanup EXIT
[ -x "$BIN" ] || { echo "build first"; exit 1; }

now_ms(){ python3 -c 'import time;print(int(time.time()*1000))'; }

start_cluster() {
  local parallel="$1"
  pkill -9 -f "aegisfs serve" 2>/dev/null || true
  rm -rf "$ROOT/run-bench"; mkdir -p "$ROOT/run-bench"
  local seed="127.0.0.1:$BASE"
  for i in $(seq 0 $((N-1))); do
    local port=$((BASE+i)); local data="$ROOT/run-bench/n$i"; mkdir -p "$data"
    local seeds=(); [ "$i" -ne 0 ] && seeds=(--seeds "$seed")
    "$BIN" serve --data "$data" --host 127.0.0.1 --port "$port" --id "bn$i" \
       --parallel "$parallel" ${seeds[@]+"${seeds[@]}"} > "$ROOT/run-bench/n$i.log" 2>&1 &
  done
  sleep 4
}

echo "mode,op,size_kb,rep,latency_ms,bytes_per_s" > "$RESULTS/raw.csv"

run_matrix() {
  local mode="$1" parallel="$2"
  start_cluster "$parallel"
  local E="127.0.0.1:$BASE"
  for kb in "${SIZES_KB[@]}"; do
    local bytes=$((kb*1024))
    head -c "$bytes" /dev/urandom > "$TMP/in_$kb.bin"
    for r in $(seq 1 $REPS); do
      local name="bench/${mode}_${kb}_${r}"
      # CREATE
      local t0 t1 dt bps
      t0=$(now_ms); if ! "$BIN" --entry "$E" create "$TMP/in_$kb.bin" "$name" >/dev/null 2>&1; then
        echo "create failed for $name" >&2; exit 1
      fi; t1=$(now_ms)
      dt=$((t1-t0)); [ "$dt" -eq 0 ] && dt=1; bps=$((bytes*1000/dt))
      echo "$mode,create,$kb,$r,$dt,$bps" >> "$RESULTS/raw.csv"
      # GET
      t0=$(now_ms); if ! "$BIN" --entry "$E" get "$name" "$TMP/out_$kb.bin" >/dev/null 2>&1; then
        echo "get failed for $name" >&2; exit 1
      fi; t1=$(now_ms)
      dt=$((t1-t0)); [ "$dt" -eq 0 ] && dt=1; bps=$((bytes*1000/dt))
      echo "$mode,get,$kb,$r,$dt,$bps" >> "$RESULTS/raw.csv"
    done
    echo "  [$mode] ${kb}KiB done"
  done
}

echo "== sequential replication =="
run_matrix "sequential" 0
echo "== parallel replication =="
run_matrix "parallel" 1

echo "Raw results -> $RESULTS/raw.csv"
python3 "$ROOT/benchmarks/analyze.py" "$RESULTS/raw.csv" "$RESULTS"
python3 "$ROOT/benchmarks/validate-results.py" "$RESULTS/raw.csv"
echo "Done."
