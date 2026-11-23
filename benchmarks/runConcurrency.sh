#!/usr/bin/env bash
# Concurrency-scaling benchmark: throughput (ops/sec) of create+append as the number of concurrent
# clients grows (1, 5, 10, 20), each client driving a distinct file. Process-local 5-node cluster.
# Emits benchmarks/results/concurrency.csv. Plain wall-clock timings.
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/build/aegisfs"
RESULTS="$ROOT/benchmarks/results"
TMP="$(mktemp -d)"
N=5
BASE=9400
OPS_PER_CLIENT=20          # create 1 + 19 appends per client
CLIENT_COUNTS=(1 5 10 20)
mkdir -p "$RESULTS"
cleanup(){ pkill -9 -f "aegisfs serve" 2>/dev/null || true; rm -rf "$TMP" "$ROOT/run-bench"; }
trap cleanup EXIT
[ -x "$BIN" ] || { echo "build first"; exit 1; }
now_ms(){ python3 -c 'import time;print(int(time.time()*1000))'; }

start_cluster(){
  pkill -9 -f "aegisfs serve" 2>/dev/null || true
  rm -rf "$ROOT/run-bench"; mkdir -p "$ROOT/run-bench"
  local seed="127.0.0.1:$BASE"
  for i in $(seq 0 $((N-1))); do
    local port=$((BASE+i)); local data="$ROOT/run-bench/n$i"; mkdir -p "$data"
    local seeds=(); [ "$i" -ne 0 ] && seeds=(--seeds "$seed")
    "$BIN" serve --data "$data" --host 127.0.0.1 --port "$port" --id "cn$i" \
       ${seeds[@]+"${seeds[@]}"} > "$ROOT/run-bench/n$i.log" 2>&1 &
  done
  sleep 4
}

head -c 4096 /dev/urandom > "$TMP/payload"
echo "clients,total_ops,wall_ms,ops_per_s" > "$RESULTS/concurrency.csv"

start_cluster
for C in "${CLIENT_COUNTS[@]}"; do
  local_total=$((C*OPS_PER_CLIENT))
  t0=$(now_ms)
  pids=()
  for c in $(seq 0 $((C-1))); do
    (
      E="127.0.0.1:$((BASE + (c % N)))"
      name="conc/c${C}_${c}"
      if ! "$BIN" --entry "$E" create "$TMP/payload" "$name" >/dev/null 2>&1; then
        echo "create failed for $name" >&2; exit 1
      fi
      for a in $(seq 1 $((OPS_PER_CLIENT-1))); do
        if ! "$BIN" --entry "$E" append "$TMP/payload" "$name" >/dev/null 2>&1; then
          echo "append failed for $name (attempt $a)" >&2; exit 1
        fi
      done
    ) &
    pids+=($!)
  done
  for p in "${pids[@]}"; do wait "$p"; done
  t1=$(now_ms); dt=$((t1-t0)); [ "$dt" -eq 0 ] && dt=1
  ops_s=$(python3 -c "print(round($local_total*1000/$dt,1))")
  echo "$C,$local_total,$dt,$ops_s" >> "$RESULTS/concurrency.csv"
  echo "  clients=$C ops=$local_total wall=${dt}ms -> ${ops_s} ops/s"
done

echo "Wrote $RESULTS/concurrency.csv"
