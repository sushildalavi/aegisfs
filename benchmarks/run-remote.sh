#!/usr/bin/env bash
# Same create/get timing matrix as run-matrix.sh, but against an already-running cluster instead of
# one this script starts itself, for measuring over a real network between separate machines
# rather than loopback on one host. This script does not start, stop, or otherwise control the
# cluster; point it at any live entry node and it just measures.
#
# Usage: run-remote.sh <entry_host:port> [output_name]
#   benchmarks/run-remote.sh 10.0.0.5:9100 wan
#
# Setup (on separate machines or network-isolated hosts):
#   host A: aegisfs serve --data /tmp/a --host 0.0.0.0 --port 9100 --id a
#   host B: aegisfs serve --data /tmp/b --host 0.0.0.0 --port 9100 --id b --seeds <hostA>:9100
#   host C: aegisfs serve --data /tmp/c --host 0.0.0.0 --port 9100 --id c --seeds <hostA>:9100
#   then from any machine that can reach host A:
#   benchmarks/run-remote.sh <hostA-ip>:9100
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/build/aegisfs"
RESULTS="$ROOT/benchmarks/results"
TMP="$(mktemp -d)"
REPS="${REPS:-5}"
SIZES_KB=(1 1024 10240 102400)

ENTRY="${1:?usage: run-remote.sh <entry_host:port> [output_name]}"
NAME_TAG="${2:-remote}"
OUT="$RESULTS/raw_${NAME_TAG}.csv"
mkdir -p "$RESULTS"
trap 'rm -rf "$TMP"' EXIT
[ -x "$BIN" ] || { echo "build first: cmake --build build"; exit 1; }

now_ms(){ python3 -c 'import time;print(int(time.time()*1000))'; }

if ! "$BIN" --entry "$ENTRY" health >/dev/null 2>&1; then
  echo "cannot reach $ENTRY, start a real cluster and pass a live entry address" >&2
  exit 1
fi

echo "mode,op,size_kb,rep,latency_ms,bytes_per_s" > "$OUT"
for kb in "${SIZES_KB[@]}"; do
  bytes=$((kb*1024))
  head -c "$bytes" /dev/urandom > "$TMP/in_$kb.bin"
  for r in $(seq 1 "$REPS"); do
    fname="bench/${NAME_TAG}_${kb}_${r}"
    t0=$(now_ms)
    "$BIN" --entry "$ENTRY" create "$TMP/in_$kb.bin" "$fname" >/dev/null 2>&1 || { echo "create failed for $fname" >&2; exit 1; }
    t1=$(now_ms); dt=$((t1-t0)); [ "$dt" -eq 0 ] && dt=1; bps=$((bytes*1000/dt))
    echo "$NAME_TAG,create,$kb,$r,$dt,$bps" >> "$OUT"

    t0=$(now_ms)
    "$BIN" --entry "$ENTRY" get "$fname" "$TMP/out_$kb.bin" >/dev/null 2>&1 || { echo "get failed for $fname" >&2; exit 1; }
    t1=$(now_ms); dt=$((t1-t0)); [ "$dt" -eq 0 ] && dt=1; bps=$((bytes*1000/dt))
    echo "$NAME_TAG,get,$kb,$r,$dt,$bps" >> "$OUT"
  done
  echo "  [${NAME_TAG}] ${kb}KiB done"
done

echo "Raw results -> $OUT"
