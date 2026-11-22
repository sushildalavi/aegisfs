#!/usr/bin/env bash
# Launch / stop a process-local AegisFS cluster (one OS process per node).
# Usage:
#   scripts/cluster.sh start [N]     # default N=5
#   scripts/cluster.sh stop
#   scripts/cluster.sh status
#   scripts/cluster.sh kill <idx>    # SIGKILL node <idx> (0-based)
#   scripts/cluster.sh restart <idx> # relaunch node <idx> with its persistent data dir
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="${AEGISFS_BIN:-$ROOT/build/aegisfs}"
RUN="$ROOT/run"
BASE_PORT="${AEGISFS_BASE_PORT:-9100}"
HOST="127.0.0.1"

mkdir -p "$RUN"

start() {
  local n="${1:-5}"
  echo "Starting $n-node cluster on $HOST:$BASE_PORT..$((BASE_PORT+n-1))"
  local seed="$HOST:$BASE_PORT"
  for i in $(seq 0 $((n-1))); do
    local port=$((BASE_PORT+i))
    local data="$RUN/node$i"
    mkdir -p "$data"
    local seeds_arg=()
    if [ "$i" -ne 0 ]; then seeds_arg=(--seeds "$seed"); fi
    "$BIN" serve --data "$data" --host "$HOST" --port "$port" --id "node$i" ${seeds_arg[@]+"${seeds_arg[@]}"} \
      > "$RUN/node$i.log" 2>&1 &
    echo $! > "$RUN/node$i.pid"
    echo "  node$i pid=$! port=$port data=$data"
  done
  echo "$n" > "$RUN/.count"
  echo "Cluster up. Entry node: $seed"
}

stop() {
  echo "Stopping cluster..."
  for pidf in "$RUN"/node*.pid; do
    [ -e "$pidf" ] || continue
    local pid; pid="$(cat "$pidf")"
    if kill -0 "$pid" 2>/dev/null; then kill "$pid" 2>/dev/null || true; fi
    rm -f "$pidf"
  done
  echo "Stopped."
}

status() {
  for pidf in "$RUN"/node*.pid; do
    [ -e "$pidf" ] || continue
    local pid; pid="$(cat "$pidf")"
    local name; name="$(basename "$pidf" .pid)"
    if kill -0 "$pid" 2>/dev/null; then echo "$name RUNNING pid=$pid"; else echo "$name DOWN"; fi
  done
}

kill_node() {
  local idx="$1"; local pidf="$RUN/node$idx.pid"
  [ -e "$pidf" ] || { echo "no such node $idx"; exit 1; }
  local pid; pid="$(cat "$pidf")"
  kill -9 "$pid" 2>/dev/null || true
  rm -f "$pidf"
  echo "killed node$idx (pid=$pid)"
}

restart_node() {
  local idx="$1"; local port=$((BASE_PORT+idx)); local data="$RUN/node$idx"
  local seed="$HOST:$BASE_PORT"
  local seeds_arg=(--seeds "$seed")
  "$BIN" serve --data "$data" --host "$HOST" --port "$port" --id "node$idx" ${seeds_arg[@]+"${seeds_arg[@]}"} \
    > "$RUN/node$idx.log" 2>&1 &
  echo $! > "$RUN/node$idx.pid"
  echo "restarted node$idx pid=$! port=$port"
}

case "${1:-}" in
  start) start "${2:-5}" ;;
  stop) stop ;;
  status) status ;;
  kill) kill_node "${2:?node index}" ;;
  restart) restart_node "${2:?node index}" ;;
  *) echo "usage: $0 {start [N]|stop|status|kill <idx>|restart <idx>}"; exit 2 ;;
esac
