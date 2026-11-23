#!/usr/bin/env bash
# Process-local reliability checks using real TCP nodes and process signals.
# Exit 77 means the host cannot bind localhost sockets and the suite was skipped.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="${AEGISFS_BIN:-$ROOT/build/aegisfs}"
BASE="${AEGISFS_BASE_PORT:-9500}"
RUN="$ROOT/run"
TMP="$(mktemp -d)"
FAILED=0

pass() { printf '[pass] %s\n' "$1"; }
fail() { printf '[fail] %s\n' "$1" >&2; FAILED=1; }
cleanup() {
  AEGISFS_BASE_PORT="$BASE" "$ROOT/scripts/cluster.sh" stop >/dev/null 2>&1 || true
  rm -rf "$TMP" "$RUN"
}
trap cleanup EXIT

[ -x "$BIN" ] || { echo "build first: $BIN" >&2; exit 2; }

python3 - "$BASE" <<'PY'
import socket
import sys

sock = socket.socket()
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
try:
    sock.bind(("127.0.0.1", int(sys.argv[1])))
except OSError as error:
    print(f"SKIP: localhost sockets unavailable: {error}")
    raise SystemExit(77)
finally:
    sock.close()
PY
probe=$?
[ "$probe" -eq 0 ] || exit "$probe"

field() { printf '%s\n' "$1" | awk -F= -v key="$2" '$1 == key { print $2; exit }'; }
sha() { shasum -a 256 "$1" | awk '{print $1}'; }

rm -rf "$RUN"
AEGISFS_BASE_PORT="$BASE" "$ROOT/scripts/cluster.sh" start 5 >/dev/null
entry="127.0.0.1:$BASE"
converged=0
for _ in $(seq 1 40); do
  converged=1
  for p in 0 1 2 3 4; do
    members="$("$BIN" --entry "127.0.0.1:$((BASE + p))" listMemIds 2>/dev/null | wc -l | tr -d ' ')"
    [ "$members" -eq 5 ] || { converged=0; break; }
  done
  [ "$converged" -eq 1 ] && break
  sleep .25
done
[ "$converged" -eq 1 ] && pass "five-node membership converged" || fail "membership did not converge on all nodes"

head -c 32768 /dev/zero > "$TMP/base"
"$BIN" --entry "$entry" create "$TMP/base" reliability/file >/dev/null 2>&1 \
  && pass "replicated create acknowledged" || fail "replicated create failed"

lsOutput="$("$BIN" --entry "$entry" ls reliability/file 2>/dev/null)"
r0="$(field "$lsOutput" r0_addr)"
r1="$(field "$lsOutput" r1_addr)"
r2="$(field "$lsOutput" r2_addr)"
[ -n "$r0" ] && [ -n "$r1" ] && [ -n "$r2" ] \
  && pass "three designated replicas reported" || fail "replica placement missing"

if [ -n "$r0" ] && [ -n "$r1" ]; then
  i0=$((10#$(printf '%s' "$r0" | cut -d: -f2) - BASE))
  i1=$((10#$(printf '%s' "$r1" | cut -d: -f2) - BASE))
  AEGISFS_BASE_PORT="$BASE" "$ROOT/scripts/cluster.sh" kill "$i0" >/dev/null
  AEGISFS_BASE_PORT="$BASE" "$ROOT/scripts/cluster.sh" kill "$i1" >/dev/null
  survivor=$(( (i0 + 1) % 5 ))
  [ "$survivor" -eq "$i1" ] && survivor=$(( (survivor + 1) % 5 ))
  out="$TMP/two-failure"
  if "$BIN" --entry "127.0.0.1:$((BASE + survivor))" get reliability/file "$out" >/dev/null 2>&1 \
      && [ "$(sha "$out")" = "$(sha "$TMP/base")" ]; then
    pass "read survives two replica failures"
  else
    fail "read after two replica failures"
  fi
fi

probeEntry="${survivor:-0}"
probePort=$((BASE + probeEntry))
python3 - "$probePort" <<'PY'
import socket
import sys

sock = socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=2)
sock.sendall(b"not-an-aegisfs-frame")
sock.close()
PY
if "$BIN" --entry "127.0.0.1:$probePort" health >/dev/null 2>&1; then
  pass "malformed frame does not kill the listener"
else
  fail "listener health check after malformed frame"
fi

if [ "$FAILED" -eq 0 ]; then
  echo "RELIABILITY PASS"
else
  echo "RELIABILITY FAIL" >&2
fi
exit "$FAILED"
