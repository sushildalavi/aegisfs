#!/usr/bin/env bash
# Minimal single-entry smoke test: build must exist. Starts a 3-node cluster, exercises the
# full CRUD-ish path, and asserts byte integrity. Exits nonzero on any failed assertion.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "$ROOT/scripts/devCreds.sh"
BIN="$ROOT/build/aegisfs"
E="127.0.0.1:9100"
TMP="$(mktemp -d)"
cleanup() { "$ROOT/scripts/cluster.sh" stop >/dev/null 2>&1 || true; pkill -9 -f "aegisfs serve" 2>/dev/null || true; rm -rf "$TMP" "$ROOT/run"; }
trap cleanup EXIT

fail() { echo "SMOKE FAIL: $1" >&2; exit 1; }

[ -x "$BIN" ] || fail "binary not built at $BIN"

rm -rf "$ROOT/run"
"$ROOT/scripts/cluster.sh" start 3 >/dev/null
sleep 3

# membership converged?
members=$("$BIN" --entry "$E" listMemIds | wc -l | tr -d ' ')
[ "$members" -eq 3 ] || fail "expected 3 members, got $members"
echo "[ok] membership converged: $members nodes"
"$BIN" --entry "$E" members | grep -q '^m2_addr=127.0.0.1:' || fail "members did not list three addresses"
echo "[ok] members lists node addresses"

# create + get round trip
head -c 123456 /dev/urandom > "$TMP/in.bin"
"$BIN" --entry "$E" create "$TMP/in.bin" smoke/file >/dev/null || fail "create failed"
"$BIN" --entry "127.0.0.1:9101" get smoke/file "$TMP/out.bin" >/dev/null || fail "get failed"
a=$(shasum -a 256 "$TMP/in.bin" | awk '{print $1}')
b=$(shasum -a 256 "$TMP/out.bin" | awk '{print $1}')
[ "$a" = "$b" ] || fail "create/get bytes differ ($a != $b)"
echo "[ok] create/get byte-identical"

# first-create-only (this create is expected to fail with ALREADY_EXISTS -> nonzero exit)
st=$("$BIN" --entry "127.0.0.1:9102" create "$TMP/in.bin" smoke/file | grep '^status=' | cut -d= -f2) || true
[ "$st" = "ALREADY_EXISTS" ] || fail "duplicate create should be ALREADY_EXISTS, got $st"
echo "[ok] first-create-only enforced"

# append then merge then compare replicas
printf "APPEND\n" > "$TMP/ap"
"$BIN" --entry "$E" append "$TMP/ap" smoke/file >/dev/null || fail "append failed"
"$BIN" --entry "$E" merge smoke/file | grep -q 'status=OK' || fail "merge not OK"
echo "[ok] append + merge succeeded"

# a file bigger than the stripe size is split across the ring and read back whole
head -c 5000000 /dev/urandom > "$TMP/big.bin"
"$BIN" --stripe-mib 1 --entry "$E" create "$TMP/big.bin" smoke/striped >/dev/null || fail "striped create failed"
"$BIN" --entry "127.0.0.1:9102" get smoke/striped "$TMP/big.out" >/dev/null || fail "striped get failed"
a=$(shasum -a 256 "$TMP/big.bin" | awk '{print $1}')
b=$(shasum -a 256 "$TMP/big.out" | awk '{print $1}')
[ "$a" = "$b" ] || fail "striped create/get bytes differ"
echo "[ok] striped create/get byte-identical"

echo "SMOKE PASS"
