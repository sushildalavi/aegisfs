#!/usr/bin/env bash
# Verify that every designated replica of a file holds byte-identical content (matching SHA-256).
# Usage: scripts/checks/verifyIntegrity.sh <entry host:port> <aegis-name>
# Exits 0 if all replicas agree, nonzero otherwise.
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${AEGISFS_BIN:-$ROOT/build/aegisfs}"
E="${1:?usage: verifyIntegrity.sh <entry> <name>}"
NAME="${2:?usage: verifyIntegrity.sh <entry> <name>}"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
sha(){ shasum -a 256 "$1" | awk '{print $1}'; }

LS="$("$BIN" --entry "$E" ls "$NAME")" || { echo "ls failed"; exit 2; }
RF="$(echo "$LS" | grep '^replicas=' | cut -d= -f2)"
[ -n "${RF:-}" ] || { echo "no replica info for $NAME"; exit 2; }

echo "File: $NAME  (fileId=$(echo "$LS" | grep '^fileId=' | cut -d= -f2))"
declare -a SHAS=()
for i in $(seq 0 $((RF-1))); do
  addr="$(echo "$LS" | grep "^r${i}_addr=" | cut -d= -f2)"
  out="$TMP/r$i.bin"
  if "$BIN" --entry "$E" getFromReplica "$addr" "$NAME" "$out" >/dev/null 2>&1; then
    s="$(sha "$out")"
    echo "  replica $i  $addr  $s"
    SHAS+=("$s")
  else
    echo "  replica $i  $addr  UNREACHABLE/CORRUPT"
    SHAS+=("UNREACHABLE")
  fi
done

ref=""
for s in "${SHAS[@]}"; do
  [ "$s" = "UNREACHABLE" ] && { echo "RESULT: incomplete (a replica was unreachable)"; exit 1; }
  if [ -z "$ref" ]; then ref="$s"; elif [ "$s" != "$ref" ]; then echo "RESULT: DIVERGED"; exit 1; fi
done
echo "RESULT: all $RF replicas identical ($ref)"
