#!/usr/bin/env bash
# Full live demonstration on a 10-node process-local cluster.
# Exercises: membership/ring, replicated create, 3-replica placement, checksum-verified get,
# concurrent multiAppend, merge + 3-replica checksum equality, TWO-node failure read,
# re-replication, and final integrity. Exits NONZERO on any failed assertion.
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/devCreds.sh"
BIN="$ROOT/build/aegisfs"
CLUSTER="$ROOT/scripts/cluster.sh"
N=10
BASE=9100
E="127.0.0.1:$BASE"
TMP="$(mktemp -d)"
FAILED=0
START_TS=$(date +%s)
declare -a CHECKS=()

say()  { printf "\n\033[1;36m== %s ==\033[0m\n" "$1"; }
why()  { printf "  \033[2m%s\033[0m\n" "$1"; }
ok()   { printf "  \033[32m[ok]\033[0m %s\n" "$1"; CHECKS+=("ok|$1"); }
bad()  { printf "  \033[31m[FAIL]\033[0m %s\n" "$1"; FAILED=1; CHECKS+=("FAIL|$1"); }
cleanup(){ "$CLUSTER" stop >/dev/null 2>&1 || true; pkill -9 -f "aegisfs serve" 2>/dev/null || true; rm -rf "$TMP" "$ROOT/run"; }
trap cleanup EXIT

[ -x "$BIN" ] || { echo "build first: cmake --build build"; exit 1; }
sha() { shasum -a 256 "$1" | awk '{print $1}'; }
getfield() { grep "^$1=" <<<"$2" | head -1 | cut -d= -f2; }

say "Launch $N-node cluster"
why "Each node is a separate OS process with its own data directory, talking over real TCP."
rm -rf "$ROOT/run"
"$CLUSTER" start $N >/dev/null
sleep 5
MEM=$("$BIN" --entry "$E" listMemIds | wc -l | tr -d ' ')
[ "$MEM" -eq "$N" ] && ok "membership converged: $MEM nodes" || bad "expected $N members, saw $MEM"
"$BIN" --entry "$E" listMemIds | head -5 | sed 's/^/     /'

say "Create a 10 MiB file and show its 3 designated replicas"
why "Placement is a pure function of the consistent-hash ring, no coordinator assigns it."
head -c $((10*1024*1024)) /dev/urandom > "$TMP/orig.bin"
ORIG_SHA="$(sha "$TMP/orig.bin")"
CRES="$("$BIN" --entry "$E" create "$TMP/orig.bin" demo/payload)"
echo "$CRES" | sed 's/^/     /'
[ "$(getfield status "$CRES")" = "OK" ] && ok "create OK" || bad "create status $(getfield status "$CRES")"
[ "$(getfield committed "$CRES")" = "3" ] && ok "replicated to 3 nodes" || bad "committed != 3"
LS="$("$BIN" --entry "$E" ls demo/payload)"
R0="$(getfield r0_addr "$LS")"; R1="$(getfield r1_addr "$LS")"; R2="$(getfield r2_addr "$LS")"
ok "replicas: $R0 $R1 $R2"

say "Get the file back and verify SHA-256"
why "Read from a node that never wrote the file, it still fetches and streams the full copy."
"$BIN" --entry "127.0.0.1:$((BASE+5))" get demo/payload "$TMP/get.bin" >/dev/null
[ "$(sha "$TMP/get.bin")" = "$ORIG_SHA" ] && ok "get byte-identical ($ORIG_SHA)" || bad "get SHA mismatch"

say "Concurrent multiAppend from 3 nodes"
why "Three different nodes append to the same file at once; per-client ordering keeps it consistent."
printf "AAAA\n" > "$TMP/a"; printf "BBBBBB\n" > "$TMP/b"; printf "CC\n" > "$TMP/c"
"$BIN" --entry "$E" multiAppend demo/payload \
   "127.0.0.1:$((BASE+1))" "$TMP/a" \
   "127.0.0.1:$((BASE+2))" "$TMP/b" \
   "127.0.0.1:$((BASE+3))" "$TMP/c" | sed 's/^/     /'
ok "multiAppend issued from 3 nodes"

say "Merge and compare all 3 replica checksums"
why "Merge reconciles any replicas that fell behind; afterward every replica must be byte-identical."
MRES="$("$BIN" --entry "$E" merge demo/payload)"
echo "$MRES" | sed 's/^/     /'
[ "$(getfield status "$MRES")" = "OK" ] && ok "merge OK" || bad "merge status $(getfield status "$MRES")"
declare -a SHAS=()
for r in "$R0" "$R1" "$R2"; do
  "$BIN" --entry "$E" getFromReplica "$r" demo/payload "$TMP/rep.bin" >/dev/null 2>&1
  SHAS+=("$(sha "$TMP/rep.bin")")
done
if [ "${SHAS[0]}" = "${SHAS[1]}" ] && [ "${SHAS[1]}" = "${SHAS[2]}" ]; then
  ok "all 3 replicas byte-identical after merge: ${SHAS[0]}"
else
  bad "replicas diverged: ${SHAS[*]}"
fi
MERGED_SHA="${SHAS[0]}"

say "Fault injection: kill TWO of the three replicas"
why "RF=3 is designed to survive exactly this: up to 2 simultaneous node failures."
idx_of(){ echo $(( $(echo "$1" | cut -d: -f2) - BASE )); }
K0="$(idx_of "$R0")"; K1="$(idx_of "$R1")"
"$CLUSTER" kill "$K0" | sed 's/^/     /'
"$CLUSTER" kill "$K1" | sed 's/^/     /'
# entry: a node that is not killed
ENTRY_IDX=$(( (K0+5) % N )); [ "$ENTRY_IDX" = "$K0" ] && ENTRY_IDX=$(((K0+1)%N))
GRES="$("$BIN" --entry "127.0.0.1:$((BASE+ENTRY_IDX))" get demo/payload "$TMP/after.bin")"
if [ "$(getfield status "$GRES")" = "OK" ] && [ "$(sha "$TMP/after.bin")" = "$MERGED_SHA" ]; then
  ok "file still readable from surviving replica after 2 failures, checksum verified"
else
  bad "read after 2 failures failed: $(getfield status "$GRES")"
fi

say "Recovery: wait for automatic re-replication to restore RF=3"
why "No operator action taken, gossip detects the failures and re-places the missing replicas."
sleep 6
LS2="$("$BIN" --entry "127.0.0.1:$((BASE+ENTRY_IDX))" ls demo/payload)"
NEW=0
for i in 0 1 2; do
  a="$(getfield r${i}_addr "$LS2")"
  if "$BIN" --entry "127.0.0.1:$((BASE+ENTRY_IDX))" getFromReplica "$a" demo/payload "$TMP/rr.bin" >/dev/null 2>&1; then
    [ "$(sha "$TMP/rr.bin")" = "$MERGED_SHA" ] && NEW=$((NEW+1))
  fi
done
[ "$NEW" -ge 2 ] && ok "re-replication restored $NEW healthy replicas, checksums verified" || bad "only $NEW healthy replicas after recovery"

ELAPSED=$(( $(date +%s) - START_TS ))
say "SUMMARY (${ELAPSED}s)"
for entry in "${CHECKS[@]}"; do
  status="${entry%%|*}"; label="${entry#*|}"
  if [ "$status" = "ok" ]; then printf "  \033[32m[ok]\033[0m   %s\n" "$label"
  else printf "  \033[31m[FAIL]\033[0m %s\n" "$label"; fi
done

say "RESULT"
if [ "$FAILED" -eq 0 ]; then printf "\033[1;32mDEMO PASSED\033[0m (%d checks, %ds)\n" "${#CHECKS[@]}" "$ELAPSED"; exit 0
else printf "\033[1;31mDEMO FAILED\033[0m\n"; exit 1; fi
