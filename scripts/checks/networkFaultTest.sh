#!/usr/bin/env bash
# Network-fault tests against the real ten-container cluster: a node cut off from the network,
# packet loss, added latency, and a one-way broken link. The in-process tests simulate these
# inside one process; this one uses the kernel (docker network disconnect, tc netem, iptables).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
command -v docker >/dev/null || { echo "SKIP: docker is not installed"; exit 0; }

export AEGISFS_CREDS_DIR="$ROOT/.dev-compose"
AEGISFS_DEV_DIR="$AEGISFS_CREDS_DIR" \
  AEGISFS_DEV_SANS="$(printf 'DNS:node%s,' $(seq 0 9) | sed 's/,$//')" \
  "$ROOT/scripts/devCreds.sh" >/dev/null

FILES=(-f deploy/docker/compose.yaml -f deploy/docker/compose.faults.yaml)
NETWORK="${COMPOSE_PROJECT_NAME:-aegisfs}_aegis"
NODES=(node0 node1 node2 node3 node4 node5 node6 node7 node8 node9)

cleanup() { docker compose "${FILES[@]}" down -v >/dev/null 2>&1 || true; }
trap cleanup EXIT
fail() { echo "network fault test FAIL: $*" >&2; docker compose "${FILES[@]}" ps >&2 || true; exit 1; }
ok() { echo "[ok] $*"; }

cli() { local node="$1"; shift; docker compose "${FILES[@]}" exec -T "$node" aegisfs --entry "$node:9100" "$@"; }
shIn() { local node="$1"; shift; docker compose "${FILES[@]}" exec -T "$node" sh -c "$*"; }
containerOf() { docker compose "${FILES[@]}" ps -q "$1"; }

# number of members the given node sees in a state (ALIVE, SUSPECT, FAILED)
members() { cli "$1" listMemIds 2>/dev/null | grep -c " $2 " || true; }
awaitAlive() {  # awaitAlive <viewpoint node> <count> <seconds>
  local end=$((SECONDS + $3))
  while [ $SECONDS -lt $end ]; do
    [ "$(members "$1" ALIVE)" -eq "$2" ] && return 0
    sleep 2
  done
  return 1
}
# Watches the cluster for `seconds` and fails if any node is ever reported FAILED. A node being
# SUSPECT for a moment is normal (it is how a refutation gets a chance), FAILED is not.
assertNeverFailed() {  # assertNeverFailed <viewpoint node> <seconds> <what>
  local end=$((SECONDS + $2))
  while [ $SECONDS -lt $end ]; do
    echo "    [$3] non-alive at $1: $(cli "$1" listMemIds | grep -v ' ALIVE ' | tr -s ' ' | tr '\n' ';')"
    [ "$(members "$1" FAILED)" -eq 0 ] || fail "$3 got a healthy node declared FAILED: $(cli "$1" listMemIds | grep -v ' ALIVE ' | tr -s ' ' | tr '\n' ';')"
    sleep 3
  done
  awaitAlive "$1" 10 60 || fail "$3 left some node not ALIVE afterwards: $(cli "$1" listMemIds | grep -v ' ALIVE ' | tr -s ' ' | tr '\n' ';')"
}
stateOf() { cli "$1" listMemIds 2>/dev/null | awk -v n="$2" '$2==n {print $3}'; }
sha() { shIn "$1" "sha256sum $2" | awk '{print $1}'; }
statusOf() { grep -o '^status=[A-Z_]*' | cut -d= -f2; }

docker compose "${FILES[@]}" up -d --build "${NODES[@]}"
awaitAlive node0 10 120 || fail "cluster did not reach 10 ALIVE nodes"
ok "cluster of 10 is up"

shIn node0 'head -c 3000000 /dev/urandom > /tmp/in.bin'
want=$(sha node0 /tmp/in.bin)
st=$( (cli node0 create /tmp/in.bin net/base || true) | statusOf)
[ "$st" = OK ] || fail "baseline create returned $st"

# ---- 1. a node loses the network entirely ----
c9=$(containerOf node9)
docker network disconnect "$NETWORK" "$c9"
end=$((SECONDS + 150)); detected=0
while [ $SECONDS -lt $end ]; do
  s=$(stateOf node0 node9)
  if [ "$s" = FAILED ]; then detected=1; break; fi
  sleep 2
done
[ "$detected" -eq 1 ] || fail "node9 was cut off but never marked FAILED (state: $(stateOf node0 node9))"
ok "isolated node is detected as FAILED"

t0=$SECONDS
out=$(cli node0 create /tmp/in.bin net/during || true)
st=$(printf '%s\n' "$out" | statusOf)
[ "$st" = OK ] || [ "$st" = DEGRADED ] || fail "create during the partition returned: $out"
echo "    create during the partition took $((SECONDS - t0))s"
t0=$SECONDS
out=$(cli node1 get net/base /tmp/during.bin || true)
printf '%s\n' "$out" | grep -q 'status=OK' || fail "read during the partition failed: $out"
echo "    read during the partition took $((SECONDS - t0))s"
[ "$(sha node1 /tmp/during.bin)" = "$want" ] || fail "read during the partition returned different bytes"
ok "reads and writes keep working on the majority side"

docker network connect --alias node9 "$NETWORK" "$c9"
awaitAlive node0 10 120 || fail "node9 did not rejoin as ALIVE (state: $(stateOf node0 node9))"
cli node9 get net/during /tmp/healed.bin | grep -q 'status=OK' || fail "healed node cannot read a file written during the partition"
[ "$(sha node9 /tmp/healed.bin)" = "$want" ] || fail "healed node returned different bytes"
ok "node rejoins after the partition heals and sees what was written meanwhile"

# ---- 2. packet loss ----
for n in node2 node4 node6; do shIn "$n" 'tc qdisc add dev eth0 root netem loss 15%'; done
st=$( (cli node0 create /tmp/in.bin net/lossy || true) | statusOf)
[ "$st" = OK ] || [ "$st" = DEGRADED ] || fail "create under 15% loss returned $st"
cli node5 get net/lossy /tmp/lossy.bin | grep -q 'status=OK' || fail "read under loss failed"
[ "$(sha node5 /tmp/lossy.bin)" = "$want" ] || fail "bytes differ after transfer under loss"
for n in node2 node4 node6; do shIn "$n" 'tc qdisc del dev eth0 root'; done
ok "transfers stay byte-identical under 15% packet loss"
awaitAlive node0 10 120 || fail "membership did not settle after the loss test"

# ---- 3. latency ----
for n in "${NODES[@]}"; do shIn "$n" 'tc qdisc add dev eth0 root netem delay 80ms 20ms'; done
st=$( (cli node0 create /tmp/in.bin net/slow || true) | statusOf)
[ "$st" = OK ] || fail "create with 80ms latency returned $st"
cli node3 get net/slow /tmp/slow.bin | grep -q 'status=OK' || fail "read with latency failed"
[ "$(sha node3 /tmp/slow.bin)" = "$want" ] || fail "bytes differ under latency"
assertNeverFailed node0 20 "80ms of latency"
for n in "${NODES[@]}"; do shIn "$n" 'tc qdisc del dev eth0 root'; done
ok "80ms of added latency causes no false failures and no corruption"

# ---- 4. a one-way broken link ----
ip2=$(shIn node1 'getent hosts node2' | awk '{print $1}')
shIn node1 "iptables -A OUTPUT -d $ip2 -j DROP"
assertNeverFailed node0 30 "a single broken link"
shIn node1 'head -c 3000000 /dev/urandom > /tmp/oneway.bin'
st=$( (cli node1 create /tmp/oneway.bin net/oneway || true) | statusOf)
[ "$st" = OK ] || [ "$st" = DEGRADED ] || fail "create through a node with a broken link returned $st"
cli node5 get net/oneway /tmp/oneway.out | grep -q 'status=OK' || fail "read of the file written through the broken link failed"
[ "$(sha node5 /tmp/oneway.out)" = "$(sha node1 /tmp/oneway.bin)" ] || fail "bytes differ after writing through a broken link"
shIn node1 "iptables -D OUTPUT -d $ip2 -j DROP"
ok "one broken link does not get a healthy node declared dead"

echo "network fault test PASS"
