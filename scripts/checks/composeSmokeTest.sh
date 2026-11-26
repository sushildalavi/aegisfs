#!/usr/bin/env bash
# Starts the 10-node Docker Compose cluster and checks it end to end: every node alive, a replicated
# create read back from a different node, a striped file, and reads that survive two stopped nodes.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
COMPOSE=deploy/docker/compose.yaml
command -v docker >/dev/null || { echo "SKIP: docker is not installed"; exit 0; }

# Throwaway credentials whose certificate names every node in the compose file.
export AEGISFS_CREDS_DIR="$ROOT/.dev-compose"
AEGISFS_DEV_DIR="$AEGISFS_CREDS_DIR" \
  AEGISFS_DEV_SANS="$(printf 'DNS:node%s,' $(seq 0 9) | sed 's/,$//')" \
  "$ROOT/scripts/devCreds.sh" >/dev/null

docker compose -f "$COMPOSE" config >/dev/null
cleanup() { docker compose -f "$COMPOSE" down -v >/dev/null 2>&1 || true; }
trap cleanup EXIT
fail() { echo "compose smoke FAIL: $*" >&2; docker compose -f "$COMPOSE" ps >&2 || true; exit 1; }

# run a client command inside a node's container
cli() { local node="$1"; shift; docker compose -f "$COMPOSE" exec -T "$node" aegisfs --entry "$node:9100" "$@"; }
shIn() { local node="$1"; shift; docker compose -f "$COMPOSE" exec -T "$node" sh -c "$*"; }

docker compose -f "$COMPOSE" up -d --build node0 node1 node2 node3 node4 node5 node6 node7 node8 node9

alive=0
for _ in $(seq 1 60); do
  alive=$(cli node0 listMemIds 2>/dev/null | grep -c ' ALIVE ' || true)
  [ "$alive" -eq 10 ] && break
  sleep 2
done
[ "$alive" -eq 10 ] || fail "only $alive of 10 nodes became ALIVE"
echo "[ok] all 10 nodes alive"

shIn node0 'head -c 3000000 /dev/urandom > /tmp/in.bin'
want=$(shIn node0 'sha256sum /tmp/in.bin' | awk '{print $1}')
cli node0 create /tmp/in.bin compose/file | grep -q 'status=OK' || fail "create was not OK"
cli node5 get compose/file /tmp/out.bin | grep -q 'status=OK' || fail "get from node5 failed"
got=$(shIn node5 'sha256sum /tmp/out.bin' | awk '{print $1}')
[ "$want" = "$got" ] || fail "bytes differ between node0 and node5"
echo "[ok] replicated create read back identically from another node"

cli node0 --stripe-mib 1 create /tmp/in.bin compose/striped | grep -q 'status=OK' || fail "striped create was not OK"
cli node7 get compose/striped /tmp/striped.bin | grep -q 'status=OK' || fail "striped get failed"
got=$(shIn node7 'sha256sum /tmp/striped.bin' | awk '{print $1}')
[ "$want" = "$got" ] || fail "striped bytes differ"
echo "[ok] striped file read back identically"

docker compose -f "$COMPOSE" stop node8 node9 >/dev/null
sleep 3
cli node0 get compose/file /tmp/after.bin | grep -q 'status=OK' || fail "read failed after stopping two nodes"
got=$(shIn node0 'sha256sum /tmp/after.bin' | awk '{print $1}')
[ "$want" = "$got" ] || fail "bytes differ after stopping two nodes"
echo "[ok] file still readable with two nodes stopped"

echo "compose smoke PASS"
