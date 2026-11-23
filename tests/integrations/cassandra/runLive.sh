#!/usr/bin/env bash
# Run the Cassandra integration tests against real services.
#
#   tests/integrations/cassandra/runLive.sh            # unit tests, then live tests
#   KEEP_UP=1 tests/integrations/cassandra/runLive.sh  # leave Cassandra and AegisFS running afterwards
#
# It starts a 3-node process-local AegisFS cluster (ports 9300-9302, run dir /tmp/aegis-cass) and the
# Cassandra container (Compose project aegis-cass, 127.0.0.1:9042), runs the tests with
# AEGISFS_INT_CASSANDRA_LIVE=1 so a missing service is a failure, and stops only what it started.
# Needs: Docker, a built build/aegisfs, and python3 (a venv is created in extensions/integrations/.venv-cass).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
VENV="$ROOT/extensions/integrations/.venv-cass"
export AEGISFS_RUN_DIR="${AEGISFS_RUN_DIR:-/tmp/aegis-cass}"
export AEGISFS_BASE_PORT="${AEGISFS_BASE_PORT:-9300}"
export AEGIS_CASS_RUN_DIR="$AEGISFS_RUN_DIR" AEGIS_CASS_BASE_PORT="$AEGISFS_BASE_PORT" AEGIS_CASS_NODES=3
PROJECT="${AEGIS_CASS_PROJECT:-aegis-cass}"
export AEGIS_CASS_CONTAINER="${PROJECT}-cassandra-1"

[ -x "$ROOT/build/aegisfs" ] || { echo "build/aegisfs is missing; build the project first" >&2; exit 1; }
if [ ! -x "$VENV/bin/python" ]; then
  python3 -m venv "$VENV"
  "$VENV/bin/pip" install -q -r "$ROOT/extensions/integrations/requirements.txt"
fi

started_cluster=0
started_cassandra=0
cleanup() {
  status=$?
  if [ "${KEEP_UP:-0}" != 1 ]; then
    [ "$started_cluster" = 1 ] && "$ROOT/scripts/cluster.sh" stop >/dev/null 2>&1 || true
    [ "$started_cassandra" = 1 ] && "$ROOT/scripts/integrations/cassandraDown.sh" >/dev/null 2>&1 || true
  fi
  exit "$status"
}
trap cleanup EXIT

# shellcheck disable=SC1091
source "$ROOT/scripts/devCreds.sh"
export AEGISFS_INT_AEGISFS_BIN="$ROOT/build/aegisfs"
export AEGISFS_INT_AEGISFS_ENTRY="127.0.0.1:$AEGISFS_BASE_PORT"

if ! "$ROOT/build/aegisfs" --entry "$AEGISFS_INT_AEGISFS_ENTRY" health >/dev/null 2>&1; then
  "$ROOT/scripts/cluster.sh" start 3
  started_cluster=1
fi
for _ in $(seq 1 30); do
  "$ROOT/build/aegisfs" --entry "$AEGISFS_INT_AEGISFS_ENTRY" members 2>/dev/null | grep -q '^count=3' && break
  sleep 1
done

if [ "$(docker inspect -f '{{.State.Running}}' "$AEGIS_CASS_CONTAINER" 2>/dev/null || echo false)" != true ]; then
  "$ROOT/scripts/integrations/cassandraUp.sh" --no-init
  started_cassandra=1
fi

cd "$ROOT/tests/integrations/cassandra"
echo "== unit tests =="
"$VENV/bin/python" -W error::ResourceWarning -m unittest testCassandraModel testCassandraStore \
  testCassandraSpool testCassandraSchema testCassandraIndexer
echo "== live tests =="
AEGISFS_INT_CASSANDRA_LIVE=1 "$VENV/bin/python" -W error::ResourceWarning -m unittest -v testCassandraLive
