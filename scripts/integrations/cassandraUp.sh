#!/usr/bin/env bash
# Start the Cassandra service of the optional metadata index and apply the schema.
#
#   scripts/integrations/cassandraUp.sh              # Cassandra + schema init
#   scripts/integrations/cassandraUp.sh --indexer    # also start the indexer service
#   scripts/integrations/cassandraUp.sh --no-init    # Cassandra only, no schema init (tests make their own)
#
# Uses Compose project "aegis-cass" (override with AEGIS_CASS_PROJECT). Only Cassandra's native
# port is published, on 127.0.0.1:9042. The 10 AegisFS nodes are not started.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PROJECT="${AEGIS_CASS_PROJECT:-aegis-cass}"
compose() {
  docker compose -p "$PROJECT" -f "$ROOT/deploy/docker/compose.yaml" \
    -f "$ROOT/deploy/docker/compose.cassandra.yaml" --profile cassandra "$@"
}

with_indexer=0
with_init=1
for arg in "$@"; do
  case "$arg" in
    --indexer) with_indexer=1 ;;
    --no-init) with_init=0 ;;
    -h|--help) sed -n '2,10p' "${BASH_SOURCE[0]}"; exit 0 ;;
    *) echo "unknown option: $arg" >&2; exit 2 ;;
  esac
done

compose up -d cassandra
echo "waiting for Cassandra to become healthy (the first start takes a minute or two)..."
for _ in $(seq 1 90); do
  state="$(docker inspect -f '{{.State.Health.Status}}' "${PROJECT}-cassandra-1" 2>/dev/null || echo starting)"
  [ "$state" = healthy ] && break
  sleep 5
done
[ "$state" = healthy ] || { echo "Cassandra did not become healthy" >&2; compose logs --tail 40 cassandra >&2; exit 1; }

if [ "$with_init" = 0 ]; then
  :
elif [ "$with_indexer" = 1 ]; then
  compose up -d cassandra-init cassandra-indexer
else
  compose up cassandra-init
fi
echo "Cassandra is up on 127.0.0.1:9042 (project $PROJECT)."
