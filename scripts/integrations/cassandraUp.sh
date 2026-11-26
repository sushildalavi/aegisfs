#!/usr/bin/env bash
# Start the Cassandra service of the optional metadata index and apply the schema.
#
#   scripts/integrations/cassandraUp.sh              # Cassandra + schema init
#   scripts/integrations/cassandraUp.sh --indexer    # also start the indexer service
#   scripts/integrations/cassandraUp.sh --no-init    # Cassandra only, no schema init (tests make their own)
#   scripts/integrations/cassandraUp.sh --secure     # test variant with TLS + password login (see compose file)
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

withIndexer=0
withInit=1
secure=0
for arg in "$@"; do
  case "$arg" in
    --indexer) withIndexer=1 ;;
    --no-init) withInit=0 ;;
    --secure) secure=1 ;;
    -h|--help) sed -n '2,11p' "${BASH_SOURCE[0]}"; exit 0 ;;
    *) echo "unknown option: $arg" >&2; exit 2 ;;
  esac
done

service=cassandra
if [ "$secure" = 1 ]; then
  service=cassandra-secure
  withInit=0
  AEGIS_CASS_SECURE_DIR="$("$ROOT/deploy/docker/cassandra/secureCreds.sh")"
  export AEGIS_CASS_SECURE_DIR
fi
compose up -d "$service"
echo "waiting for Cassandra to become healthy (the first start takes a minute or two)..."
for _ in $(seq 1 90); do
  state="$(docker inspect -f '{{.State.Health.Status}}' "${PROJECT}-${service}-1" 2>/dev/null || echo starting)"
  [ "$state" = healthy ] && break
  sleep 5
done
[ "$state" = healthy ] || { echo "Cassandra did not become healthy" >&2; compose logs --tail 40 "$service" >&2; exit 1; }

if [ "$secure" = 1 ]; then
  PY="$ROOT/extensions/integrations/.venv-cass/bin/python"
  [ -x "$PY" ] || PY=python3
  "$PY" "$ROOT/deploy/docker/cassandra/secureSetup.py" "$AEGIS_CASS_SECURE_DIR"
elif [ "$withInit" = 0 ]; then
  :
elif [ "$withIndexer" = 1 ]; then
  compose up -d cassandra-init cassandra-indexer
else
  compose up cassandra-init
fi
echo "Cassandra is up on 127.0.0.1:9042 (project $PROJECT)."
