#!/usr/bin/env bash
# Stop the Cassandra index services. Data volumes are kept unless --purge is given.
#
#   scripts/integrations/cassandraDown.sh           # stop and remove containers, keep volumes
#   scripts/integrations/cassandraDown.sh --purge   # also delete the Cassandra data and the spool volume
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PROJECT="${AEGIS_CASS_PROJECT:-aegis-cass}"
purge=0
for arg in "$@"; do
  case "$arg" in
    --purge) purge=1 ;;
    -h|--help) sed -n '2,6p' "${BASH_SOURCE[0]}"; exit 0 ;;
    *) echo "unknown option: $arg" >&2; exit 2 ;;
  esac
done

args=(-p "$PROJECT" -f "$ROOT/deploy/docker/compose.yaml" -f "$ROOT/deploy/docker/compose.cassandra.yaml" --profile cassandra)
if [ "$purge" = 1 ]; then
  echo "removing containers AND volumes for project $PROJECT"
  docker compose "${args[@]}" down --volumes --remove-orphans
else
  docker compose "${args[@]}" down --remove-orphans
  echo "containers removed; volumes kept (use --purge to delete the data)"
fi
