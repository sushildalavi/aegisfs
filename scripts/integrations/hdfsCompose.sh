#!/usr/bin/env bash
# Shared setup for the hdfs*.sh helpers. Source it; it defines `dc` (docker compose for the
# aegis-hdfs project with the core file and the HDFS overlay) and the credential variables.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
export AEGISFS_CREDS_DIR="${AEGISFS_CREDS_DIR:-$ROOT/.dev-compose}"
# Host ports for the three AegisFS nodes, away from the 9100-9109 range other scripts use.
export AEGISFS_PORT0="${AEGISFS_PORT0:-9600}"
export AEGISFS_PORT1="${AEGISFS_PORT1:-9601}"
export AEGISFS_PORT2="${AEGISFS_PORT2:-9602}"
HDFS_PROJECT="aegis-hdfs"
dc() {
  docker compose -p "$HDFS_PROJECT" -f "$ROOT/deploy/docker/compose.yaml" \
    -f "$ROOT/deploy/docker/compose.hdfs.yaml" --profile hdfs "$@"
}
