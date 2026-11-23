#!/usr/bin/env bash
# Start the small HDFS test cluster (1 NameNode, 2 DataNodes), the connector container, and by
# default three AegisFS nodes, then wait until they are usable.
#
#   scripts/integrations/hdfsUp.sh               HDFS + connector + AegisFS node0-node2
#   scripts/integrations/hdfsUp.sh --no-aegis    HDFS and connector only
#
# Throwaway TLS credentials are made in .dev-compose if none exist there. Volumes are kept
# between runs; scripts/integrations/hdfsDown.sh --purge removes them.
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/hdfsCompose.sh"
cd "$ROOT"
command -v docker >/dev/null || { echo "docker is not installed" >&2; exit 1; }

withAegis=1
for arg in "$@"; do
  case "$arg" in
    --no-aegis) withAegis=0 ;;
    -h|--help) sed -n '2,9p' "$0"; exit 0 ;;
    *) echo "unknown option: $arg" >&2; exit 2 ;;
  esac
done

if [ ! -f "$AEGISFS_CREDS_DIR/token" ]; then
  AEGISFS_DEV_DIR="$AEGISFS_CREDS_DIR" \
    AEGISFS_DEV_SANS="$(printf 'DNS:node%s,' $(seq 0 9) | sed 's/,$//')" \
    "$ROOT/scripts/devCreds.sh" >/dev/null
fi

services=(namenode datanode1 datanode2 connector)
[ "$withAegis" = 1 ] && services+=(node0 node1 node2)
dc up -d --build "${services[@]}"

echo "waiting for the NameNode and both DataNodes..."
live=0
for _ in $(seq 1 90); do
  live=$(curl -s 'http://127.0.0.1:'"${AEGISFS_HDFS_NN_PORT:-9870}"'/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState' \
    | grep -o '"NumLiveDataNodes" : [0-9]*' | grep -o '[0-9]*$' || true)
  [ "${live:-0}" = 2 ] && break
  sleep 3
done
[ "${live:-0}" = 2 ] || { echo "only ${live:-0} DataNode(s) registered" >&2; exit 1; }
dc exec -T namenode hdfs dfsadmin -safemode wait >/dev/null

# The test directory belongs to the "aegisfs" user the connector authenticates as. It is made by
# the HDFS superuser inside the NameNode container.
dc exec -T namenode hdfs dfs -mkdir -p /aegisfs-test
dc exec -T namenode hdfs dfs -chown aegisfs:supergroup /aegisfs-test
echo "HDFS ready (replication 2, user aegisfs, directory /aegisfs-test)"

if [ "$withAegis" = 1 ]; then
  echo "waiting for the AegisFS nodes..."
  alive=0
  for _ in $(seq 1 60); do
    alive=$(dc exec -T connector aegisfs --entry node0:9100 listMemIds 2>/dev/null | grep -c ' ALIVE ' || true)
    [ "$alive" = 3 ] && break
    sleep 2
  done
  [ "$alive" = 3 ] || { echo "only $alive AegisFS node(s) alive" >&2; exit 1; }
  echo "AegisFS ready (3 nodes, host ports $AEGISFS_PORT0-$AEGISFS_PORT2)"
fi
