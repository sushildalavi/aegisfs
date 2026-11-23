#!/usr/bin/env bash
# Bring up the HDFS test stack, run the unit tests and the live tests against it, and stop it.
#
#   tests/integrations/hdfs/runLive.sh              run everything, then stop the containers
#   tests/integrations/hdfs/runLive.sh --keep       leave the stack running afterwards
#   tests/integrations/hdfs/runLive.sh --no-up      use a stack that is already running
#
# Volumes are never deleted here (use scripts/integrations/hdfsDown.sh --purge for that).
# Live tests run inside the compose network because WebHDFS redirects to DataNode host names that
# only resolve there. AEGISFS_HDFS_LIVE_BIG_MIB (default 100) sets the large-file size.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"
. "$ROOT/scripts/integrations/hdfsCompose.sh"
cd "$ROOT"

keep=0; up=1
for arg in "$@"; do
  case "$arg" in
    --keep) keep=1 ;;
    --no-up) up=0; keep=1 ;;
    -h|--help) sed -n '2,10p' "$0"; exit 0 ;;
    *) echo "unknown option: $arg" >&2; exit 2 ;;
  esac
done

status=0
if [ "$up" = 1 ]; then
  "$ROOT/scripts/integrations/hdfsUp.sh" || { echo "could not start the stack" >&2; exit 1; }
fi
stop() { [ "$keep" = 1 ] || "$ROOT/scripts/integrations/hdfsDown.sh" >/dev/null 2>&1; }
trap stop EXIT

echo "== unit tests (fake WebHDFS server, no containers needed)"
(cd "$HERE" && python3 -m unittest testWebHdfs testTransfer testTls 2>&1 | grep -E "^(Ran|OK|FAILED|FAIL:|ERROR:)") || status=1

echo "== live tests (real HDFS and AegisFS, inside the compose network)"
dc exec -T -e AEGISFS_INT_HDFS_LIVE=1 -e PYTHONDONTWRITEBYTECODE=1 \
  -e AEGISFS_HDFS_LIVE_BIG_MIB="${AEGISFS_HDFS_LIVE_BIG_MIB:-100}" \
  -w /opt/aegisfs-integration-tests/hdfs connector python3 -m unittest -v testLive || status=1

echo "== DataNodes killed mid-export"
"$HERE/interruptDataNode.sh" || status=1

[ "$status" = 0 ] && echo "ALL HDFS TESTS PASSED" || echo "SOME HDFS TESTS FAILED" >&2
exit "$status"
