#!/usr/bin/env bash
# Live scenario: both DataNodes are killed in the middle of an export. Checks that the export
# fails, that the destination was never created, that HDFS and AegisFS stay usable afterwards,
# and that `hdfs cleanup` removes whatever temporary file the dead transfer left behind.
# Needs the stack from scripts/integrations/hdfsUp.sh. Exits nonzero if any check fails.
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
. "$ROOT/scripts/integrations/hdfsCompose.sh"
SIZE_MIB="${AEGISFS_HDFS_INTERRUPT_MIB:-400}"
RUN="dn$(date +%s)"
DIR="/aegisfs-test/$RUN"
NAME="hdfs-it/$RUN/big"
tool() { dc exec -T connector python3 /opt/aegisfs-integrations/aegisfs-integrations "$@"; }
fail() { echo "FAIL: $*" >&2; dc start datanode1 datanode2 >/dev/null 2>&1; exit 1; }
liveDataNodes() {
  curl -s "http://127.0.0.1:${AEGISFS_HDFS_NN_PORT:-9870}/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState" \
    | grep -o '"NumLiveDataNodes" : [0-9]*' | grep -o '[0-9]*$'
}

tool hdfs list /aegisfs-test >/dev/null 2>&1 || fail "HDFS is not reachable through the connector"
dc exec -T connector python3 -c "
import os
block = os.urandom(1 << 20)
with open('/tmp/interrupt.bin', 'wb') as f:
    for _ in range($SIZE_MIB): f.write(block)"
dc exec -T connector aegisfs --entry node0:9100 create /tmp/interrupt.bin "$NAME" | grep -q 'status=OK' \
  || fail "could not create the AegisFS source"
want=$(dc exec -T connector sha256sum /tmp/interrupt.bin | awk '{print $1}')

dc exec -T connector sh -c "mkdir -p /tmp/run; python3 /opt/aegisfs-integrations/aegisfs-integrations --json hdfs export '$NAME' '$DIR/out.dat' > /tmp/run/out.json 2> /tmp/run/err.txt; echo \$? > /tmp/run/rc" &
bg=$!

# wait for the temporary file to show up, then kill both DataNodes
found=""
for _ in $(seq 1 600); do
  if tool hdfs list "$DIR" 2>/dev/null | grep -q 'aegis-tmp'; then found=1; break; fi
  if dc exec -T connector test -f /tmp/run/rc 2>/dev/null; then break; fi
  sleep 0.1
done
[ -n "$found" ] || fail "the export finished (or never started) before the DataNodes could be killed; raise AEGISFS_HDFS_INTERRUPT_MIB"
docker kill "$(dc ps -q datanode1)" "$(dc ps -q datanode2)" >/dev/null
wait "$bg"
rc=$(dc exec -T connector cat /tmp/run/rc | tr -d '\r\n')
echo "export exit code after killing both DataNodes: $rc"
dc exec -T connector cat /tmp/run/err.txt | tail -3
[ "$rc" != "0" ] || fail "the export reported success although the DataNodes were killed"

# bring HDFS back
dc start datanode1 datanode2 >/dev/null 2>&1
for _ in $(seq 1 60); do [ "$(liveDataNodes)" = 2 ] && break; sleep 3; done
[ "$(liveDataNodes)" = 2 ] || fail "DataNodes did not come back"
dc exec -T namenode hdfs dfsadmin -safemode wait >/dev/null

tool hdfs exists "$DIR/out.dat" >/dev/null 2>&1 && fail "the destination exists after a failed export"
echo "ok: destination was not created"
echo "temporary files left in $DIR after the failure:"
tool hdfs list "$DIR" 2>/dev/null | grep 'aegis-tmp' || echo "  (none; the exporter removed its temp file)"
tool hdfs cleanup "$DIR" --older-than-hours 0 || fail "cleanup failed"
tool hdfs list "$DIR" 2>/dev/null | grep -q 'aegis-tmp' && fail "temporary file still present after cleanup"
echo "ok: no temporary file remains"

# AegisFS source untouched and the same transfer now works
got=$(dc exec -T connector sh -c "aegisfs --entry node0:9100 get '$NAME' /tmp/after.bin >/dev/null && sha256sum /tmp/after.bin" | awk '{print $1}')
[ "$want" = "$got" ] || fail "the AegisFS source changed"
tool hdfs export "$NAME" "$DIR/out.dat" >/dev/null || fail "retry after recovery failed"
echo "ok: retry after recovery succeeded"
dc exec -T connector rm -f /tmp/interrupt.bin /tmp/after.bin
echo "interruptDataNode PASS"
