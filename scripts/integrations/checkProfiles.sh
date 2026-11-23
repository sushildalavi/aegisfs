#!/usr/bin/env bash
# Check that the optional Compose profiles stay optional: with no profile selected the stack is
# exactly the core cluster, and each profile adds only its own services. Needs Docker Compose but
# starts nothing.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
D="$ROOT/deploy/docker"
FILES=(-f "$D/compose.yaml" -f "$D/compose.cassandra.yaml" -f "$D/compose.hdfs.yaml")

services() { docker compose -p profilecheck "${FILES[@]}" "$@" config --services | sort | tr '\n' ' '; }
core="$(docker compose -p profilecheck -f "$D/compose.yaml" config --services | sort | tr '\n' ' ')"
none="$(services)"
fail=0
check() { if [ "$2" = "$3" ]; then echo "[ok] $1"; else echo "[fail] $1" >&2; echo "  expected: $3" >&2; echo "  got:      $2" >&2; fail=1; fi; }

check "no profile gives exactly the core stack" "$none" "$core"
for svc in cassandra cassandra-init cassandra-indexer namenode datanode1 datanode2 hdfs-init connector; do
  case " $none " in *" $svc "*) echo "[fail] $svc runs without its profile" >&2; fail=1 ;; esac
done
cass="$(services --profile cassandra)"
hdfs="$(services --profile hdfs)"
for svc in cassandra cassandra-init cassandra-indexer; do
  case " $cass " in *" $svc "*) ;; *) echo "[fail] $svc missing from the cassandra profile" >&2; fail=1 ;; esac
done
for svc in namenode datanode1 datanode2 hdfs-init connector; do
  case " $hdfs " in *" $svc "*) ;; *) echo "[fail] $svc missing from the hdfs profile" >&2; fail=1 ;; esac
done
case " $cass " in *" namenode "*) echo "[fail] the cassandra profile starts HDFS services" >&2; fail=1 ;; esac
case " $hdfs " in *" cassandra "*) echo "[fail] the hdfs profile starts Cassandra services" >&2; fail=1 ;; esac
[ "$fail" = 0 ] && echo "PROFILES OK" || { echo "PROFILES FAILED" >&2; exit 1; }
