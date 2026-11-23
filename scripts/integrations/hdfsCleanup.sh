#!/usr/bin/env bash
# Remove the files the live tests leave under /aegisfs-test in HDFS, without touching anything
# else and without removing volumes. AegisFS names created by the tests stay (AegisFS has no
# delete command); they use the prefix "hdfs-it/".
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/hdfsCompose.sh"
cd "$ROOT"
dc exec -T namenode hdfs dfs -rm -r -f -skipTrash '/aegisfs-test/*' || true
