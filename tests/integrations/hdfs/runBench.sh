#!/usr/bin/env bash
# Run the transfer benchmark inside the connector container and write the CSV and JSON files to
# benchmarks/integrations/hdfs/ (mounted into the container). Needs the stack from hdfsUp.sh.
#
#   tests/integrations/hdfs/runBench.sh [benchTransfer.py options]
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
. "$ROOT/scripts/integrations/hdfsCompose.sh"
mkdir -p "$ROOT/benchmarks/integrations/hdfs"
host() { sysctl -n "$1" 2>/dev/null || true; }
dc exec -T \
  -e BENCH_HOST_CPU="$(host machdep.cpu.brand_string)" \
  -e BENCH_HOST_CORES="$(host hw.ncpu)" \
  -e BENCH_HOST_MEMORY_BYTES="$(host hw.memsize)" \
  -e BENCH_HOST_OS="$(uname -sr)" \
  -e BENCH_HOST_ARCH="$(uname -m)" \
  -e BENCH_DOCKER_VERSION="$(docker version --format '{{.Server.Version}}' 2>/dev/null)" \
  -e BENCH_HADOOP_IMAGE="apache/hadoop:3.4.1 (linux/amd64, emulated on arm64 hosts)" \
  -e BENCH_HDFS_REPLICATION=2 -e BENCH_HDFS_DATANODES=2 -e BENCH_AEGISFS_NODES=3 \
  -e PYTHONDONTWRITEBYTECODE=1 \
  -w /opt/aegisfs-integration-tests/hdfs connector python3 benchTransfer.py --out /benchmarks-out "$@"
