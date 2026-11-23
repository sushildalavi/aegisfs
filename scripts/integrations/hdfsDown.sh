#!/usr/bin/env bash
# Stop the HDFS test stack. Data volumes are kept unless --purge is given.
#
#   scripts/integrations/hdfsDown.sh            stop and remove containers, keep volumes
#   scripts/integrations/hdfsDown.sh --purge    also delete the volumes of this project
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/hdfsCompose.sh"
cd "$ROOT"
case "${1:-}" in
  "") dc down --remove-orphans ;;
  --purge) dc down --remove-orphans --volumes ;;
  -h|--help) sed -n '2,6p' "$0" ;;
  *) echo "unknown option: $1" >&2; exit 2 ;;
esac
