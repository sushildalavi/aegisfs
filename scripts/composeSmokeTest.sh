#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
command -v docker >/dev/null || { echo "SKIP: docker is not installed"; exit 0; }
docker compose config >/dev/null
cleanup() { docker compose down -v >/dev/null 2>&1 || true; }
trap cleanup EXIT
docker compose up -d --build
for _ in $(seq 1 30); do
  if docker compose exec -T node0 aegisfs --entry node0:9100 health >/dev/null 2>&1; then break; fi
  sleep 2
done
docker compose exec -T node0 aegisfs --entry node0:9100 listMemIds
printf 'compose smoke PASS\n'
