#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
command -v docker >/dev/null || { echo "SKIP: docker is not installed"; exit 0; }
docker compose config >/dev/null
cleanup() { docker compose down -v >/dev/null 2>&1 || true; }
trap cleanup EXIT
docker compose up -d --build
healthy=0
for _ in $(seq 1 30); do
  if docker compose exec -T node0 aegisfs --entry node0:9100 health >/dev/null 2>&1; then
    healthy=1
    break
  fi
  sleep 2
done
[ "$healthy" -eq 1 ] || {
  echo "compose smoke FAIL: node0 did not become healthy" >&2
  docker compose ps >&2 || true
  docker compose logs --tail=40 node0 >&2 || true
  exit 1
}
docker compose exec -T node0 aegisfs --entry node0:9100 listMemIds
printf 'compose smoke PASS\n'
