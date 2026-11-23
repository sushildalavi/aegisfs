# AegisFS extensions

## Retries

The C++ client retries transient network, timeout, unavailable, degraded, and stale results with
bounded linear backoff. Uploads attach a stable `opId`, so a retry is an idempotent replay rather
than a second append. Applications can tune this with `Client::setRetryPolicy()`.

## TLS and authentication

Enable TLS on a node with:

```text
aegisfs serve --data ./data --port 9100 --tls-cert server.crt --tls-key server.key --tls-ca ca.crt
```

Clients use `--tls-ca ca.crt`; mutual TLS additionally uses `--tls-client-cert` and
`--tls-client-key`. TLS requires PEM certificates and negotiates TLS 1.2 or newer. The REST
gateway supports bearer authentication through `AEGISFS_AUTH_TOKEN`.

## Quorum reads

`getQuorum <name> <localfile>` asks the coordinator to read a majority of designated replicas and
select the highest reachable manifest version. It returns `UNAVAILABLE` if a majority cannot be
contacted. `merge` remains the explicit operation for repairing divergent replicas.

## REST gateway and metrics

Run `extensions/gateway/gateway.py` with `AEGISFS_BIN`, `AEGISFS_ENTRY`, and optionally
`AEGISFS_AUTH_TOKEN`. Endpoints are `GET /health`, `GET /status`, `GET /metrics`, `GET /files/{name}`
for reads, `PUT /files/{name}` for create, and `POST /files/{name}` for append.

`extensions/metrics/exporter.py` converts the existing metrics CLI output to Prometheus
exposition format. `extensions/metrics/prometheus.yml` and
`extensions/metrics/grafana/dashboard.json` provide a starter monitoring stack. The compose file in
`deploy/docker/` starts the gateway, exporter, Prometheus and Grafana alongside the ten nodes.

## Docker Compose

`scripts/checks/composeSmokeTest.sh` validates the Compose file, builds the images, starts the stack,
checks node health, and prints membership. It prints a skip message and exits 0 when Docker is
not installed, which is the case on the machine this was written on, so the Compose setup has
not been run.
