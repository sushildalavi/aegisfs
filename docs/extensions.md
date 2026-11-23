# AegisFS extensions

## Retries

The C++ client retries transient network, timeout, unavailable, degraded, and stale results with
bounded linear backoff. Uploads attach a stable `opId`, so a retry is an idempotent replay rather
than a second append. Applications can tune this with `Client::setRetryPolicy()`.

## TLS and authentication

TLS and a cluster token are required unless `--insecure` is given. A node needs:

```bash
aegisfs serve --data ./data --port 9100 \
  --tls-cert node.pem --tls-key node.key --tls-ca ca.pem --token-file token
```

Clients (and the gateway and exporter, which run the `aegisfs` binary) need `--tls-ca ca.pem` and
`--token-file token`; mutual TLS additionally uses `--tls-client-cert` and `--tls-client-key`. Each
flag also has an environment variable (`AEGISFS_TLS_CA`, `AEGISFS_TLS_CERT`, `AEGISFS_TLS_KEY`,
`AEGISFS_TLS_CLIENT_CERT`, `AEGISFS_TLS_CLIENT_KEY`, `AEGISFS_TOKEN_FILE`, `AEGISFS_CLUSTER_TOKEN`),
which is how the gateway picks them up. Certificates are PEM, TLS 1.2 or newer, and the server
certificate must name the address clients dial. The REST gateway has its own bearer token,
`AEGISFS_AUTH_TOKEN`, which is separate from the cluster token.

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
