# Operations

## Local cluster

```
scripts/cluster.sh start 10      # 127.0.0.1:9100-9109, node0 is the seed
scripts/cluster.sh status
scripts/cluster.sh kill 3        # SIGKILL one node
scripts/cluster.sh restart 3     # relaunch it with its data directory
scripts/cluster.sh stop
```

`AEGISFS_BASE_PORT`, `AEGISFS_RUN_DIR` (default `run/`) and `AEGISFS_BIN` (default `build/aegisfs`)
change the ports, data location and binary. Node logs are `run/node<N>.log`.

The script sources `scripts/devCreds.sh`, which creates a throwaway CA, a node certificate and a
cluster token in `.dev/` the first time. To run your own commands against the cluster, source it in
the same shell:

```
source scripts/devCreds.sh
build/aegisfs --entry 127.0.0.1:9100 listMemIds
```

## Running nodes by hand

TLS and a cluster token are required unless `--insecure` is given. Certificates and keys are PEM
files.

```
aegisfs serve --data /var/lib/aegisfs/n0 --host 0.0.0.0 --advertise host-a --port 9100 --id n0 \
  --tls-cert node.pem --tls-key node.key --tls-ca ca.pem --token-file token
aegisfs serve ... --advertise host-b --id n1 --seeds host-a:9100
aegisfs --tls-ca ca.pem --token-file token --entry host-a:9100 listMemIds
```

The certificate must name the address that peers and clients use, which is the `--advertise` value
when one is given. `--host` has to be an IPv4 address: any other value binds every interface.

### Node options

| Flag | Default | Meaning |
|---|---|---|
| `--data` | `./data` | data directory |
| `--host` | `127.0.0.1` | IPv4 bind address |
| `--advertise` | bind address | address announced to peers |
| `--port` | 0 (ephemeral) | listen port |
| `--id` | random | node id, used only the first time; afterwards `node.id` wins |
| `--seeds` | none | comma-separated `host:port` list to join through |
| `--rf` | 3 | replication factor; give every node the same value |
| `--parallel` | 1 | 0 sends replicas one at a time |
| `--probe-interval-ms` | 400 | probe period and timeout |
| `--suspect-ms` | 1500 | time to refute a suspicion before `FAILED` |
| `--indirect-probes` | 3 | helpers asked before suspecting a peer |
| `--scrub-interval-ms` | 300000 | full re-hash period; 0 re-hashes on every maintenance cycle |

The probe defaults suit loopback. The Compose cluster uses 1000 ms and 6000 ms, because a probe that
includes a TLS handshake sometimes exceeded the defaults and got healthy nodes declared failed.

### Credentials

| Flag | Environment variable | Used by |
|---|---|---|
| `--tls-cert`, `--tls-key` | `AEGISFS_TLS_CERT`, `AEGISFS_TLS_KEY` | nodes |
| `--tls-ca` | `AEGISFS_TLS_CA` | nodes and clients |
| `--token-file` | `AEGISFS_TOKEN_FILE` (or the token itself in `AEGISFS_CLUSTER_TOKEN`) | nodes and clients |
| `--tls-client-cert`, `--tls-client-key` | `AEGISFS_TLS_CLIENT_CERT`, `AEGISFS_TLS_CLIENT_KEY` | clients, for mutual TLS |
| `--tls-require-client` | none | nodes, to require client certificates |
| `--insecure` | `AEGISFS_INSECURE=1` | turns TLS and the token check off |

## Inspection

```
aegisfs --entry <host:port> status          # node id, alive count, file count, Lamport clock
aegisfs --entry <host:port> health
aegisfs --entry <host:port> listMemIds      # ring id, node, state, incarnation
aegisfs --entry <host:port> members         # the same with addresses, as key=value lines
aegisfs --entry <host:port> ls <name>       # file id, designated replicas, which of them hold it
aegisfs --entry <host:port> listStore       # files stored on that node
aegisfs --entry <host:port> metrics         # counters and latency percentiles of that node
scripts/checks/verifyIntegrity.sh <host:port> <name>   # compare every replica's SHA-256
```

Counters include `rpc.accepted`, `rpc.unauthorized`, `replicatePut`, `replicateAppend`,
`handoffDelete`, `antiEntropyRepair` and `gracefulLeave`. Latencies are recorded for `create`,
`append`, `get` and `merge` on the coordinating node, in microseconds. Each keeps at most the first
100,000 samples since the process started, so the percentiles stop moving after that.

## Failed and rejoining nodes

1. Confirm the state with `listMemIds`.
2. Repair is automatic. Check it with `ls <name>`: the designated replicas should all be live and
   show `present=yes`. `getFromReplica` reads one replica's copy for comparison.
3. Restart the node with the same data directory to bring it back.

To remove a node deliberately, send it `leave`, wait until `listStore` on it is empty, then stop it.

## Docker Compose

`deploy/docker/compose.yaml` runs ten nodes, the REST gateway, the metrics exporter, Prometheus and
Grafana. Make credentials that name every node first:

```
AEGISFS_DEV_SANS="$(printf 'DNS:node%s,' $(seq 0 9) | sed 's/,$//')" scripts/devCreds.sh >/dev/null
docker compose -f deploy/docker/compose.yaml up -d --build
docker compose -f deploy/docker/compose.yaml exec node0 aegisfs --entry node0:9100 listMemIds
docker compose -f deploy/docker/compose.yaml down        # add -v to delete the node volumes
```

If `.dev/` already holds a certificate without those names, remove it or point `AEGISFS_DEV_DIR` and
`AEGISFS_CREDS_DIR` at a new directory.

This file publishes its ports on all host interfaces and sets the gateway token to `change-me`.
Edit both before using it anywhere other than a private machine. See [SECURITY.md](../SECURITY.md).

`scripts/checks/composeSmokeTest.sh` and `scripts/checks/networkFaultTest.sh` start this stack with
their own credentials in `.dev-compose/` and finish with `docker compose down -v`, which deletes the
volumes of the `aegisfs` Compose project. Set `COMPOSE_PROJECT_NAME` to keep your own cluster apart.

## REST gateway and metrics

`extensions/gateway/gateway.py` is a standard-library HTTP server that runs the `aegisfs` binary for
each request.

| Request | Action |
|---|---|
| `GET /health`, `GET /status`, `GET /metrics` | node health, status and counters as JSON |
| `GET /files/{name}` | read a file |
| `PUT /files/{name}` | create |
| `POST /files/{name}` | append |

It reads `AEGISFS_BIN`, `AEGISFS_ENTRY`, `GATEWAY_HOST` (default `0.0.0.0`), `GATEWAY_PORT` (8080)
and the client credential variables above. If `AEGISFS_AUTH_TOKEN` is set, requests need
`Authorization: Bearer <token>`; if it is unset, the gateway accepts every request. It holds each
upload and download in memory, serves plain HTTP, and answers any failed write with status 409.

`extensions/metrics/exporter.py` serves the entry node's metrics at `/metrics` (port 9108, or
`EXPORTER_PORT`) in Prometheus text format, for example `aegisfs_lat_create_p95Us`. It covers one
node. The Compose file runs Prometheus against it. Grafana starts empty: add the Prometheus data
source and import `extensions/metrics/grafana/dashboard.json`.

## Troubleshooting

| Symptom | Likely cause | Action |
|---|---|---|
| `NETWORK_ERROR cannot reach entry node` | wrong `--entry`, node down, or TLS names do not match | check `scripts/cluster.sh status` and the node log |
| `UNAUTHORIZED` | token missing or different | use the cluster's token file |
| `create` returns `DEGRADED` | fewer than three replicas committed | restore nodes and check `ls <name>` |
| `create` returns `CHECKSUM_MISMATCH` | a retried create after a partial or lost reply, see [Known limitations](../README.md#known-limitations) | check `ls <name>`; the file may already exist |
| `get` returns `STALE` | no replica has this client's latest write yet | retry |
| `merge` or `append` returns `CONFLICT` | two replicas hold different base segments | compare with `getFromReplica` and repair by hand |
| a node fails to start | port in use, or unreadable certificate files | find the process holding the port |
