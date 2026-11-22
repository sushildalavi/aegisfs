# AegisFS Operations Runbook

## Bootstrapping a cluster
Process-local (no Docker):
```
scripts/cluster.sh start 10      # nodes on 127.0.0.1:9100..9109, node0 is the seed
scripts/cluster.sh status
scripts/cluster.sh stop
```
A node is just one process:
```
aegisfs serve --data /var/lib/aegisfs/n0 --host 0.0.0.0 --port 9100 --id n0
aegisfs serve --data /var/lib/aegisfs/n1 --host 0.0.0.0 --port 9101 --id n1 --seeds <seedhost>:9100
```
Flags: `--rf <k>` (default 3), `--parallel 0|1` (replica fan-out; default 1).

Docker (on a host that has it):
```
docker compose build && docker compose up -d
docker compose exec node0 aegisfs --entry node0:9100 list_mem_ids
```

## Health & inspection
```
aegisfs --entry <host:port> status          # node id, alive count, file count, lamport clock
aegisfs --entry <host:port> health          # liveness ping
aegisfs --entry <host:port> list_mem_ids    # ring id | node | state | incarnation
aegisfs --entry <host:port> ls <name>       # file id + 3 designated replicas
aegisfs --entry <host:port> liststore       # files stored locally on that node
```

## Recovering a failed node
1. Confirm it is down: `scripts/cluster.sh status` or `list_mem_ids` (shows `FAILED`).
2. Re-replication is automatic: surviving replicas push missing segments to the new successor; this
   is driven by `reconcile_placement()` on the maintenance loop (default every ~1s). Verify with
   `ls <name>` + `getfromreplica <newaddr> <name> /tmp/x` and compare SHA-256.
3. To bring the original node back with its data intact:
   `scripts/cluster.sh restart <idx>` (process-local) — it rejoins and reconciles stale state.

## Monitoring repair / rebalance
- After a failure or a join, watch `ls <name>` — the 3 replica addresses should settle onto live
  nodes within a few seconds.
- `liststore` on a node that left the replica set should eventually show the file **gone** (safe
  handoff deletes a local copy only after the new replicas confirm they have the data).
- Counters are tracked in-process (`metrics`): `replicate_put`, `replicate_append`, `handoff_delete`.

## Cleaning volumes
```
scripts/cluster.sh stop
rm -rf run/                 # process-local data dirs
# Docker:
docker compose down -v      # removes node volumes
```

## Troubleshooting
| Symptom | Likely cause | Action |
|---------|--------------|--------|
| `NETWORK_ERROR cannot reach entry node` | wrong `--entry` or node down | check `scripts/cluster.sh status`, logs in `run/nodeN.log` |
| `create` returns `DEGRADED` | fewer than 3 replicas reachable | restore nodes; retry; data is on `committed` replicas |
| `get` returns `STALE` | no replica meets read-your-writes watermark yet | retry; ensure the writing client's appends were acknowledged |
| `merge` returns `CONFLICT` | a replica unreachable mid-merge | retry once the cluster is quiescent and all 3 replicas are up |
| node "listening" but refused | stale process / port in use | `pkill -f "aegisfs serve"`, remove `run/`, restart |

## Reproducible tests
```
ctest --test-dir build --output-on-failure        # 53 tests
scripts/smoke_test.sh                              # 3-node e2e
scripts/demo.sh                                    # 10-node demo + fault injection
benchmarks/runner/bench.sh                         # latency/throughput
scripts/verify_definition_of_done.sh               # full acceptance gate
```

## Data layout on disk (per node `--data` dir)
```
node.id                      persisted identity
files/<file_id>/manifest     atomically-committed metadata (checksummed)
files/<file_id>/base.seg     immutable base segment
files/<file_id>/append-*.seg one immutable segment per committed append
tmp/                         staging for fsync+rename (cleaned on recovery)
```
