# AegisFS Scripts

| Script | Purpose |
|--------|---------|
| `bootstrap.sh` | Install the toolchain (brew/apt) and build the project. |
| `cluster.sh` | Start/stop/status a process-local N-node cluster; `kill`/`restart` a node. |
| `smoke_test.sh` | 3-node end-to-end sanity (create/get/first-create/append/merge). Exits nonzero on failure. |
| `demo.sh` | 10-node demo: membership, replication, multiappend, merge, 2-node failure, re-replication. |
| `verify_integrity.sh <entry> <name>` | Compare all replica checksums for a file. |
| `verify_definition_of_done.sh` | Full acceptance gate: build, tests, demo, bench, identity, commit count. |

Typical flow:
```
scripts/bootstrap.sh
scripts/smoke_test.sh
scripts/cluster.sh start 10
./build/aegisfs --entry 127.0.0.1:9100 status
scripts/cluster.sh stop
```
