# AegisFS

[![ci](https://github.com/sushildalavi/aegisfs/actions/workflows/ci.yml/badge.svg)](https://github.com/sushildalavi/aegisfs/actions/workflows/ci.yml)
![language](https://img.shields.io/badge/C%2B%2B-20-blue)
![license](https://img.shields.io/badge/license-MIT-green)

**AegisFS** is an original, fault-tolerant **distributed file system** written from scratch in
**C++20**. It combines HDFS-style replicated storage with Cassandra-style consistent-hash placement
and SWIM-style gossip membership, and exposes a HyDFS-compatible command set. Any node can serve
any client command; there is **no permanent central coordinator** for placement, membership, or
correctness.

> Independent project, inspired by the public designs of HDFS, Apache Cassandra, and the publicly
> described HyDFS concept. It is **not** a port of any of those and contains no code from them, and
> it is not course submission work.

## Highlights
- **Consistent-hash ring** over a 64-bit SHA-256 id space; each file's 3 replicas live on its first
  3 distinct ring successors (RF=3). Deterministic placement; every node agrees given the same view.
- **Gossip membership** (SWIM-inspired) with suspicion and **incarnation numbers** that stop stale
  rumors from resurrecting dead nodes.
- **Crash-safe storage**: immutable base + append segments, per-file manifests committed atomically
  (stage → fsync → rename), orphan-segment GC on recovery.
- **Correct concurrency**: per-client append ordering, Lamport-ordered convergence across clients,
  idempotent retries, **read-your-writes**, and a checksum-verified **merge** that makes all
  replicas byte-identical.
- **Survives 2 node failures** for any file that had 3 healthy replicas; automatic re-replication
  and safe-handoff rebalancing on join.
- **Streaming transfers** in bounded 1 MiB chunks — 100 MiB files move without loading into RAM.
- Length-framed, versioned, bounds-checked binary protocol over a bounded TCP thread pool.

## Build
Requires CMake ≥ 3.20, a C++20 compiler (Clang/GCC), OpenSSL, Ninja. GoogleTest is fetched
automatically.
```
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build
ctest --test-dir build --output-on-failure
```

## Quick start (process-local cluster)
Docker is optional (`compose.yaml` provided). Without it, run nodes as local processes:
```
scripts/cluster.sh start 10          # 10 nodes on 127.0.0.1:9100..9109
./build/aegisfs --entry 127.0.0.1:9100 listMemIds
```
```
echo "hello" > /tmp/hello.txt
./build/aegisfs --entry 127.0.0.1:9100 create /tmp/hello.txt notes/hello.txt
./build/aegisfs --entry 127.0.0.1:9100 ls notes/hello.txt
./build/aegisfs --entry 127.0.0.1:9103 get notes/hello.txt /tmp/back.txt
scripts/cluster.sh stop
```

### Real sample output
```
$ aegisfs --entry 127.0.0.1:9100 create ./payload demo/payload
status=OK
fileId=85d92d92cccf9f24
committed=3
rf=3
r0=127.0.0.1:9106
r1=127.0.0.1:9107
r2=127.0.0.1:9100

$ aegisfs --entry 127.0.0.1:9100 merge demo/payload
status=OK
replicas=3
fileSha=4c10d5b6c4aa73e3f02515d724325f15356cf4f4bbaed25e411347ebb5b2e81e
```

## Command reference
| Command | Meaning |
|---------|---------|
| `create <local> <name>` | Upload a new file. Only the first create for a name wins (else `ALREADY_EXISTS`). |
| `get <name> <local>` | Download, SHA-256 verified; includes this client's own prior appends (read-your-writes). |
| `getQuorum <name> <local>` | Majority read; selects the highest reachable replica version. |
| `append <local> <name>` | Append bytes to an existing file (ordered per client). Missing file → `NOT_FOUND`. |
| `merge <name>` | Reconcile all reachable replicas to one byte-identical committed view. |
| `ls <name>` | Show file id and the 3 designated replica addresses. |
| `listStore` | List files stored on the entry node (name, size, version, segments). |
| `getFromReplica <addr> <name> <local>` | Fetch one replica's exact bytes, no read-repair. |
| `listMemIds` | List members sorted by ring id with state + incarnation. |
| `multiAppend <name> <node> <file> [...]` | Concurrent appends initiated from several nodes. |
| `status` / `health` | Node status / liveness. |

Exit codes are nonzero on failure so scripts can detect real success/failure.

## Consistency & failure model (precise)
- **Guaranteed:** a file with 3 healthy durable replicas stays readable after ≤2 node failures. In
  the healthy case a write is acknowledged `OK` only after **all 3 replicas** durably commit.
- **Degraded writes:** if fewer than 3 replicas are reachable, a write returns `DEGRADED` with the
  committed count (weaker durability, surfaced explicitly — never a silent success).
- **Reads:** `get` returns `STALE` rather than serving data that does not meet the client's
  read-your-writes watermark. AegisFS is **CP-leaning** for a partitioned minority.
- Routing is **O(log N)** (binary search on the ring), not O(1). See `docs/tradeoffs.md`.

## Testing
- Unit + integration (3/5/10-node, 64 MiB streaming) + concurrency + chaos (2-node kill,
  re-replication, restart recovery, join, graceful leave, anti-entropy repair) + stress —
  **85+ tests**, all passing.
- `scripts/smokeTest.sh` — 3-node end-to-end. `scripts/demo.sh` — 10-node demo with fault
  injection (asserts, exits nonzero on failure).
- Clean under **AddressSanitizer/UBSan**; concurrent paths clean under **ThreadSanitizer**.

## Documentation
- [`docs/architecture.md`](docs/architecture.md) — components, protocol, write/read paths, failure model
- [`docs/invariants.md`](docs/invariants.md) — invariants mapped to tests
- [`docs/tradeoffs.md`](docs/tradeoffs.md) — design decisions and alternatives
- [`docs/operations.md`](docs/operations.md) — runbook
- [`docs/benchmarks.md`](docs/benchmarks.md) — real measurements
- [`docs/interviewGuide.md`](docs/interviewGuide.md) — Q&A on the design
- [context.d](context.d) — canonical project context, evidence, and resume-ready findings

## Limitations (honest)
- Replicas of one file are its 3 ring successors (no sub-file striping across many nodes).
- Loopback-only benchmarks; a single-host 10-node cluster shares CPU/disk.
- Docker Compose topology is provided but was **not run** on the dev host (no Docker installed);
  the process-local path is what has been exercised and verified.
- Optional TLS, REST gateway, Prometheus exporter, and Grafana dashboard are documented in
  [`docs/extensions.md`](docs/extensions.md).

## License
MIT — see [LICENSE](LICENSE).
