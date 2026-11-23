# AegisFS

[![CI](https://github.com/sushildalavi/aegisfs/actions/workflows/ci.yml/badge.svg)](https://github.com/sushildalavi/aegisfs/actions/workflows/ci.yml)
![C++20](https://img.shields.io/badge/C%2B%2B-20-blue)
![License](https://img.shields.io/badge/license-MIT-green)

AegisFS is a coordinator-free distributed file system written in C++20. It combines
consistent-hash placement, gossip membership, replicated storage, crash-safe manifests,
and automatic repair behind a small command-line client.

The project is an independent implementation inspired by public ideas from HDFS, Cassandra,
SWIM, and HyDFS. It is not a fork or port of those systems.

## What it provides

- Deterministic SHA-256 placement on a 64-bit virtual-node ring.
- RF=3 replication across the first three distinct ring successors.
- SWIM-inspired membership with suspicion, incarnation numbers, and tombstone cleanup.
- Immutable base and append segments with checksummed, atomically committed manifests.
- Per-client append ordering, idempotent operation IDs, read-your-writes, and merge convergence.
- Automatic re-replication, safe handoff, restart recovery, and anti-entropy repair.
- Bounded streaming transfers, client retries, quorum reads, optional TLS/mTLS, REST, and metrics.

## Current status

The core implementation builds successfully. The current verification snapshot is:

| Check | Result |
|---|---|
| C++ build | Pass |
| Full CTest inventory | 93/93 pass |
| ASan build (focused subset) | 60/60 pass |
| Python extension tests | 4/4 pass |
| Benchmark result validation | 80 rows pass |
| Docker Compose validation | Not run; Docker is unavailable on the development host |

See [`docs/`](docs/) for the full architecture writeup, invariants, tradeoffs, operations
runbook, and benchmark methodology.

## Build

Requirements: CMake 3.20+, Ninja, a C++20 compiler, OpenSSL 3, and Python 3 for extension tests.
GoogleTest is fetched by CMake.

```bash
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build
```

Run the focused local suite:

```bash
ctest --test-dir build --output-on-failure \
  -R '^(Hash|Ring|Protocol|Manifest|StorageFixture|Membership|Common|Metrics|ThreadPool)'
```

Run the complete suite on a host that permits local sockets:

```bash
ctest --test-dir build --output-on-failure
```

## Quick start

Start a process-local cluster:

```bash
scripts/cluster.sh start 10
```

Create, inspect, and download a file:

```bash
printf 'hello\n' >/tmp/hello.txt
./build/aegisfs --entry 127.0.0.1:9100 create /tmp/hello.txt notes/hello.txt
./build/aegisfs --entry 127.0.0.1:9100 ls notes/hello.txt
./build/aegisfs --entry 127.0.0.1:9103 get notes/hello.txt /tmp/back.txt
scripts/cluster.sh stop
```

The three-node smoke test and fault-injection demo are also available:

```bash
scripts/smokeTest.sh
scripts/demo.sh
```

## CLI

| Command | Purpose |
|---|---|
| `create <local> <name>` | Create a file; the first successful create wins |
| `get <name> <local>` | Read a file with checksum verification |
| `getQuorum <name> <local>` | Read from a majority and select the highest version |
| `append <local> <name>` | Append bytes with per-client ordering |
| `merge <name>` | Reconcile reachable replicas to one byte-identical view |
| `ls <name>` | Show file metadata and designated replicas |
| `listStore` | List files stored on the entry node |
| `getFromReplica <addr> <name> <local>` | Read one replica without read repair |
| `listMemIds` | List members, states, incarnation numbers, and ring IDs |
| `multiAppend <name> <node> <file> [...]` | Issue concurrent appends from multiple nodes |
| `status` / `health` | Inspect node state or liveness |

## Consistency and failure model

- A file with three healthy durable replicas remains readable after up to two node failures.
- Healthy writes acknowledge only after all three replicas durably commit.
- Writes with fewer reachable replicas return `DEGRADED` and expose the committed count.
- A read that cannot satisfy the client's read-your-writes watermark returns `STALE`.
- Ring routing is O(log N), using binary search over virtual nodes.

## Repository guide

- [`docs/architecture.md`](docs/architecture.md) — system components and request paths.
- [`docs/invariants.md`](docs/invariants.md) — correctness invariants mapped to tests.
- [`docs/tradeoffs.md`](docs/tradeoffs.md) — design choices and alternatives.
- [`docs/operations.md`](docs/operations.md) — operating and recovery runbook.
- [`docs/benchmarks.md`](docs/benchmarks.md) — benchmark methodology and measurements.
- [`docs/interviewGuide.md`](docs/interviewGuide.md) — concise design questions and answers.
- [`docs/extensions.md`](docs/extensions.md) — TLS, REST, Prometheus, Grafana, and Compose.

## License

MIT — see [`LICENSE`](LICENSE).
