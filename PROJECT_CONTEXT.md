# AegisFS — Complete Project Context

> One-stop reference for anyone picking this up: what was built, why, how it works,
> how to run it, what every file does, and what (if anything) is left to do.

**Status as of 2026-11-22:** Complete. All 10 acceptance gate checks pass.  
**Repo:** https://github.com/sushildalavi/aegisfs (private)  
**Author:** Sushil Dalavi `<75267758+sushildalavi@users.noreply.github.com>`  
**Commits:** 101  

---

## Table of Contents

1. [What it is](#1-what-it-is)
2. [System design](#2-system-design)
3. [Codebase map](#3-codebase-map)
4. [Key design decisions and tradeoffs](#4-key-design-decisions-and-tradeoffs)
5. [Build and run](#5-build-and-run)
6. [CLI reference](#6-cli-reference)
7. [Test suite](#7-test-suite)
8. [Benchmarks](#8-benchmarks)
9. [Known limitations](#9-known-limitations)
10. [What is left to do](#10-what-is-left-to-do)
11. [Resuming future work](#11-resuming-future-work)

---

## 1. What it is

AegisFS is an **original** fault-tolerant distributed file system written from scratch in C++20.
It is inspired by the public designs of HDFS (replicated storage), Apache Cassandra (consistent-hash
ring, gossip membership), and the HyDFS concept (HyDFS-compatible CLI). It contains no code from
any of those systems and is not course submission work.

**Single binary:** `aegisfs` runs both as a **storage node** and as a **client** issuing commands
to the cluster.  
**No permanent coordinator:** placement, membership, and correctness are all coordinator-free.  
**Replication factor:** RF=3. Every file's bytes live on exactly 3 nodes.  
**Fault tolerance:** files stay readable after up to 2 simultaneous node failures (provided all 3
replicas were healthy beforehand).

---

## 2. System design

### Consistent-hash ring

- Key space: 64-bit unsigned integers (top 64 bits of SHA-256 of a node's address string).
- Each physical node expands into **64 virtual nodes** for load balance.
- A file's 3 replica nodes are its **first 3 distinct physical successors** on the ring.
- Lookup is `O(log N)` binary search on a sorted array — no O(1) claim is made.
- Ring membership is recalculated any time the gossip layer updates the live member set.

### Gossip membership (SWIM-inspired)

- Each node maintains a `MemberList`: `{node_id, addr, state, incarnation}` for every known node.
- States: `ALIVE → SUSPECT → FAILED → LEFT`.
- Failure detection: a node that misses heartbeats is marked `SUSPECT`; after a configurable TTL
  it is promoted to `FAILED`.
- **Incarnation numbers** prevent stale `SUSPECT`/`FAILED` rumors from resurrecting or killing a
  healthy node: a node refutes a rumor by incrementing its own incarnation and gossiping back.
- `LEFT` is a graceful departure state; left nodes are tombstoned and reaped after a TTL.
- Gossip tick is 200ms; seeds are contacted by address, not injected as fake members.

### Crash-safe storage engine

Each node stores files under a per-node data directory:
```
<data_dir>/<file_id>/
    base.<hex_sha>          ← immutable first-create segment
    seg.<seq>.<client>.<hex_sha>  ← one file per append
    manifest                ← committed order + checksums (stage→fsync→rename)
    manifest.stage          ← written first, then renamed to manifest (atomic)
```

- **Atomic commit:** a new segment is written to a `.stage` path, fsynced, then renamed; the
  manifest is updated the same way. A crash between write and manifest-commit leaves an orphan
  segment that is GC'd on recovery — it never corrupts committed state.
- **Orphan GC:** on startup, any segment not referenced by the manifest is deleted.
- **SHA-256 verification:** every segment's content is checksummed and stored in the manifest;
  mismatches surface as errors on read.
- **Immutable segments:** existing segments are never overwritten; append always writes a new segment.

### Write path (create / append)

1. Client sends `CREATE` or `APPEND` to any node (the **coordinator**).
2. Coordinator computes the file's 3 successor nodes from the ring.
3. Coordinator fans out the segment to all 3 replicas in parallel (`--parallel`) or sequentially.
4. For self-writes the coordinator calls `apply_segment_local` in-process (no TCP loopback).
5. Each replica writes the segment and updates its manifest atomically.
6. Coordinator returns `OK` only when all 3 replicas have durably committed. If fewer succeed,
   it returns `DEGRADED` with the committed count — never a silent partial success.

**First-create-only arbitration:** the primary replica (first successor) rejects a `create` if the
file already has a base segment. All coordinators go primary-first, so only one base ever commits.

### Read path (get)

1. Client sends `GET` to any node (coordinator).
2. Coordinator finds the 3 successors and picks the first live one.
3. Streams the reconstructed file (base + all append segments in manifest order) back in 1 MiB
   chunks — no full-file buffering in memory.
4. If the file version is behind the client's read-your-writes watermark, the coordinator returns
   `STALE` rather than serving stale bytes.

### Append ordering and convergence

- Each client maintains a monotonically increasing `client_seq` per file.
- Each segment carries `(lamport_ts, client_id, client_seq)`.
- Merge total order is `(lamport_ts, client_id, client_seq)` — deterministic across all replicas
  on quiescence.
- Per-client causal order is always preserved (a client's later append always has a higher seq).
- `merge` command: coordinator gathers manifests from all 3 replicas, builds the union order,
  pushes any missing segments to replicas that lack them, and re-commits manifests. After merge,
  `get` from any replica returns byte-identical content (verified with SHA-256).

### Failure handling

| Event | Response |
|-------|----------|
| Node marked FAILED | Remaining nodes detect via gossip; `reconcile_placement` pushes missing replicas to new successors |
| Node restart | Runs orphan-segment GC, reads manifest, rejoins gossip; ring re-includes it |
| Node join | Rebalances: new node fetches files it now owns from its predecessor |
| Graceful leave | Node hands off its replicas to other successors, disseminates `LEFT`, shuts down |
| Corrupt segment | Anti-entropy detects checksum mismatch, pulls clean copy from a peer |

**Anti-entropy** runs every 30 seconds: each node pulls manifests from its peers for files it owns,
fetches missing segments, and repairs corrupted ones.

### Networking

- Hand-rolled POSIX TCP, no Boost.Asio/gRPC dependency.
- **Length-framed binary protocol:** `MAGIC(4) | VERSION(1) | TYPE(1) | HEADER_LEN(4) | HEADER | PAYLOAD_LEN(8) | PAYLOAD`, all little-endian.
- Large files stream in 1 MiB chunks; the protocol supports split `read_message_head` / `stream_payload_in` so payloads are never fully buffered.
- Bounded thread pool with backpressure (default 32 threads, queue depth 128).
- `SIGPIPE` ignored process-wide (static initializer) so a peer reset doesn't kill the node.

---

## 3. Codebase map

```
AegisFS/
├── CMakeLists.txt          ← builds aegisfs_core (lib) + aegisfs (exe); FetchContent GoogleTest
├── src/
│   ├── main.cpp            ← entry point (3 lines: calls cli::run)
│   ├── hash.cpp            ← sha256(), to_hex/from_hex, ring_id, Sha256Stream
│   ├── ring.cpp            ← Ring: set_members (virtual nodes), successors, first_at_or_after
│   ├── protocol.cpp        ← MsgType enum, Fields TLV bag, LE encode/decode helpers
│   ├── net.cpp             ← Conn (non-blocking connect), Listener (atomic fd_ + shutdown),
│   │                          write_frame, read_frame, streaming split API, SIGPIPE init
│   ├── manifest.cpp        ← SegmentRef, Manifest (has_op, add_segment, ordered, total_size),
│   │                          checksummed serialize/parse, make_file_id
│   ├── storage.cpp         ← StorageEngine: put_segment_stream, commit_manifest, reconstruct,
│   │                          file_sha, recover/orphan-GC, valid_filename
│   ├── metrics.cpp         ← counters, latency histograms, percentiles, ScopedTimer
│   ├── membership.cpp      ← gossip merge with incarnation refute, tick (suspect→failed+reap),
│   │                          ring_nodes
│   ├── replication.cpp     ← seg_to_fields, fetch_manifest, replicate_segment, fetch_segment,
│   │                          get_from_replica, send_reply/read_reply helpers
│   ├── node.cpp            ← THE ORCHESTRATOR (1000 lines): NodeConfig, start/stop, accept loop,
│   │                          all h_* RPC handlers, apply_segment_local, fanout,
│   │                          reconcile_placement, anti_entropy, gossip/failure/maintenance loops
│   ├── client.cpp          ← Client: watermark tracking, all CLI operations as methods
│   └── cli.cpp             ← serve mode, client command dispatch, shell REPL, multiappend
│
├── include/aegisfs/
│   ├── common.hpp          ← Status enum, Result<T>, kReplicationFactor=3, kStreamChunkBytes=1MiB
│   ├── hash.hpp
│   ├── ring.hpp
│   ├── protocol.hpp        ← MsgType, Fields
│   ├── net.hpp             ← Conn, Listener
│   ├── manifest.hpp        ← SegmentRef, Manifest
│   ├── storage.hpp         ← StorageEngine
│   ├── metrics.hpp
│   ├── membership.hpp      ← Member, MemberList, gossip merge
│   ├── threadpool.hpp      ← bounded thread pool (header-only)
│   ├── replication.hpp
│   ├── node.hpp            ← NodeConfig, Node
│   ├── client.hpp          ← Client
│   ├── cli.hpp             ← run(argc, argv)
│   └── version.hpp         ← kVersion = "0.1.0"
│
├── tests/
│   ├── support/
│   │   └── cluster_fixture.hpp   ← in-process Cluster, make_temp_file, file_sha_hex
│   ├── unit/               ← hash, ring, protocol, manifest, storage, membership,
│   │                          net, metrics, threadpool, common
│   ├── integration/
│   │   ├── cluster_test.cpp      ← 12 tests: convergence, create/get, streaming, first-create-only,
│   │   │                            append, get_from_replica, membership, ls, error cases
│   │   └── replication_test.cpp  ← 5 tests: append ordering, merge, idempotent replay,
│   │                                read-your-writes watermark, replica count
│   ├── concurrency/
│   │   ├── concurrency_test.cpp  ← ordering, single-winner race, independent files
│   │   └── stress_test.cpp       ← seeded mixed workload, 8 clients
│   └── chaos/
│       └── failure_test.cpp      ← 2-kill readable, re-replication, restart, join,
│                                    graceful leave, anti-entropy repair
│
├── scripts/
│   ├── cluster.sh          ← start/stop/status/kill/restart N process-local nodes (ports 9100+)
│   ├── smoke_test.sh       ← 3-node end-to-end sanity (create/get/append/merge)
│   ├── demo.sh             ← 10-node demo: 2-node kill, re-replication assertions (exits nonzero on fail)
│   ├── verify_definition_of_done.sh  ← ACCEPTANCE GATE (10 checks; exits nonzero on any fail)
│   ├── verify_integrity.sh ← compare all replica checksums for a named file
│   └── bootstrap.sh        ← one-shot toolchain install + build (macOS + Debian/Ubuntu)
│
├── benchmarks/
│   ├── runner/
│   │   ├── bench.sh        ← sequential vs parallel fan-out latency/throughput
│   │   ├── bench_concurrency.sh  ← ops/sec at 1/5/10/20 concurrent clients
│   │   └── analyze.py      ← stdlib CSV → summary.{csv,md}
│   ├── fixtures/
│   │   └── gen_data.py     ← deterministic fixture files
│   └── results/
│       ├── raw.csv         ← 80 data points from a real 5-node run
│       ├── summary.csv
│       ├── summary.md      ← human-readable tables
│       └── concurrency.csv ← ops/sec scaling table
│
├── docs/
│   ├── ARCHITECTURE.md     ← components, protocol, write/read paths, Mermaid diagrams
│   ├── INVARIANTS.md       ← 25 system invariants mapped to passing tests
│   ├── TRADEOFFS.md        ← design decisions and alternatives considered
│   ├── OPERATIONS.md       ← runbook: bootstrap, recovery, rebalance, troubleshooting
│   ├── BENCHMARKS.md       ← methodology, real numbers, honest interpretation
│   ├── INTERVIEW_GUIDE.md  ← 23 Q&A mapped to code paths and tests
│   └── RESUME_CLAIMS.md    ← 3 verified resume bullets with test evidence
│
├── .github/
│   ├── workflows/ci.yml    ← build + ctest + ASan matrix + TSan job
│   ├── PULL_REQUEST_TEMPLATE.md
│   ├── ISSUE_TEMPLATE/     ← bug + feature templates
│   └── CODEOWNERS
│
├── CLAUDE.md               ← durable execution rules for any future agent session
├── PROGRESS.md             ← milestone log and verification history
├── build-status.json       ← machine-readable state (complete: true)
├── FINAL_REPORT.md         ← acceptance gate output + verified results
├── PROJECT_CONTEXT.md      ← this file
├── README.md               ← public-facing overview
├── CHANGELOG.md
├── CONTRIBUTING.md
├── SECURITY.md
├── AUTHORS
├── LICENSE                 ← MIT
├── Makefile                ← convenience wrapper (make build / test / smoke / demo)
├── Dockerfile              ← single-node image
├── compose.yaml            ← 10-node Docker Compose (NOT RUN — Docker not on dev host)
├── .clang-format           ← Google style, 100 cols
├── .editorconfig
└── .gitattributes
```

**Scale:** ~5,400 lines of C++ (src + include + tests); ~1,500 lines of test code.  
Largest file: `src/node.cpp` (1,000 lines — the full node orchestrator).

---

## 4. Key design decisions and tradeoffs

| Decision | Chosen | Alternative | Why |
|----------|--------|-------------|-----|
| Networking | Hand-rolled POSIX TCP + bounded thread pool | Boost.Asio, gRPC | Zero heavy dependency; framing/streaming is the interesting work |
| Placement | 3 ring successors per file, segments not independently placed | Chunk-hash striping across all nodes | Matches HyDFS model; simpler repair and merge reasoning |
| Ring lookup | O(log N) binary search | O(1) DHT tricks | Honest; virtual nodes give balance without exotic algorithms |
| CAP stance | CP-leaning: require all 3 replicas for durable ack; return DEGRADED/STALE on partition | AP-leaning (Dynamo hinted handoff) | Cleaner to test read-your-writes and 2-failure survivability |
| Cross-client order | Lamport `(ts, client_id, seq)` total order | Vector clocks, Paxos/Raft per append | Deterministic convergence on quiescence without consensus rounds |
| Membership | SWIM gossip + incarnation numbers | ZooKeeper/etcd strongly-consistent membership | No single point of failure; incarnation prevents stale rumors |
| Coordinator | Per-operation (any node), no cluster leader | Raft leader | No leader election, no leader bottleneck |
| Concurrency | Per-file mutex + lock-free self-writes | Global lock / MVCC | Fine-grained; coordinator never holds file lock across network fan-out |

**Critical bug history (self-healed during build):**
- `Listener` missing move ops → copy semantics closed the fd in the temporary's destructor → nodes refused connections. Fixed by adding move ctor/assign and deleting copy.
- Self-loopback deadlock: coordinator held file lock then replicated to itself over TCP, which tried to acquire the same lock. Fixed by `apply_segment_local` (in-process for self-writes, no lock held).
- Cross-node lock cycle under concurrent create/append: coordinator held file lock across network fan-out that caused a cycle. Fixed by never holding a lock across fan-out.
- TSan accept/close data race on `Listener::fd_`: made `fd_` `std::atomic<int>` and called `::shutdown()` in `close()`.
- Ring load imbalance without virtual nodes: 10 nodes gave key spread of 3–4294 (one node owned 43%). Virtual nodes (64/node) brought it to 645–1243.
- `SIGPIPE` killing the process on peer reset: added `signal(SIGPIPE, SIG_IGN)` in a static initializer.

---

## 5. Build and run

### Prerequisites

- macOS (or Linux): CMake ≥ 3.20, C++20 compiler (Apple clang or GCC), OpenSSL, Ninja.
- GoogleTest is fetched automatically by CMake (no system install needed).
- Docker is optional (`compose.yaml` provided but not required).

```bash
# Install tools (macOS, one-time)
brew install cmake ninja openssl

# Build
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build

# Run all 87 tests
ctest --test-dir build --output-on-failure

# 3-node smoke test
scripts/smoke_test.sh

# Start a 10-node process-local cluster (ports 9100–9109)
scripts/cluster.sh start 10

# 10-node demo with fault injection
scripts/demo.sh

# Full acceptance gate
scripts/verify_definition_of_done.sh

# AddressSanitizer build
cmake -S . -B build-asan -G Ninja -DCMAKE_BUILD_TYPE=Debug -DENABLE_ASAN=ON
cmake --build build-asan && ctest --test-dir build-asan

# ThreadSanitizer build
cmake -S . -B build-tsan -G Ninja -DCMAKE_BUILD_TYPE=Debug -DENABLE_TSAN=ON
cmake --build build-tsan && ctest --test-dir build-tsan -R "Concurrency|Stress|Chaos"
```

### Makefile shortcuts

```bash
make build    # cmake + ninja
make test     # ctest
make smoke    # scripts/smoke_test.sh
make demo     # scripts/demo.sh
make bench    # benchmarks/runner/bench.sh
```

---

## 6. CLI reference

```
# Start a node (storage server)
aegisfs serve --port 9100 --data /var/aegisfs/node0 [--seeds 127.0.0.1:9101,127.0.0.1:9102]

# Client commands (--entry points at any live node)
aegisfs --entry 127.0.0.1:9100 create <local_file> <remote_name>
aegisfs --entry 127.0.0.1:9100 get    <remote_name> <local_file>
aegisfs --entry 127.0.0.1:9100 append <local_file>  <remote_name>
aegisfs --entry 127.0.0.1:9100 merge  <remote_name>
aegisfs --entry 127.0.0.1:9100 ls     <remote_name>
aegisfs --entry 127.0.0.1:9100 liststore
aegisfs --entry 127.0.0.1:9100 getfromreplica <node_addr> <remote_name> <local_file>
aegisfs --entry 127.0.0.1:9100 list_mem_ids
aegisfs --entry 127.0.0.1:9100 multiappend <remote_name> <node1> <file1> [<node2> <file2> ...]
aegisfs --entry 127.0.0.1:9100 status
aegisfs --entry 127.0.0.1:9100 health
aegisfs --entry 127.0.0.1:9100 metrics
aegisfs --entry 127.0.0.1:9100 leave   # graceful departure
aegisfs --entry 127.0.0.1:9100 shell   # interactive REPL
aegisfs version
```

**Exit codes:** nonzero on any failure — safe to use in scripts.

---

## 7. Test suite

87 tests, 100% passing. Last run: 42 seconds total (clean under ASan/UBSan and TSan).

| Suite | File | Tests | What it covers |
|-------|------|-------|----------------|
| Unit: common | tests/unit/common_test.cpp | 3 | Status names, Result<T>, wire codes |
| Unit: hash | tests/unit/hash_test.cpp | 4 | sha256, hex roundtrip, ring_id |
| Unit: ring | tests/unit/ring_test.cpp | 6 | Successors, distribution, is_replica, LEFT excluded |
| Unit: protocol | tests/unit/protocol_test.cpp | 5 | Fields TLV, bad magic rejected |
| Unit: manifest | tests/unit/manifest_test.cpp | 5 | Serialization, file-id, ordering, total size |
| Unit: storage | tests/unit/storage_test.cpp | 7 | Put/reconstruct, crash recovery, orphan GC |
| Unit: membership | tests/unit/membership_test.cpp | 5 | Gossip merge, incarnation refute, tombstone reap |
| Unit: net | tests/unit/net_test.cpp | 4 | Endpoint parse, framed + streaming round-trips |
| Unit: metrics | tests/unit/metrics_test.cpp | 3 | Counters, thread-safety, percentiles |
| Unit: threadpool | tests/unit/threadpool_test.cpp | 2 | Task execution, backpressure |
| Integration: cluster | tests/integration/cluster_test.cpp | 12 | Convergence, byte-identical create/get, streaming, first-create-only, append accumulation, getfromreplica purity, membership propagation, ls presence heals after kill |
| Integration: replication | tests/integration/replication_test.cpp | 5 | Append ordering, merge byte-identical, idempotent replay, read-your-writes watermark, replica count |
| Concurrency | tests/concurrency/concurrency_test.cpp | 7 | Ordering/no-dup, single-winner create race, 10-way independent files |
| Stress | tests/concurrency/stress_test.cpp | 2 | Seeded mixed workload, 8 clients |
| Chaos | tests/chaos/failure_test.cpp | 7 | 2-kill readable, re-replication restores replica, restart recovery, join keeps files, graceful leave, anti-entropy repairs corrupt segment |

**Cluster fixture** (`tests/support/cluster_fixture.hpp`): spins up N real nodes in-process on
ephemeral ports, waits for gossip convergence, provides `make_temp_file` and `file_sha_hex` helpers.

---

## 8. Benchmarks

Measured on a 5-node process-local cluster (loopback only). Absolute numbers are machine-specific.
Run `benchmarks/runner/bench.sh` to reproduce.

### Latency and throughput by size

| op | size | mode | mean ms | MB/s |
|----|------|------|---------|------|
| create | 1 KiB | parallel | 54.8 | 0.02 |
| create | 1 MiB | parallel | 62.4 | 16.1 |
| create | 10 MiB | parallel | 127.4 | 78.5 |
| create | 100 MiB | parallel | 540.8 | 189.3 |
| get | 100 MiB | parallel | 188.6 | 530.5 |
| create | 100 MiB | sequential | 647.2 | 157.0 |

**Parallel fan-out reduces 100 MiB create latency by ~16%** (647 ms → 541 ms). Small files show
neutral or slightly negative effect (fan-out coordination overhead exceeds the parallelism gain).

### Concurrency scaling

| clients | ops/sec |
|---------|---------|
| 1 | 38 |
| 5 | 61 |
| 10 | 69 |
| 20 | 69 (plateau) |

Throughput plateaus at ~10 clients on independent files. Appends to a single file serialise by
design (per-file lock + ordering), so that path does not scale horizontally.

---

## 9. Known limitations

These are honestly documented — not aspirational:

1. **Docker Compose not run.** Docker is not installed on the dev host. The process-local 10-node
   cluster path (`scripts/cluster.sh`, in-process test fixture) is what has been exercised.
   `compose.yaml` is provided for environments that have Docker.

2. **No sub-file striping.** A file's replicas live on its 3 ring successors; large files are not
   split across many nodes. Throughput for a single huge file is bounded by one node's I/O.

3. **Loopback benchmarks only.** A single-host 10-node cluster shares CPU, disk, and NIC, so
   numbers reflect software overhead, not network-bound distributed performance.

4. **O(log N) ring lookup.** Not O(1). Binary search on a sorted array; stated honestly throughout.

5. **No TLS / authentication.** The protocol is unauthenticated plaintext TCP. Noted in
   `docs/SECURITY.md` with a hardening roadmap.

6. **No sub-file deduplication or compression.**

7. **Replication is synchronous.** `create`/`append` block until all 3 replicas commit. This is
   intentional for the CP-leaning correctness model but limits write throughput at high latency.

---

## 10. What is left to do

The project is **functionally complete** per its own definition of done. These are genuine
extensions that were out of scope, not gaps in what was required:

| Item | Effort | Notes |
|------|--------|-------|
| Run Docker Compose topology | Low | Install Docker, run `docker compose up`; the compose.yaml is already written |
| TLS on the protocol layer | Medium | Add OpenSSL TLS handshake in `net.cpp`; the framing layer already abstracts I/O |
| Multi-network benchmark | Medium | Run the benchmark across real or VM-separated nodes to get network-bound numbers |
| Sub-file striping / erasure coding | High | Rearchitect placement so segments are independently placed; adds Reed-Solomon-style recovery |
| Quorum reads (AP mode) | Medium | Add a quorum read path as an alternative to the current CP primary read; expose as a flag |
| Client retry on DEGRADED/STALE | Low | Currently surfaced to the user; could auto-retry with backoff |
| Persistent gossip seed discovery | Low | Currently seeds are passed at startup; a seed service or DNS-SD would remove the manual step |
| Metrics dashboard | Low | The `/metrics` RPC returns raw counters; a Prometheus exporter + Grafana dashboard would visualise them |
| CI on Linux | Low | The CI workflow is defined in `.github/workflows/ci.yml`; it needs a Linux runner configured in GitHub Actions |
| REST/gRPC gateway | High | Add an HTTP layer so non-C++ clients can talk to the cluster without the `aegisfs` CLI |

---

## 11. Resuming future work

Any future session (human or agent) should:

```bash
# 1. Check current state
cat build-status.json
tail -30 PROGRESS.md

# 2. Verify the build still passes
git status
cmake --build build
ctest --test-dir build --output-on-failure

# 3. Run the acceptance gate
scripts/verify_definition_of_done.sh

# 4. Continue from here
```

Key invariants that must never be broken (from `CLAUDE.md`):
- No permanent central coordinator.
- RF=3; a file's replicas are on its first 3 distinct ring successors.
- Atomic write path: stage → fsync → rename.
- Readable after up to 2 node failures.
- Per-client append ordering, read-your-writes, idempotent retries.

The machine-readable resumability state is in `build-status.json`. All milestones M0–M7 are `done`
and `complete: true`. Any new milestone should be appended to `PROGRESS.md` and reflected in
`build-status.json`.
