# AegisFS — PROGRESS

Living log of real commands, outcomes, failures, and next steps. Newest notes at the bottom
of each milestone. Commit baseline: **UNBORN** (new repository). Commits are counted from the
first project commit forward.

## Environment (verified 2026-11-22)
- Host: macOS (darwin 27.2), Apple clang 21 → C++20.
- `cmake` 4.5.1 + `ninja` 1.13.2 installed via Homebrew (user-owned `/opt/homebrew`, no sudo).
- OpenSSL@3 present (`/opt/homebrew/opt/openssl@3`) → SHA-256.
- **docker NOT installed** → multi-node cluster runs as process-local nodes on distinct ports.
  Docker Compose scenarios are marked NOT RUN. (`BLOCKER: DOCKER_UNAVAILABLE`, non-fatal.)
- GitHub CLI authenticated as **`sushildalavi`** (id 75267758) → identity verified, private repo
  `sushildalavi/aegisfs` created, remote `origin` set.
- Local git identity set: `Sushil Dalavi <75267758+sushildalavi@users.noreply.github.com>`.

## Milestone plan (status)
| M | Scope | Status |
|---|-------|--------|
| M0 | Skeleton: CMake, docs, basic exe, smoke test | in_progress |
| M1 | Node identity, framed TCP protocol, CLI, storage engine | todo |
| M2 | Membership + hash ring + successor placement | todo |
| M3 | Replicated create/get, checksums, idempotency, streaming | todo |
| M4 | Append segments, ordering, read-your-writes, merge, anti-entropy | todo |
| M5 | Failure detection, re-replication, restart reconcile, rebalance | todo |
| M6 | Concurrency, fault injection, 10-node demo, stress, sanitizers, CI | todo |
| M7 | Benchmarks, charts, docs, interview guide, resume claims | todo |

---

## M0 — Skeleton
Goal: a buildable repo with a real `aegisfs` executable, docs skeleton, unit-test harness, CI,
and a single-node smoke test. Target ~10 commits.

### Log
- Created repo meta (.gitignore, LICENSE, CLAUDE.md, build-status.json, this file).
- Architecture/invariants/tradeoffs docs written; CMake skeleton + GoogleTest FetchContent.
- Implemented and verified far beyond M0 in this session (see below).

---

## M1–M4 — Core distributed file system (DONE, verified)
Implemented and validated end-to-end:
- **M1**: node identity persisted (`node.id`); length-framed + streaming TCP protocol (POSIX
  sockets, bounded thread pool); CLI `serve` + client; crash-safe storage (stage→fsync→rename,
  orphan-segment GC on recovery).
- **M2**: SWIM-inspired gossip membership with incarnation numbers; consistent-hash ring;
  first-3-successor placement. `list_mem_ids` shows a clean 5-node ring (no phantom seeds).
- **M3**: replicated `create`/`get` across 3 successors; SHA-256 verified; first-create-only
  returns ALREADY_EXISTS; binary streaming (verified byte-identical round trip).
- **M4**: `append` with per-client seq + Lamport ordering; read-your-writes watermark in client;
  `merge` reconciles replicas (verified all 3 replicas byte-identical, matching SHA-256);
  reconcile_placement (push-based re-replication/rebalance + safe handoff) + pull anti-entropy.

### Verification commands run (real output observed)
- `ctest` → **33/33 unit tests pass** (hash, ring, protocol, manifest, storage, membership).
- `scripts/smoke_test.sh` → **SMOKE PASS** (3-node: membership converge, create/get identical,
  first-create-only, append+merge).
- Manual 5-node: create replicates to 3 nodes; `get` SHA matches; duplicate create →
  ALREADY_EXISTS; append → client_seq=1; merge → all 3 replicas identical SHA
  `2ca02a87…`; `getfromreplica` on each replica returns identical bytes.

### Bugs found & fixed (self-heal log)
- `Listener` lacked move ops → `listener_ = bind(...)` copied fd and the temporary's dtor closed
  the socket (node "listening" but refused connections). Added move ctor/assign + deleted copy.
- Seed placeholder members with synthetic ids polluted the ring → contact seeds by address in
  gossip instead of inserting fake members.
- **Self-loopback deadlock**: `h_append`/`h_merge` held the per-file lock then replicated to the
  same node, whose `h_replicate` waited on the same lock. Added `apply_segment_local` so a node
  writes its own replica in-process instead of looping back over TCP.
- Ring test bound a reference to a temporary vector element (dangling); test fixed.

## M5 — Failure handling & topology (DONE)
Failure detector (suspect→failed), push-based re-replication, restart recovery, join rebalance with
safe handoff, graceful `leave`, periodic anti-entropy (pull + corrupt-segment repair). Chaos tests:
`TwoNodeKillStillReadable`, `ReReplicationRestoresReplica`, `RestartNodeRecoversData`,
`JoinKeepsFilesAvailable`, `GracefulLeaveHandsOffData`, `AntiEntropyRepairsCorruptSegment` — all pass.

## M6 — Concurrency, fault injection, demo, sanitizers, CI (DONE)
- Concurrency tests (ordering/no-dup, single-winner create race, 10-way different files) + a seeded
  stress test (~720 ops, 8 clients). Found & fixed a cross-node lock-cycle deadlock and a SIGPIPE
  crash; found & fixed an accept/close TSan data race.
- `scripts/demo.sh`: 10-node demo with 2-node failure + re-replication (passes).
- Clean under ASan/UBSan (all suites) and ThreadSanitizer (concurrency/membership).
- CI workflow: build + ctest + ASan matrix + dedicated TSan job.

## M7 — Benchmarks & docs (DONE)
- `benchmarks/runner/bench.sh` + `bench_concurrency.sh`: real numbers in `benchmarks/results/`.
  Parallel fan-out cuts 100 MiB create latency ~16%; small-file result neutral (reported honestly).
- Docs: README, ARCHITECTURE (+Mermaid), INVARIANTS, TRADEOFFS, OPERATIONS, BENCHMARKS,
  INTERVIEW_GUIDE, RESUME_CLAIMS, CHANGELOG, CONTRIBUTING, SECURITY.

## Later additions (M5–M7 polish)
Virtual nodes for ring balance (10-node spread 3..4294 → 645..1243); `metrics`/`leave`/`version`/
`shell` commands; `ls` verified-presence; tmp GC; 85 tests total.

## Final acceptance
Run `scripts/verify_definition_of_done.sh`. Docker Compose scenarios are NOT RUN (no Docker on the
dev host); the process-local 10-node path is verified instead. See `FINAL_REPORT.md`.
