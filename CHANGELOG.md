# Changelog

All notable changes to AegisFS. Format loosely follows Keep a Changelog; this project is pre-1.0.

## [0.1.0] - 2026-11-22
Initial working release of the AegisFS distributed file system.

### Added
- Consistent-hash ring with O(log N) first-3-successor placement (RF=3).
- SWIM-inspired gossip membership with suspicion and incarnation numbers.
- Crash-safe segmented storage: immutable base + append segments, atomic manifest commits,
  orphan-segment GC on recovery.
- Replicated `create`/`get`/`append`/`merge` with SHA-256 integrity, first-create-only,
  per-client ordering, Lamport cross-client convergence, idempotent retries, read-your-writes.
- Failure handling: 2-node-failure readability, push-based re-replication, join rebalance with
  safe handoff, graceful `leave`, and periodic anti-entropy (pull + corrupt-segment repair).
- Length-framed, versioned, bounds-checked TCP protocol with bounded thread pool and streaming
  transfers (100+ MiB without buffering).
- CLI: `serve`, `create`, `get`, `append`, `merge`, `ls`, `liststore`, `getfromreplica`,
  `list_mem_ids`, `multiappend`, `status`, `health`, `leave`, `metrics`, `version`, `shell`.
- Optional parallel replica fan-out (`--parallel`), ~16% faster 100 MiB creates.
- 53+ tests (unit/integration/concurrency/chaos/stress); ASan/UBSan + TSan clean.
- Docs: architecture (with diagrams), invariants, tradeoffs, operations, benchmarks, interview
  guide, resume claims. CI (build + ctest + ASan matrix + TSan). Dockerfile + compose (not run
  on the dev host; process-local verified).

### Known limitations
- No sub-file striping (a file's replicas are its 3 ring successors).
- No transport encryption/auth (see SECURITY.md).
- Docker Compose topology provided but not exercised on the dev host (no Docker installed).
