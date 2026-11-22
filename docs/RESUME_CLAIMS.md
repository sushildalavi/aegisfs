# AegisFS — Resume Claims (verified)

Exactly three resume-ready bullets. Each is backed by code and a passing test or a real
measurement in this repository — nothing aspirational.

1. **Built an original fault-tolerant distributed file system in C++20** (consistent-hash placement,
   SWIM-style gossip membership with incarnation numbers, RF=3 replication across ring successors)
   that keeps files readable through **2 simultaneous node failures** and automatically
   re-replicates to restore redundancy.
   *Evidence:* `Chaos.TwoNodeKillStillReadable`, `Chaos.ReReplicationRestoresReplica`
   (`tests/chaos/failure_test.cpp`); verified live in `scripts/demo.sh` on a 10-node cluster.

2. **Designed a crash-safe, strongly-ordered write path** — immutable segmented storage with
   atomic (fsync + rename) manifest commits, per-client append ordering, idempotent retries, and
   read-your-writes — proven byte-identical across all replicas after `merge`, with correct
   crash recovery of uncommitted writes.
   *Evidence:* `Replication.ReadYourWrites`, `Replication.IdempotentReplay`,
   `Replication.MergeMakesReplicasIdentical`, `StorageFixture.CrashRecoveryDropsOrphanSegment`;
   53 unit/integration/concurrency/chaos tests pass, clean under ASan/UBSan and ThreadSanitizer.

3. **Measured and optimized the replication path**: parallel replica fan-out reduced **100 MiB
   create latency ~16%** (647 ms → 541 ms, ~157 → ~189 MB/s) over sequential fan-out, with reads
   streaming at ~530 MB/s — all reproducible via a committed benchmark harness, with small-file
   results reported honestly (neutral, since transfer time is below fan-out overhead).
   *Evidence:* `benchmarks/runner/bench.sh`, `benchmarks/results/*`, `docs/BENCHMARKS.md`.

> All claims describe this independent project (inspired by HDFS/Cassandra/HyDFS concepts), not any
> employer or course work.
