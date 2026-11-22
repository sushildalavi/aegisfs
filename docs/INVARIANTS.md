# AegisFS Invariants

Each invariant lists the test(s) that demonstrate it. "NOT RUN" marks tests that need an
environment feature (e.g. Docker) absent on the build host; those run process-local where possible.

| # | Invariant | Demonstrated by |
|---|-----------|-----------------|
| I1 | `ring_id` is stable across processes/runs (SHA-256 derived; never `std::hash`). | `ring_test.StableAcrossInstances` |
| I2 | Ring lookup returns the first 3 **distinct** successors, correct across wraparound. | `ring_test.SuccessorsWrap`, `ring_test.ThreeDistinct` |
| I3 | Adding/removing a node changes ownership only for the affected contiguous ranges. | `ring_test.MinimalMovement` |
| I4 | A file maps to exactly one ordered replica set; all nodes agree given the same membership. | `ring_test.AgreementAcrossNodes` |
| I5 | Only the first `create` for a name succeeds; later creates return `ALREADY_EXISTS`. | `integration: create_once` |
| I6 | `append` on a missing file errors; never implicitly creates. | `integration: append_missing` |
| I7 | Per-client append order is preserved on every replica. | `manifest_test.PerClientOrder`, `integration: ordered_append` |
| I8 | Retried ops (same op-id) are applied at most once (idempotent). | `manifest_test.Idempotent`, `integration: retry_idempotent` |
| I9 | `get` returns the invoking client's own completed appends (read-your-writes). | `integration: read_your_writes` |
| I10 | Segment + file SHA-256 verified on write, read, replicate, and recovery. | `storage_test.ChecksumRoundTrip`, `integration: binary_integrity` |
| I11 | Crash between data-write and manifest-commit recovers to a consistent state (no torn file). | `storage_test.CrashRecovery` |
| I12 | After a successful quiescent `merge`, all designated replicas are byte-identical. | `integration: merge_identical` |
| I13 | A file with 3 replicas stays readable after ≤2 node failures. | `chaos: two_node_kill` |
| I14 | Failed node → automatic re-replication restores RF=3 (checksum-verified) within bounded time. | `chaos: re_replication` |
| I15 | Node join → rebalance with safe handoff; file never transiently unavailable. | `chaos: join_rebalance` |
| I16 | Incarnation numbers prevent a stale gossip message from resurrecting a failed node. | `membership_test.IncarnationRefute` |
| I17 | 100+ MiB files transfer without loading the whole file into RAM (bounded chunking). | `integration: large_file_streaming` |
| I18 | Filenames are normalized/validated; path traversal (`../`) is rejected. | `storage_test.PathValidation` |
| I19 | Concurrent appends to one file produce no torn manifest / duplicated segment. | `Concurrency.ConcurrentAppendsNoDuplicateOrTornManifest` |
| I20 | `getfromreplica` has no read-repair / merge side effects. | `Integration.GetFromReplicaPurityAndAgreement` |
| I21 | Concurrent creates of the same name: exactly one wins (primary-arbitrated). | `Concurrency.SimultaneousCreateExactlyOneWins` |
| I22 | Anti-entropy detects and repairs an on-disk corrupt segment from a healthy peer. | `Chaos.AntiEntropyRepairsCorruptSegment` |
| I23 | Graceful leave hands off replicas; files stay available. | `Chaos.GracefulLeaveHandsOffData` |
| I24 | Sustained mixed workload (hundreds of ops, 8 clients) stays consistent. | `Stress.MixedWorkloadThousandsOfOps` |
| I25 | No cross-node lock cycle under concurrent create/append (coordinator holds no lock across fan-out). | `Concurrency.*` (regression for the `fix(node)` deadlock) |

## Test inventory
53+ tests across `tests/{unit,integration,concurrency,chaos}/`, plus `scripts/smoke_test.sh` and
`scripts/demo.sh`. Clean under AddressSanitizer/UBSan; concurrent paths clean under ThreadSanitizer.
