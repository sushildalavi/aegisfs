# AegisFS Interview Guide

Realistic questions with precise answers, grounded in the actual implementation. File references
point at the code path that implements each answer.

### 1. How are objects placed? Why consistent hashing?
Each file maps to `ringId(filename)` = top 64 bits of SHA-256 (`src/hash.cpp`). The ring is the
sorted set of live node ring ids; a file's replicas are the first 3 **distinct** successors
(`Ring::successors`, `src/ring.cpp`). Consistent hashing means adding/removing a node only remaps a
contiguous slice of the key space, so rebalancing moves ~1/N of keys, not all of them
(`RingTest.MinimalMovement`).

### 2. Why SHA-256 ids and not `std::hash`?
`std::hash` is per-process and unspecified across runs/implementations — fatal for a persistent
ring shared by many processes. SHA-256-derived ids are stable and uniform
(`HashTest.RingIdDeterministic`).

### 3. Is routing O(1)?
No. Lookup is a binary search over the sorted ring: **O(log N)**. We state this honestly rather
than claiming O(1) (`Ring::firstAtOrAfter`).

### 4. How does membership work, and how do you avoid resurrecting dead nodes?
SWIM-inspired gossip: each node periodically exchanges its member table with a few peers and seeds
(`Node::gossip_once`). A failed probe marks a node `SUSPECT`, which escalates to `FAILED` after a
bounded window (`Membership::tick`). **Incarnation numbers** order conflicting rumors: a higher
incarnation always wins, and a node refutes a false `SUSPECT`/`FAILED` about itself by bumping its
own incarnation (`Membership::merge`, `MembershipTest.IncarnationRefute`).

### 5. Walk through a write (create/append).
The client streams bytes to any node (the per-op **coordinator**, not a cluster leader). The
coordinator spools the payload to a temp file (bounded memory), computes its SHA-256, assigns an
operation id, then fans the segment out to the 3 replicas (`Node::hCreate`/`hAppend`,
`Node::fanout`). Each replica persists the segment (stage→fsync→rename) and commits its manifest
atomically. The write is `OK` only if all 3 replicas commit; otherwise `DEGRADED`.

### 6. How is per-client append order preserved despite retries and network reordering?
Within a client, appends are serialized by the client (it waits for an ack before issuing the next),
and each append carries a per-client monotonically increasing `clientSeq`. Segments are ordered by
`(base-first, lamportTs, clientId, clientSeq)` (`SegmentRef::order_before`). So append A (lower
seq) always precedes later append B from the same client on every replica.

### 7. How do concurrent appends from different clients converge?
Each op gets a Lamport timestamp from the coordinator's logical clock
(`Node::nextLamport`/`observeLamport`). The total order `(lamport, clientId, clientSeq)` is
deterministic, so once the cluster is quiescent all replicas reconstruct the identical byte sequence.
Lamport order is causal, not wall-clock — a deliberate tradeoff (`docs/tradeoffs.md`).

### 8. What makes retries idempotent?
Every op has a unique `opId`. A replica's `add_segment` is a no-op if the `opId` already exists
(`Manifest::add_segment`, `ManifestTest.Idempotent`), and `apply_segment_local` short-circuits a
duplicate op. So replaying the same append at-most-once-applies (`Replication.IdempotentReplay`).

### 9. How is read-your-writes implemented?
The client tracks, per file, the highest `clientSeq` it has had acknowledged (`Client`). `get`
sends that watermark; the coordinator only serves a replica whose manifest already contains an op at
or above it, else it returns `STALE` rather than stale bytes (`Node::hGet`,
`Replication.ReadYourWrites`).

### 10. What exactly does `merge` guarantee?
In a quiescent, healthy cluster, `merge` reconciles every reachable designated replica to the union
of committed ops in canonical order, then verifies all three reconstruct to the **same SHA-256**; it
returns `OK` only then, `CONFLICT` otherwise (`Node::hMerge`,
`Replication.MergeMakesReplicasIdentical`). It never falsely succeeds.

### 11. How do you survive 2 node failures?
RF=3 on distinct successors. Killing 2 of a file's 3 replicas leaves the third — which by ring
geometry moves up to primary — still serving (`Chaos.TwoNodeKillStillReadable`). The read path tries
replicas in order and serves the first reachable one that meets the watermark.

### 12. How does re-replication work after a failure?
On the maintenance loop, each node runs `reconcilePlacement()`: for every local file it computes the
current successor set and **pushes** any missing ops to replicas that lack them. A new successor
thus receives the data from a surviving replica without needing a global file index
(`Chaos.ReReplicationRestoresReplica`).

### 13. What about a node join — how do you avoid transient unavailability?
Safe handoff: when a node is no longer a designated replica for a file, it deletes its local copy
**only after** confirming every new designated replica has the full file (`reconcilePlacement`,
`Chaos.JoinKeepsFilesAvailable`). The file is never unavailable because an old replica was deleted
too early.

### 14. What happens on a crash between writing data and committing the manifest?
The op was never acknowledged, so correctness requires it to vanish. On restart, recovery deletes
orphan segments not referenced by the committed manifest and wipes the staging dir
(`StorageEngine::recover`, `StorageFixture.CrashRecoveryDropsOrphanSegment`). No torn file.

### 15. How do you stream 100 MiB without blowing up memory?
All transfers move in bounded 1 MiB chunks via pull/sink callbacks; segments are hashed and written
incrementally (`writeFrameStreaming`/`streamPayloadIn`, `StorageEngine::putSegmentStream`).
The 64 MiB integration test and 100 MiB benchmark exercise this.

### 16. Durability semantics — when is data safe?
A segment is durable on a replica after fsync + atomic rename of both the segment and the manifest
(`commitManifest`). A write is acknowledged only when all 3 replicas reach that state, so any 2 can
fail afterward without data loss.

### 17. How do you handle a false-positive failure detector?
Suspicion is a soft state; a suspected node refutes by gossiping a higher incarnation, flipping
itself back to `ALIVE` everywhere (`Membership::merge`). Re-replication is idempotent, so even if it
starts before a refute, no data is duplicated or lost.

### 18. Where's the concurrency control? A global lock?
No global cluster lock. There is a per-file lock (`shared_ptr<mutex>` keyed by file id) that guards
**only** the short, purely-local manifest mutation (`apply_local_locked`). Coordinators never hold a
lock across network fan-out — doing so caused a cross-node deadlock that we found and fixed (see the
`fix(node)` commit); the fix is why `Concurrency.*` tests pass.

### 19. CAP stance under partition?
CP-leaning for the partitioned minority: writes need all 3 replicas for a durable ack (else
`DEGRADED`), and reads refuse to serve below the watermark (`STALE`). We do not claim unrestricted
partition tolerance (`docs/tradeoffs.md`).

### 20. Protocol design — how is framing safe?
`MAGIC | VERSION | TYPE | HEADER_LEN | HEADER | PAYLOAD_LEN | PAYLOAD`, little-endian, every length
bounds-checked against caps before allocation (`src/protocol.cpp`, `src/net.cpp`,
`ProtocolTest.RejectsTruncated`). Header fields are length-prefixed and binary-safe.

### 21. What are the main bottlenecks, and what did you measure?
`create` is replication-bound; parallel fan-out cuts 100 MiB create latency ~16% vs sequential
(`docs/benchmarks.md`). For small files the thread overhead cancels the gain — a real,
un-massaged result. `get` is single-replica and reaches ~530 MB/s over loopback.

### 22. How would this scale past tens of nodes?
Gossip membership scales to hundreds with fanout tuning. The main limits here are one-thread-per-
connection IO and no sub-file striping; adding striping (place segments on independent successor
sets) and async IO would be the next steps. Stated as future work, not implemented.

### 23. What would you do with more time?
Hinted handoff for degraded writes, vector-clock conflict detection for truly concurrent
cross-client edits, a pluggable async networking backend, and TLS between nodes.
