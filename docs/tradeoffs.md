# AegisFS Design Tradeoffs

Decisions, the alternatives I looked at, and why I chose what I did.

## Networking: hand-rolled POSIX TCP + thread pool (vs Boost.Asio / gRPC)
**Chosen:** length-framed binary RPC over blocking POSIX TCP sockets, served by a bounded thread pool.
**Alternatives:** Boost.Asio (async), gRPC/protobuf.
**Why:** zero heavyweight external networking dependency → reproducible build on a bare toolchain;
the framing/streaming logic is the interesting distributed-systems work and is clearer when owned.
**Cost:** one thread per in-flight connection (bounded); no built-in async multiplexing. Acceptable for
a ≤tens-of-nodes cluster. Framing is explicitly versioned and bounds-checked.

## Placement: successor-replica contract (vs arbitrary chunk hashing)
**Chosen:** all replicas of a logical file live on the file's first 3 ring successors; data is split into
immutable *segments* (base + per-append) but segments are **not** independently placed by chunk hash.
**Why:** preserves the HyDFS/Cassandra successor-replica model and makes `ls`, repair, and merge
reasoning simple; keeps a file's replicas co-located and predictable.
**Cost:** no sub-file striping across many nodes; a single huge file's replicas are 3 nodes, not N.

## Ring lookup: O(log N) binary search (vs O(1) claims)
Lookup is a binary search on a sorted ring, so O(log N), not O(1). Ownership changes on a
membership change are localized to the neighbouring ranges.

## Consistency vs availability under partition (CAP stance)
**Chosen:** favor consistency for reads (read-your-writes) and require all 3 replicas for a *durable*
ack in the healthy case. During degraded membership, writes return `DEGRADED` with the committed count
rather than silently accepting weaker durability; reads that cannot meet the client watermark return
`STALE` rather than serving stale bytes. This is CP-leaning for the partitioned minority.
**Alternative:** Dynamo-style always-writable with hinted handoff + quorum reads (AP-leaning).
**Why:** read-your-writes and surviving two failures of three-replica data are easier to state and
test under a CP-leaning policy. Responses carry the committed and rf counts, so a client can see
how far a write got.

## Cross-client ordering: Lamport timestamp + stable tie-break (vs vector clocks / consensus)
**Chosen:** total order `(lamportTs, clientId, clientSeq)` for segments; per-client order from the
client serializing its own appends.
**Why:** gives deterministic convergence on quiescence without a consensus round per append.
**Cost:** Lamport order is not "real time"; concurrent appends from different clients may interleave in
a way that is deterministic but not wall-clock. Causal per-client order is always preserved.

## Membership: SWIM-inspired gossip + incarnation numbers (vs strongly-consistent membership)
**Chosen:** eventually-consistent gossip with suspicion and incarnation numbers.
**Why:** no central membership leader; tolerates message loss; refutes false positives.
**Cost:** brief windows where nodes disagree on membership; placement reconciliation is event-driven and
idempotent to tolerate this.

## Metadata: files on disk + binary manifests (vs embedded SQLite)
**Chosen:** plain segment files + a binary manifest per file, atomic-committed. SQLite considered for
local metadata but not required; avoids a hidden "centralized DB" smell and keeps recovery transparent.

## Process-local cluster vs. Docker Compose
**Chosen:** the 10-node cluster runs as separate processes on distinct localhost ports
(`scripts/cluster.sh`), which needs nothing but the binary. The Dockerfiles and compose file in
`deploy/docker/` do the same with containers, but I developed on a machine without Docker and
have never run them. The processes are real, with their own data directories, real TCP and real
failures via `kill`; what is missing is separate network stacks.

## Integrity checking: stat every cycle, hash on a schedule
**Chosen:** the maintenance loop stats every segment each cycle (missing or truncated files) and
fully re-hashes a file's segments once per `--scrub-interval-ms`, five minutes by default.
**Why:** the first version re-hashed everything each second. With a few 100 MiB files stored that
was hundreds of megabytes of SHA-256 per node per second, and 100 MiB creates went from about
250 ms to several seconds in a back-to-back benchmark.
**Cost:** silent corruption that keeps a file's size can sit undetected for up to the scrub
interval. Reads still verify checksums, so a bad segment is never served; it is just repaired
later.
