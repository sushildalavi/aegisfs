# Design trade-offs

## Own TCP protocol instead of gRPC or Asio

Length-framed binary RPC over blocking POSIX sockets, served by a bounded thread pool. This keeps
the dependencies to OpenSSL and the standard library and keeps framing and streaming in plain view.
The cost is a thread per in-flight request, one connection per request, and a protocol that has to be
bounds-checked by hand. It suits a cluster of tens of nodes.

## Successor placement, with striping in the client

A file's replicas are the first three ring successors of its name, and appends add segments to the
same replica set. Placement is a pure function of membership, which keeps `ls`, repair and merge easy
to reason about. A large file would otherwise sit on only three nodes, so the client splits files
above the stripe size into chunk files that the ring places independently. The nodes treat chunks as
ordinary files. The price is that `ls`, `merge` and `getFromReplica` see only the index file, and a
failed create can leave orphan chunks.

## Consistency under partition

A write's first attempt reports `OK` only when all `rf` replicas have committed, and `DEGRADED` with a
count otherwise (a retried write can report `OK` with fewer, see
[Known limitations](../README.md#known-limitations)). Reads that cannot satisfy the client's
watermark return `STALE`. The alternative was an always-writable design with hinted handoff and
quorum reads. Read-your-writes and survival of two failures are easier to state and test this way. A `DEGRADED` write has still been stored on some
replicas and will spread to the others through repair, so it is not rolled back.

## Lamport order instead of consensus

Segments are ordered by Lamport timestamp with a deterministic tie-break, so replicas converge
without a consensus round per append. The order is not wall-clock order, and aegisfs makes no
linearizability claim. The current coordinator logic also leaves a gap in per-client ordering, listed
under [Known limitations](../README.md#known-limitations).

## Gossip membership

An eventually consistent table with suspicion and incarnation numbers needs no membership leader and
tolerates lost messages. Nodes can briefly disagree about placement, so repair has to be idempotent
and a node only drops a file after the designated replicas report that they have it.

Slower failure detection avoids false positives. In the container cluster with TLS, a probe that
included a handshake sometimes exceeded the loopback defaults, which started suspicions that expired
before the node could refute them. The Compose file therefore uses 1 s probes and 6 s of suspicion,
at the cost of taking longer to replace a dead node.

## Plain files and binary manifests

Each file has a directory of immutable segments and a checksummed manifest, committed by fsync and
rename. An embedded database was considered. Plain files need no dependency and make recovery easy to
inspect.

## Stat every cycle, hash on a schedule

The maintenance loop checks segment existence and size on every cycle and re-hashes a file once per
`--scrub-interval-ms`. The first version re-hashed everything every second, which pushed 100 MiB
creates from about 250 ms to several seconds once a few such files were stored. Corruption that keeps
the size can now go unnoticed for up to the interval. A serving replica still verifies checksums
before it sends data, so the corruption is not returned to a reader.

## Local processes and containers

`scripts/cluster.sh` runs each node as a process on its own loopback port. It needs nothing but the
binary and gives real TCP, separate data directories and real kills, but one network stack. The
Compose setup gives every node its own address and name, and is where the network fault tests run.
Neither covers separate physical machines.

## What would come next

- Forward data to replicas while it is still arriving. Today a segment is fully spooled on the
  coordinator before it is forwarded.
- Hinted handoff for degraded writes.
- Version vectors to detect concurrent updates from different clients.
- Batched or digest-based anti-entropy, so that repair does not ask for one manifest per file per
  cycle.
- Runs across separate machines.
