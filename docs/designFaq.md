# Design FAQ

Questions I expect about how AegisFS works, answered against the code as it is today. Function and
test names are real; `docs/architecture.md` has the longer description.

### How is a file placed on the cluster?

A file name maps to `ringId(name)`, the top 64 bits of its SHA-256. The ring is the sorted set of
live nodes' ring ids, and a file's replicas are the first three distinct successors of its id
(`Ring::successors` in `src/ring.cpp`). Because only the neighbours of a joining or leaving node
change owner, rebalancing moves about 1/N of the files rather than all of them
(`RingTest.MinimalMovement`).

### Why SHA-256 ids instead of `std::hash`?

`std::hash` is not guaranteed to give the same value in two processes or two builds, and every node
has to agree on placement. A SHA-256 prefix is stable and spread evenly (`HashTest.RingIdDeterministic`).

### Is routing O(1)?

No. It is a binary search over the sorted ring, so O(log N).

### How does membership work?

Gossip in the style of SWIM. Each node exchanges its member table with a few peers
(`Node::gossipOnce`). A failed probe makes a peer `SUSPECT`, and after a timeout `FAILED`. Before
suspecting a peer, a node asks a few others to probe it as well, so one broken link does not get a
healthy node declared dead (`Chaos.IndirectProbeSurvivesPointToPointPartition`).

Incarnation numbers order conflicting rumours. The higher one wins, and a node that hears it has been
suspected bumps its own incarnation to refute that (`Membership::merge`,
`MembershipTest.IncarnationRefute`).

### What happens on a write?

The client streams the bytes to any node, which acts as coordinator for that one operation. The
coordinator spools the upload to a temp file, hashing as it goes, assigns an operation id, and sends
the segment to the three replicas (`Node::hCreate`, `Node::hAppend`, `Node::fanout`). Each replica
fsyncs the segment, renames it into place and commits its manifest atomically.

The write returns `OK` only if three replicas committed. Anything less is `DEGRADED`, with the
number that did commit in the reply.

### How is a client's append order kept?

A client waits for the ack on one append before sending the next, and every append carries a
per-client sequence number. Segments sort by `(base first, lamportTs, clientId, clientSeq)`
(`SegmentRef::orderBefore`), so one client's earlier append always sorts before its later one.

### How do appends from different clients converge?

Each operation gets a Lamport timestamp from the coordinator's clock (`Node::nextLamport`,
`Node::observeLamport`). The tuple above is a total order, so once the cluster is quiet every replica
rebuilds the same byte sequence. That order follows Lamport time, not wall-clock time: two
concurrent appends from different clients land in a deterministic order, which need not be the order
in which a stopwatch saw them.

### What makes a retry safe?

Each operation has a unique id. `Manifest::addSegment` ignores an id it has already recorded, and
`Node::applySegmentLocal` returns early for a duplicate (`ManifestTest.Idempotent`,
`Replication.IdempotentReplay`).

### How does read-your-writes work?

The client remembers the highest sequence number the cluster acknowledged for each file and sends it
with `get`. A replica only serves the read if its manifest already contains that operation;
otherwise another replica is tried, and if none qualifies the answer is `STALE` instead of old bytes
(`Replication.ReadYourWrites`).

### What does `merge` promise?

On a quiet, healthy cluster it brings every reachable replica to the union of committed operations
in canonical order, then checks that all of them hash the same. It returns `OK` only then, and
`CONFLICT` otherwise (`Replication.MergeMakesReplicasIdentical`).

### How can it lose two nodes and stay readable?

Each file lives on three distinct nodes, so killing two leaves one, which still serves reads
(`Chaos.TwoNodeKillStillReadable`). This only holds for files that had three healthy copies before
the failures. A write that was `DEGRADED` has no such promise.

### How does re-replication work?

The maintenance loop on each node calls `Node::reconcilePlacement`. For every file it holds, it works
out the current replica set and pushes any operations a designated replica is missing. A new
successor gets its data from a surviving replica, with no global file index involved
(`Chaos.ReReplicationRestoresReplica`).

### How does a node join without making files unavailable?

A node that stops being a designated replica deletes its copy only after every new designated
replica confirms it has the whole file (`Chaos.JoinKeepsFilesAvailable`).

### What if a node crashes between writing a segment and committing the manifest?

That write was never acknowledged, so it has to disappear. On restart, `StorageEngine::recover`
removes segments the committed manifest does not reference and clears the staging directory
(`StorageFixture.CrashRecoveryDropsOrphanSegment`).

### Can one big file be spread over more than three nodes?

Yes. A client splits a file larger than `--stripe-mib` (256 MiB by default, `0` turns it off) into
chunk files named `<name>.stripe.<client>.<time>.<n>`. Each chunk is an ordinary file, so the ring
places it on its own three nodes and it is replicated, checksummed and repaired like any other. The
file keeps its real name as a small index listing the chunks (`Client::createStriped`). The index is
written last, so it is the commit point: a reader never sees a file whose chunks are not all stored.
A failed create can leave unreferenced chunk files behind, since there is no delete command yet.

The create op id of the index contains `#stripe#`, which is how the nodes know. A `get` reply says
`striped=1` and the client then fetches up to four chunks at a time and stitches them together. An
`append` to a striped file is refused by the coordinator with `striped=1`, and the client retries by
uploading the new data as chunks and appending their names to the index, so append order and
read-your-writes work through the index. `ls`, `merge` and `getFromReplica` act on the index only.

### How are 100 MiB files moved without loading them?

Everything goes through bounded 1 MiB chunks. Segments are hashed and written as they arrive
(`streamPayloadIn`, `StorageEngine::putSegmentStream`).

### How does the background integrity check work?

Each maintenance cycle every node compares each of its segment files against the manifest: the file
must exist and have the recorded size. That is one `stat` per segment. Reading every byte and
re-hashing it is expensive, so that runs once per file every `--scrub-interval-ms` (five minutes by
default), and a file's timer starts when it is first written. The first version re-hashed
everything every second, and on a laptop that was enough to slow 100 MiB creates from about 250 ms
to several seconds once a few files were stored.

### Is there a global lock?

No. There is a per-file mutex that covers only the short local manifest update
(`Node::applyLocalLocked`). A coordinator never holds it while talking to another node. An early
version did, and two coordinators writing to each other could deadlock. `Concurrency.*` guards
against that.

### Why are the failure-detector timings configurable?

The defaults (400 ms probes, 1.5 s of suspicion) were tuned on loopback. In the container cluster
with TLS on, a probe that has to complete a handshake sometimes took longer than that, and a single
slow probe could start a suspicion that ran out before the node refuted it, so a healthy node was
declared failed. The network fault test caught this intermittently. The compose file now runs
1 s probes and 6 s of suspicion, and it has been stable since. Slower detection means a really dead
node takes a few seconds longer to be replaced.

### What happens under a network partition?

The design leans toward consistency. A write needs all three replicas to be acknowledged as `OK`,
and a read that cannot meet the client's watermark says `STALE`. `scripts/checks/networkFaultTest.sh`
runs the ten-container cluster through a node cut off from the network (it is marked `FAILED`, the
majority keeps serving, and it catches up after rejoining), 15% packet loss, 80 ms of added latency,
and a one-way broken link. The in-process `Chaos.*` tests cover the same cases faster, without a
kernel network stack.

### What are the main bottlenecks?

For `create`, it is replication: the client hashes and uploads, the coordinator spools and hashes,
and then each replica spools and hashes again. Fanning out to the replicas in parallel was the first
improvement, and committing the spooled file by rename instead of copying it again was the second
(`docs/benchmarks.md` has the numbers). The remaining cost is one pass over 100 MiB per hop, because
a segment is fully spooled before it is forwarded.

### What would you do next?

Forward data to replicas while it is still arriving instead of after it is spooled; hinted handoff
for writes that are degraded; version vectors to detect truly concurrent edits from different
clients; and running the cluster across separate machines, which has only been done with containers on one.
