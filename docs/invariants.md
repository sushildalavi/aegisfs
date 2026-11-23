# Invariants

The properties the code is meant to keep, and the tests that check each one. Test names are
`Suite.Name` as `ctest -N` lists them.

| # | Invariant | Checked by |
|---|-----------|------------|
| I1 | A ring id depends only on its input, never on the process (SHA-256, not `std::hash`). | `RingTest.StableAcrossInstances`, `HashTest.RingIdDeterministic` |
| I2 | A file's replicas are its first three distinct ring successors, including across the wrap-around. | `RingTest.ThreeDistinct`, `RingTest.SuccessorsWrap` |
| I3 | Adding or removing a node only changes ownership of the ranges next to it. | `RingTest.MinimalMovement` |
| I4 | Nodes with the same membership view compute the same replica set. | `RingTest.AgreementAcrossNodes` |
| I5 | Only the first `create` of a name succeeds; later ones return `ALREADY_EXISTS`. | `Integration.FirstCreateOnly` |
| I6 | `append` to a missing file fails and never creates it. | `Integration.AppendMissingFileErrors` |
| I7 | A client's appends appear in the order it issued them on every replica. | `ManifestTest.PerClientOrder`, `Integration.RepeatedAppendAccumulates`, `Replication.MultiClientAppendsAllPresentOrdered` |
| I8 | A retried operation (same op id) is applied at most once. | `ManifestTest.Idempotent`, `Replication.IdempotentReplay` |
| I9 | `get` returns the calling client's own acknowledged appends. | `Replication.ReadYourWrites`, `Replication.WatermarkAdvances` |
| I10 | Segment checksums are verified on write, read and replication. | `StorageFixture.ChecksumRoundTrip`, `StorageFixture.RejectsWrongChecksum`, `Integration.CreateGetByteIdentical` |
| I11 | A crash between writing a segment and committing its manifest leaves no torn file. | `StorageFixture.CrashRecoveryDropsOrphanSegment` |
| I12 | After a successful `merge` on a quiet cluster, all replicas hold identical bytes. | `Replication.MergeMakesReplicasIdentical` |
| I13 | A file with three healthy replicas stays readable after two node failures. | `Chaos.TwoNodeKillStillReadable` |
| I14 | After a node fails, the replica count is restored by re-replication. | `Chaos.ReReplicationRestoresReplica` |
| I15 | A node joining does not make existing files unavailable. | `Chaos.JoinKeepsFilesAvailable` |
| I16 | A stale gossip message cannot revive a failed node or change its address. | `MembershipTest.HigherIncarnationWins`, `MembershipTest.StaleIncarnationCannotChangeAddress`, `MembershipTest.IncarnationRefute` |
| I17 | Large files move in bounded chunks and are never held in memory whole. | `Integration.LargeFileStreaming`, `NetTest.StreamingRoundTrip` |
| I18 | File names are validated; path traversal is rejected. | `StorageFixture.PathValidation`, `Integration.InvalidFilenameRejected` |
| I19 | Concurrent appends to one file never produce a torn manifest or a duplicate segment. | `Concurrency.ConcurrentAppendsNoDuplicateOrTornManifest` |
| I20 | `getFromReplica` has no side effects on the replica it reads. | `Integration.GetFromReplicaPurityAndAgreement` |
| I21 | When several clients create the same name at once, exactly one wins. | `Concurrency.SimultaneousCreateExactlyOneWins` |
| I22 | A corrupted segment on disk is detected and repaired from a healthy peer. | `Chaos.AntiEntropyRepairsCorruptSegment` |
| I23 | A node that leaves gracefully hands its data off first. | `Chaos.GracefulLeaveHandsOffData` |
| I24 | A mixed workload from several clients stays consistent. | `Stress.MixedWorkloadThousandsOfOps` |
| I25 | No lock is held across a network call, so two coordinators cannot deadlock each other. | `Concurrency.*` |
| I26 | A write that reaches fewer than `rf` replicas is reported as `DEGRADED`, even when the ring itself has fewer than `rf` members. | `Integration.CreateOnTwoNodeRingIsDegraded` |
| I27 | A single unreachable link is not treated as a node failure, but a node that nobody can reach is. | `Chaos.IndirectProbeSurvivesPointToPointPartition`, `Chaos.TrueIsolationStillDetectedAsFailed` |
| I28 | A coordinator failing mid-create does not leave an orphaned base segment. | `Chaos.PrimaryFailureDuringCreateDoesNotOrphanBase` |
| I29 | A crash at any step of a segment or manifest write leaves the file exactly as it was or exactly the new version, with no orphans or temp files after restart. | `EveryStep/CrashPoints.AppendIsAllOrNothingAfterRestart/*`, `StorageFixture.CrashDuringFirstCreateLeavesNoFile` |
| I30 | Every request must carry the cluster token, TLS clients verify the server certificate and host, and a client or node without credentials cannot talk to the cluster. | `Security.TokenIsRequiredOnEveryRequest`, `Security.AuthTokenComparison`, `Security.TlsClusterWorksAndRejectsOutsiders`, `Security.MutualTlsRejectsClientsWithoutACertificate`, `scripts/checks/reliabilityTest.sh` |

## What is and is not covered

The C++ suite starts real nodes inside one test process, on separate localhost ports, with separate
data directories. `scripts/checks/reliabilityTest.sh` repeats the two-failure case with separate OS
processes and sends malformed frames to a live node.

Not covered by any test today:

- Real network partitions (packet loss, delay, one-way links). The partition tests simulate an
  unreachable peer inside the process; they do not use `tc`, network namespaces or a proxy.
- The Docker Compose stack. The files are there but Docker has not been available to run them.
- Linearizability. Appends from different clients converge to one deterministic order, but that
  order comes from Lamport timestamps and not from wall-clock time.
