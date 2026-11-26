# Integration architecture

AegisFS has two optional integrations: a Cassandra metadata index and an HDFS import/export
connector. Neither is part of the storage engine. This page explains where they sit, what each one
does and does not do, and how the rest of the system is kept independent of them.

## What AegisFS does by itself

The C++ storage nodes own everything that decides whether data is safe:

- placement on the consistent-hash ring, with three replicas per file
- membership and failure detection
- immutable segments, checksummed manifests, and the fsync-then-rename write path
- append ordering, idempotent retries, read-your-writes, and quorum reads
- repair: anti-entropy, scrubbing, and re-replication after a failure
- TLS and the cluster token
- client-side striping of large files

None of that code calls Cassandra or HDFS, and none of it changes when they are absent. The only
C++ change made for the integrations is one read-only client command, `members`, which prints the
membership table with each node's address as `key=value` lines so an outside tool can visit every
node. `listMemIds` keeps its original layout.

## Where the integrations sit

```
   people, scripts, the REST gateway
                 |
                 v
   +-----------------------------+
   |  aegisfs client (CLI)       |  TLS, cluster token, retries, striping
   +-----------------------------+
          |                 ^
          v                 |  reads and writes only through the client
   AegisFS storage nodes    |
   (the source of truth)    |
                            |
        +-------------------+--------------------+
        |                                        |
   Cassandra index                          HDFS connector
   (what AegisFS reported)                  (WebHDFS import / export)
        |                                        |
     Cassandra                                 HDFS
```

Both integrations are one Python package, `extensions/integrations/`, started with
`aegisfs-integrations`. They reach the cluster only by running the same `aegisfs` client binary a
person would run. That is deliberate:

- TLS, the cluster token, retries and striping behave exactly as the cluster defines them. A striped
  file is read back whole by `get`, and a large `create` is striped by the client's own setting.
  The integrations never read segments or manifests directly and have no second striping format.
- Nothing in the storage nodes has to know an integration exists.

## The Cassandra index

Role: remember what AegisFS reported about each file, so names can be listed and found without
asking every storage node.

Cassandra is **not** the source of truth. Every entry is an observation with an `observed_at`
time. A lookup that succeeds in Cassandra never overrides an AegisFS error: `find --confirm` and
`show --confirm` ask AegisFS as well and report any disagreement, and reads and writes of the files
themselves never go through the index.

The index only holds what AegisFS can verify. `listStore` reports a name, id, size, version and
segment count per node, and `ls` reports replica placement. Neither reports a checksum, so a SHA-256
is stored only when the indexer actually read the file and hashed it, together with when it did. A
striped file's logical size is unknown until it is read, and the index says so instead of reporting
the size of the small index object. Details are in [cassandraIntegration.md](cassandraIntegration.md).

How it stays in step:

1. A file operation completes in AegisFS.
2. The REST gateway can queue a "this file changed" line in a local spool (off unless
   `AEGISFS_INT_CASSANDRA_SPOOL_DIR` is set). The hook only appends to a file and ignores its own
   errors, so it cannot change what the client is told.
3. The indexer later looks the file up in AegisFS and writes what it actually sees.
4. `index --reconcile` scans every alive node's inventory, adds missing entries, updates changed ones
   and marks entries whose file has gone as stale instead of dropping them. This also finds files
   written directly with the CLI, which no hook would see.
5. If Cassandra is unreachable, work stays in the spool and is replayed later. The original file
   operation never depends on any of this.

Writes use the AegisFS file version as an ordering guard, so a repeated or late event cannot replace
a newer observation.

## The HDFS connector

Role: copy a file between AegisFS and an existing HDFS cluster, over the WebHDFS REST API, and prove
the copy is identical.

HDFS is **not** a storage tier, a replica, or a backup managed by AegisFS. It is an external system
the user chooses to copy files to or from. AegisFS does not read from HDFS to serve a request, and a
file in HDFS has no effect on a file's replication or repair in AegisFS.

- **Export** reads the whole logical file with `aegisfs get`, uploads it to a temporary name in HDFS,
  reads it back from HDFS and compares SHA-256, and only then renames it into place.
- **Import** streams the HDFS file to local disk while hashing it, creates it in AegisFS through the
  normal client (so replication, durability and striping apply as for any other write), then reads it
  back through AegisFS and compares.
- Nothing is overwritten unless `--overwrite` is given, and neither the AegisFS file nor the HDFS
  file is ever deleted by a transfer. The only thing the connector deletes is its own temporary file.

WebHDFS sends a write to the NameNode, which answers with a redirect to a DataNode, and the data
then goes to the DataNode. The connector follows that redirect itself so it can check it first. See
[hdfsIntegration.md](hdfsIntegration.md) and [integrationSecurity.md](integrationSecurity.md).

## Independence, and how it is checked

| Claim | How it is enforced | What checks it |
| --- | --- | --- |
| The core cluster runs without either integration. | Neither is imported or called by the C++ code. The Compose services are in the `cassandra` and `hdfs` profiles. | `scripts/integrations/checkProfiles.sh` shows the merged Compose files list exactly the core services when no profile is chosen. The core tests and cluster checks run with no integration installed. |
| AegisFS reads and writes keep working when Cassandra is down. | The gateway hook only appends to a local file. The indexer is a separate process. | A live test stops the Cassandra container and does an AegisFS `create` and `get`. |
| AegisFS reads and writes keep working when HDFS is down. | Only the `hdfs` commands talk to HDFS. | A live test makes HDFS unreachable and does an AegisFS `create` and `get`. |
| Each integration can be on or off on its own. | Separate profiles, separate Python areas, separate configuration sections. | The profile check, and each live suite running without the other. |
| A missing Python driver or service stops only its own area. | An unavailable area reports `unavailable` and exits 3; the other area still loads. | `testCli.py` and the area unit tests. |

## What the integrations do not do

- They do not make AegisFS faster or more durable, and they are not part of the benchmarks that
  describe the storage engine.
- They do not add a coordinator. Nothing in the cluster waits for either of them.
- The Cassandra index can lag behind AegisFS, and a reconcile is a scan, not a stream.
- The HDFS connector copies whole files only: no incremental sync, no deletes, no metadata carry-over.
- Neither has been run across separate physical machines. Everything here was measured on one laptop.
