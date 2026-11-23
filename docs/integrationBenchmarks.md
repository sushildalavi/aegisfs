# Integration benchmarks

These numbers describe the two optional integrations. They say nothing about the AegisFS storage
engine, and the engine's own results in [benchmarks.md](benchmarks.md) are unchanged by this work:
the integrations are separate processes, and the only change to the C++ code was a read-only
`members` command.

Raw samples for everything below are in `benchmarks/integrations/`. Each folder has an
`environment.json` with the hardware and versions.

## Setup

- **Machine:** Apple M3, 8 cores, 16 GB RAM, macOS, Docker Desktop (8 GB for its virtual machine).
  Other work was running on the laptop, so the spread between runs is real and is shown.
- **Cassandra:** `cassandra:5.0.5`, one node, replication factor 1, in a container on loopback.
  Driver `cassandra-driver` 3.29.3, Python 3.9. Reads and writes at `LOCAL_QUORUM`, which on one
  node is the same as `ONE`. The index is split into 16 buckets per namespace.
- **HDFS:** `apache/hadoop:3.4.1`, one NameNode and two DataNodes, replication 2. The image only
  exists for amd64, so on this Apple-silicon machine HDFS runs under CPU emulation. Transfers are
  therefore slower here than they would be on native hardware.
- **AegisFS:** three nodes with TLS and the cluster token on, same machine.

One laptop and a single-node Cassandra say nothing about production scale. They show the cost of
the design, not how it behaves under real load or over a network.

## Cassandra index

Each measurement was repeated in 5 runs, each in a fresh keyspace.

| Measurement | Samples | Median | p95 | p99 |
| --- | --- | --- | --- | --- |
| Create an entry | 1,000 | 3.9 ms | 16.1 ms | 30.1 ms |
| Update an entry | 1,000 | 4.5 ms | 15.9 ms | 31.1 ms |
| Look up one file by name | 1,000 | 1.1 ms | 6.4 ms | 11.1 ms |
| List 100 files in a namespace | 200 | 17.6 ms | 50.1 ms | 96.3 ms |

Concurrent lookups by name, 5 runs each (the "clients" are threads in one Python process sharing a
driver session, so the Python interpreter limits them as well as Cassandra):

| Clients | Median ops/s | Min | Max |
| --- | --- | --- | --- |
| 1 | 507 | 475 | 971 |
| 4 | 1,577 | 1,143 | 2,387 |
| 16 | 2,232 | 2,146 | 3,619 |

Reconciling an inventory of 3,000 files against the index, 5 runs each:

| Pass | Median | Min | Max |
| --- | --- | --- | --- |
| First reconcile (index empty) | 4.5 s | 2.5 s | 8.8 s |
| Nothing changed | 1.7 s | 0.7 s | 1.9 s |
| One file changed | 1.1 s | 0.7 s | 5.6 s |

A reconcile reads the whole inventory and the whole index, so its cost grows with the number of
files. It is meant for periodic use, not as a continuous stream.

## HDFS transfers

Each transfer is a separate run of `aegisfs-integrations`, timed end to end, including the AegisFS
client, the upload, the read-back and the rename. The 1 MiB and 10 MiB rows have 5 samples, the 100 MiB
rows 3, and the 1 KiB rows 5 runs of 30 files.

| Operation | Median time | Median throughput | Min to max |
| --- | --- | --- | --- |
| Export 100 MiB | 3.07 s | 34 MB/s | 2.63 to 3.39 s |
| Import 100 MiB | 1.88 s | 56 MB/s | 1.87 to 2.34 s |
| Export 10 MiB | 0.48 s | 22 MB/s | 0.43 to 0.74 s |
| Import 10 MiB | 0.52 s | 20 MB/s | 0.41 to 0.74 s |
| Export 1 MiB | 0.35 s | 3.0 MB/s | 0.29 to 0.75 s |
| Import 1 MiB | 0.33 s | 3.2 MB/s | 0.32 to 0.45 s |
| Export 30 files of 1 KiB, one process per file | 15.4 s (0.51 s per file) | | 8.5 to 33.3 s |
| Import 30 files of 1 KiB, one process per file | 14.6 s (0.49 s per file) | | 8.9 to 32.7 s |
| Export 30 files of 1 KiB, one `export-all` | 12.7 s (0.42 s per file) | | 4.2 to 14.6 s |
| Import 30 files of 1 KiB, one `import-all` | 7.5 s (0.25 s per file) | | 4.2 to 13.9 s |

**Memory.** The peak resident memory of the Python process was about 25 MiB for 1 MiB, 10 MiB and
100 MiB transfers alike, which is what streaming in fixed chunks should look like. This is the
Python process only; it does not include the `aegisfs` client process or the Hadoop containers.
A transfer does use disk: an export stages one copy of the file locally and an import stages two.

**Small files.** Per-file time for 1 KiB files is dominated by fixed costs (several WebHDFS round
trips and starting the AegisFS client), not by data. The batch commands were not consistently faster
than one process per file here, and their journal writes are a likely reason.

### Compared with native AegisFS

For the same files, native `aegisfs create` and `get` took:

| Size | Native create | Native get |
| --- | --- | --- |
| 100 MiB | 0.41 s | 0.62 s |
| 10 MiB | 0.083 s | 0.085 s |
| 1 MiB | 0.040 s | 0.042 s |

This is not a like-for-like comparison and should not be read as one system being slower. A native
create writes the bytes once to AegisFS. An export reads the file out of AegisFS, uploads it,
reads it back from HDFS to hash it, and renames it; an import does the mirror image. Each byte moves
several times on purpose, in return for the check that the copy is identical. The HDFS side also
runs under emulation here.

## What these numbers do not show

- Behaviour across separate machines, or over a real network.
- A multi-node Cassandra cluster, or any replication factor above 1.
- A native-hardware HDFS. The live HDFS test suite ran on a Linux CI runner without emulation and
  finished its 20 tests in about 23 seconds, but that was not measured as throughput.
- Large inventories beyond a few thousand files.

## Reproducing

```
tests/integrations/cassandra/benchIndex.py     # needs the secure or plain Cassandra service running
tests/integrations/hdfs/runBench.sh            # starts the HDFS stack, runs, writes the CSV files
```
