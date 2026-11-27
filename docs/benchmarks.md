# Benchmarks

These are historical results recorded on one laptop, with the raw data committed under
`benchmarks/`. They were not re-measured for the current commit. Compare numbers only within one
table.

**Machine:** Apple M3 (8 cores), 16 GB RAM, macOS, Apple clang, `RelWithDebInfo`. All nodes and
clients ran on that host over loopback, and each client operation was a separate `aegisfs` process.

A `create` is timed from client start to exit: upload to a coordinator, fan-out to three replicas,
fsync, rename and manifest commit on each, with checksums verified and `status=OK` required. Plain
`fsync` on macOS does not force data to the physical media (that needs `F_FULLFSYNC`), so absolute
write times are friendlier than they would be with a real flush.

## 100 MiB create, before and after the write-path changes

`benchmarks/compareBuilds.py` runs two binaries on separate, fresh five-node clusters, one cluster
at a time, alternating which goes first. This run used four rounds of one warm-up and ten timed
creates per build, with TLS off on both. "Before" is commit `5f95de8`. "After" included the four
changes below. Raw samples are in `benchmarks/results/createComparison.csv` and the harness output
in `createComparison.txt`.

| 100 MiB create | Before | After | Change |
|---|---|---|---|
| Median, first 5 creates per cluster (n=20) | 461.9 ms | 227.1 ms | -50.8% |
| Median, all creates (n=40) | 496.4 ms | 254.3 ms | -48.8% |
| Mean (n=40) | 609.4 ms | 250.6 ms | -58.9% |
| p95 (n=40) | 1253.7 ms | 294.4 ms | -76.5% |
| p99 (n=40) | 1584.9 ms | 364.7 ms | -77.0% |
| Standard deviation (n=40) | 287.0 ms | 36.5 ms | -87.3% |
| Throughput at the median | 211 MB/s | 412 MB/s | +95% |

The machine was busy during this run, and the older build suffers more under load, which probably
inflates the percentages. An earlier run on a quieter day, with only the first two changes, measured
medians of 315.7 ms and 198.6 ms (-37%, n=40). Its samples are the previous revision of the same CSV
in the Git history.

The changes:

1. Replicas rename the spooled upload into place instead of copying it through a second temporary
   file.
2. The background integrity check no longer re-hashes every stored segment every second.
3. A coordinator that is also a replica hard-links its spooled file instead of re-reading, copying
   and re-hashing it.
4. Receiving, writing and hashing an upload overlap.

A first version of the harness ran both clusters at once and measured about 2 to 4 s for both
builds, because the older build's idle cluster kept re-hashing stored data and slowed whichever
cluster was being timed. That is why the harness runs one at a time.

## Size and fan-out matrix

`benchmarks/runMatrix.sh`, recorded at commit `6749499`, five repetitions per point on a fresh
five-node cluster per mode. This predates changes 3 and 4 above, striping, and TLS being on by
default. With five samples, p95 is simply the slowest run. MB/s is computed with 1 MB = 2^20 bytes.

| Operation | Size | Sequential mean | Parallel mean | Parallel p95 | Parallel MB/s |
|---|---|---|---|---|---|
| create | 1 KiB | 51.4 ms | 45.0 ms | 54 ms | 0.02 |
| create | 1 MiB | 49.6 ms | 47.6 ms | 49 ms | 21.0 |
| create | 10 MiB | 76.2 ms | 70.0 ms | 71 ms | 142.9 |
| create | 100 MiB | 276.4 ms | 218.6 ms | 225 ms | 457.6 |
| get | 1 MiB | 31.2 ms | 30.4 ms | 33 ms | 33.0 |
| get | 10 MiB | 45.2 ms | 43.4 ms | 45 ms | 230.5 |
| get | 100 MiB | 186.0 ms | 176.6 ms | 190 ms | 567.5 |

Parallel fan-out only matters for large transfers: at 100 MiB it was 21% faster than writing the
replicas in turn. Small operations are dominated by process start-up and connection setup. `get`
reads one replica, so the differences between its two columns are noise.

## Concurrency

`benchmarks/runConcurrency.sh`, recorded at the same commit as the matrix. Each client does one
create and 19 appends of 4 KiB to its own file on a five-node cluster.

| Clients | Operations | Wall time | Operations/s |
|---|---|---|---|
| 1 | 20 | 575 ms | 34.8 |
| 5 | 100 | 1404 ms | 71.2 |
| 10 | 200 | 2810 ms | 71.2 |
| 20 | 400 | 5637 ms | 71.0 |

Throughput stops rising at five clients. Nodes, clients and the load generator share one host, and
every operation starts a new process, so this does not show what the nodes alone can sustain.

## Optional integrations

These describe the Cassandra index and the HDFS connector, not the storage engine. Raw data and an
`environment.json` for each are in `benchmarks/integrations/`. Other work was running on the laptop,
and the spread between runs shows it.

### Cassandra index

One `cassandra:5.0.5` node with replication factor 1 in Docker over loopback, `cassandra-driver`
3.29.3 on Python 3.9, 16 buckets. Five runs, each in a new keyspace.

| Measurement | Samples | Median | p95 | p99 |
|---|---|---|---|---|
| Create an entry | 1,000 | 3.9 ms | 16.1 ms | 30.1 ms |
| Update an entry | 1,000 | 4.5 ms | 15.9 ms | 31.1 ms |
| Look up one file by name | 1,000 | 1.1 ms | 6.4 ms | 11.1 ms |
| List 100 files by prefix | 200 | 17.6 ms | 50.1 ms | 96.3 ms |

| Concurrent lookup threads | Median ops/s | Min | Max |
|---|---|---|---|
| 1 | 507 | 475 | 971 |
| 4 | 1,577 | 1,143 | 2,387 |
| 16 | 2,232 | 2,146 | 3,619 |

The threads share one Python process and driver session, so the interpreter limits them too.

| Reconcile of 3,000 files | Median | Min | Max |
|---|---|---|---|
| Empty index | 4.5 s | 2.4 s | 8.8 s |
| Nothing changed | 1.7 s | 0.6 s | 1.9 s |
| One file changed | 1.1 s | 0.7 s | 5.6 s |

The reconcile figures use an in-memory stand-in for the aegisfs inventory, so they measure the index
side only.

### HDFS transfers

`apache/hadoop:3.4.1` with one NameNode and two DataNodes (replication 2), run under amd64 emulation
on Apple silicon, and three aegisfs nodes with TLS, all in Docker on the same laptop. Each transfer
is a separate `aegisfs-integrations` process, including the read-back and rename. The 1 MiB and
10 MiB rows have 5 samples, the 100 MiB rows 3, and the small-file rows 5 runs of 30 files.

| Operation | Median time | Median MB/s | Min to max |
|---|---|---|---|
| Export 100 MiB | 3.07 s | 34 | 2.63 to 3.39 s |
| Import 100 MiB | 1.88 s | 56 | 1.87 to 2.34 s |
| Export 10 MiB | 0.48 s | 22 | 0.43 to 0.74 s |
| Import 10 MiB | 0.52 s | 20 | 0.41 to 0.74 s |
| Export 1 MiB | 0.35 s | 3.0 | 0.29 to 0.75 s |
| Import 1 MiB | 0.33 s | 3.2 | 0.32 to 0.45 s |
| Export 30 x 1 KiB, one process per file | 15.4 s | | 8.5 to 33.3 s |
| Import 30 x 1 KiB, one process per file | 14.6 s | | 8.9 to 32.7 s |
| Export 30 x 1 KiB, one `export-all` | 12.7 s | | 4.2 to 14.6 s |
| Import 30 x 1 KiB, one `import-all` | 7.5 s | | 4.2 to 13.9 s |

The peak resident memory of the Python process was about 25 MiB at every size, which is consistent
with streaming in fixed chunks. It excludes the `aegisfs` client process. An export stages one local
copy of the file and an import two.

For the same files, native `aegisfs create` and `get` took 0.41 s and 0.62 s at 100 MiB, 83 ms and
85 ms at 10 MiB, and 40 ms and 42 ms at 1 MiB. The comparison is not like for like: an export reads
the file from aegisfs, uploads it, reads it back from HDFS to hash it and renames it, and an import
mirrors that.

## Limits of these results

- One machine, loopback only, with replicas sharing CPU, memory and disk. Nothing here was run
  across separate hosts.
- Small sample counts, especially in the matrix.
- The macOS `fsync` behaviour described above.

## Reproducing

```
cmake --build build
benchmarks/runMatrix.sh          # overwrites benchmarks/results/raw.csv and summary.csv
benchmarks/runConcurrency.sh     # overwrites benchmarks/results/concurrency.csv

git worktree add ../aegisfs-before 5f95de8
cmake -S ../aegisfs-before -B ../aegisfs-before/build -G Ninja -DAEGISFS_BUILD_TESTS=OFF
cmake --build ../aegisfs-before/build
python3 benchmarks/compareBuilds.py --a ../aegisfs-before/build/aegisfs --b build/aegisfs \
  --rounds 4 --out comparison.csv

tests/integrations/cassandra/benchIndex.py    # run from a venv with cassandra-driver, Cassandra up
tests/integrations/hdfs/runBench.sh           # needs the stack from hdfsUp.sh
```

`benchmarks/runRemote.sh <host:port>` times a cluster that is already running elsewhere. No results
from it are recorded.
