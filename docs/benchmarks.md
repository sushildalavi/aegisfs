# Benchmarks

Everything here was measured on one machine and is only meaningful relative to other numbers taken
on the same machine. Raw samples are in `benchmarks/results/`.

**Machine:** MacBook with an Apple M3 (8 cores), 16 GB RAM, APFS SSD, macOS, Apple clang,
`RelWithDebInfo`. All nodes are processes on the same host talking over loopback, and each client
call is a separate `aegisfs` process.

## What a measurement includes

A `create` is timed from the moment the client process starts until it exits. In between, the client
uploads to a coordinator, the coordinator fans the file out to the three replicas, and each replica
fsyncs and renames the segment and commits a manifest. The call returns only after all three have
committed, with checksums verified. None of that is relaxed for the benchmarks: fsync stays on,
the replication factor stays 3, and every `create` has to come back `status=OK`.

One caveat about fsync on macOS: plain `fsync` there does not force data to the physical media (that
needs `F_FULLFSYNC`), so a 300 MiB write plus fsync takes under 100 ms on this machine. Absolute
numbers here are therefore friendlier than they would be on Linux with a real flush. The comparisons
between builds are not affected, because both builds use the same call.

## Creating a 100 MiB file: before and after

The question was whether recent changes made replicated creates faster. `benchmarks/compareBuilds.py`
runs two binaries on separate fresh 5-node clusters, one at a time, alternating which goes first over
four rounds. Each round does one warm-up create and then ten timed ones.

- **Before:** commit `399efb5`, built in a separate worktree.
- **After:** commit `3e4a697` (the changes in this document).

| 100 MiB create | Before | After | Change |
|---|---|---|---|
| Median, first 5 creates on a fresh cluster (n=20) | 312.0 ms | 198.7 ms | **−36.3%** |
| Median, all 10 creates per cluster (n=40) | 315.7 ms | 198.7 ms | **−37.1%** |
| Mean (n=40) | 337.1 ms | 207.9 ms | −38.3% |
| p95 (n=40) | 462.8 ms | 260.2 ms | −43.8% |
| p99 (n=40) | 647.9 ms | 296.7 ms | −54.2% |
| Std deviation (n=40) | 82.8 ms | 25.3 ms | −69.4% |
| Throughput at the median | 332 MB/s | 528 MB/s | +59% |

I was aiming for more than a 40% cut. The median fell by 36–37%, so that target was **not met**.
The mean, p95 and p99 improved by more than 40%, but I would not call that hitting the goal,
because the median is the number I would quote.

Two earlier numbers are worth explaining, because they pointed me in the wrong direction at first.

The old version of this page recorded 540.8 ms for a parallel 100 MiB create. I could not reproduce
that: the same commit now measures about 310 ms in this harness. The earlier run was most likely on a
busier machine, which is why the comparison above builds both sides in the same session and
interleaves them.

A first version of the comparison started both clusters at once and alternated samples between them.
That produced medians of about 2–4 s for both builds. The cause was the baseline's own background work:
every node was re-hashing every stored segment once per second, so an idle cluster full of 100 MiB
files kept the CPU busy and slowed whichever cluster was being timed. That is the scrub problem
described below, and it is also why the harness now runs one cluster at a time.

### What changed

1. **Replicas rename the spooled upload into place** instead of copying it through a second temp
   file. The hash had already been checked, so the second copy only added disk writes. Commit
   `1e194c9`.
2. **The background integrity check no longer re-hashes everything every second.** Each cycle now
   stats the segment files, and a full re-hash runs once per file every five minutes
   (`--scrub-interval-ms`). With a few 100 MiB files stored, the old behaviour was hundreds of MB of
   SHA-256 per node per second and pushed create latency from about 250 ms to several seconds in a
   back-to-back run. Commit `f511f78`.

Two other commits in the same range changed correctness rather than speed: a create or append on a
ring with fewer than three members now returns `DEGRADED` instead of `OK`, and a failed `fsync` on
an upload is now an error. Neither affects the timing above in any measurable way.

### Where the rest of the time goes

A 100 MiB create still takes about 200 ms because the data is processed several times in sequence:
read and checksummed on the client, spooled and hashed on the coordinator, then spooled and hashed
again on each replica. A segment is completely spooled before it is forwarded. Forwarding chunks to
the replicas while they are still arriving would overlap those passes, and is the obvious next
change. I have not attempted it because it alters the write protocol and needs its own failure
testing.

## Size and fan-out matrix

`benchmarks/runMatrix.sh`. Five repetitions per point on a fresh 5-node cluster for each fan-out
mode. With five samples, p95 and p99 are just the slowest runs, so read them as a rough ceiling.

| Operation | Size | Sequential mean | Parallel mean | Parallel p95 | Parallel MB/s |
|---|---|---|---|---|---|
| create | 1 KiB | 51.4 ms | 45.0 ms | 54 ms | 0.02 |
| create | 1 MiB | 49.6 ms | 47.6 ms | 49 ms | 21.0 |
| create | 10 MiB | 76.2 ms | 70.0 ms | 71 ms | 142.9 |
| create | 100 MiB | 276.4 ms | 218.6 ms | 225 ms | 457.6 |
| get | 1 MiB | 31.2 ms | 30.4 ms | 33 ms | 33.0 |
| get | 10 MiB | 45.2 ms | 43.4 ms | 45 ms | 230.5 |
| get | 100 MiB | 186.0 ms | 176.6 ms | 190 ms | 567.5 |

Parallel fan-out only matters once a transfer is large: at 100 MiB it is 21% faster than writing the
replicas one after another. At 1 KiB and 1 MiB the two are within the noise of five samples, and the
1 KiB figure is dominated by process start-up and connection setup, not by data. `get` reads from one
replica, so fan-out mode does not apply to it; the small differences between the two columns are
noise.

## Concurrency

`benchmarks/runConcurrency.sh` starts N clients at once, each doing 1 create and 19 appends to its
own file on a 5-node cluster.

| Clients | Operations | Wall time | Operations/s |
|---|---|---|---|
| 1 | 20 | 575 ms | 34.8 |
| 5 | 100 | 1404 ms | 71.2 |
| 10 | 200 | 2810 ms | 71.2 |
| 20 | 400 | 5637 ms | 71.0 |

Throughput doubles from one client to five and then stays flat. All five nodes, the clients and the
load generator share one machine, so it is the host that saturates here, not necessarily the
design. Every operation also starts a new client process, so these figures include process start-up
and a TCP connect and are a floor for what the nodes can do. `Stress.MixedWorkloadThousandsOfOps`
drives a similar mix inside one process without that overhead.

## In containers, with TLS on

One more data point, taken once and not part of the comparison above: ten Docker Compose nodes (each
with its own network address, TLS and the cluster token on, which the numbers above leave off), and
ten 100 MiB creates timed from inside the `node0` container.

| Samples | Min | Median | Max |
|---|---|---|---|
| 10 | 402 ms | 487 ms | 759 ms |

This is still one physical machine, with Docker's virtual machine in between, ten nodes instead of
five, and encryption on every hop, so it is not comparable with the loopback figures and says little
about separate hosts. I have not run AegisFS across real machines.

## Limits of these results

- One machine, loopback only. No latency between nodes, so the benefit of overlapping replica
  transfers on a real network is understated. `benchmarks/runRemote.sh` can time a cluster that is
  already running elsewhere, but I have not had separate machines to run it on.
- Replicas share a host's CPU, memory and disk, so absolute MB/s says little about a real
  deployment.
- The macOS `fsync` behaviour described above.
- Small sample counts in the matrix (five), larger in the comparison (forty per build).
- Timings are wall-clock milliseconds, so anything under a few milliseconds is near the resolution.

## Reproducing

```
cmake --build build
benchmarks/runMatrix.sh                 # size and fan-out matrix -> benchmarks/results/
benchmarks/runConcurrency.sh            # concurrency scaling
python3 benchmarks/compareBuilds.py --a /path/to/old/aegisfs --b build/aegisfs
```

To compare against an earlier commit, build it in a separate worktree
(`git worktree add ../aegisfs-old 399efb5`) and pass its `build/aegisfs` as `--a`.
