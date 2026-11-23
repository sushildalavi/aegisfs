# Integration testing

There are two kinds of test for the integrations. Unit tests use fakes and need nothing installed.
Live tests run against a real Cassandra, a real HDFS cluster and a real AegisFS cluster. Fakes
check the logic, but only the live tests show the integrations actually work with the real
systems, so both run in CI.

## What exists

| Suite | Where | Count | Needs |
| --- | --- | --- | --- |
| Shared layer: configuration, the `aegisfs` client wrapper, the command dispatcher | `tests/integrations/shared/` | 24 | nothing |
| HDFS connector, against an in-process fake WebHDFS server | `tests/integrations/hdfs/` (`testWebHdfs`, `testTransfer`, `testTls`) | 83 | `openssl` for the TLS tests |
| Cassandra index, against a fake session | `tests/integrations/cassandra/` | 112 | `cassandra-driver` |
| Gateway, including the optional index hook | `tests/python/testExtensions.py` | 7 | nothing |
| HDFS, live | `tests/integrations/hdfs/testLive.py` | 20 | Docker |
| HDFS, both DataNodes killed during an export | `tests/integrations/hdfs/interruptDataNode.sh` | 1 scenario | Docker |
| Cassandra, live, plaintext service | `tests/integrations/cassandra/testCassandraLive.py` | 23 run, 7 skipped | Docker |
| Cassandra, live, secure service (TLS and login) | same file | 28 run, 2 skipped | Docker |
| Compose profiles stay optional | `scripts/integrations/checkProfiles.sh` | 1 check | Docker Compose (starts nothing) |
| AegisFS itself | `tests/`, `scripts/checks/` | 113 C++ tests, smoke, reliability, sanitizers, ten-node and network-fault checks | see [operations.md](operations.md) |

The skipped Cassandra tests are the ones that only make sense for the other service: each mode skips
the other mode's tests.

## Running them

Unit tests:

```
python3 -m unittest discover -s tests/integrations/shared -p "test*.py"
(cd tests/integrations/hdfs && python3 -m unittest testWebHdfs testTransfer testTls)
(cd tests/integrations/cassandra && python -m unittest testCassandraModel testCassandraStore testCassandraSpool testCassandraSchema testCassandraIndexer)
python3 tests/python/testExtensions.py
```

The Cassandra tests need the driver (`pip install -r extensions/integrations/requirements.txt`).
A live test skips with a message when its service is not reachable, and fails instead when
`AEGISFS_INT_CASSANDRA_LIVE=1` or `AEGISFS_INT_HDFS_LIVE=1` is set, so a missing service cannot pass
by being skipped.

Live tests start what they need, run, and stop it:

```
tests/integrations/cassandra/runLive.sh              # plaintext, then secure
tests/integrations/cassandra/runLive.sh secure       # only the secure service
tests/integrations/hdfs/runLive.sh                   # HDFS, AegisFS and the DataNode-kill scenario
scripts/integrations/checkProfiles.sh
```

Docker volumes are never deleted by these scripts. `scripts/integrations/cassandraDown.sh --purge`
and `scripts/integrations/hdfsDown.sh --purge` remove them when asked.

The HDFS live tests run inside the Compose network, because WebHDFS redirects to DataNode host
names that only resolve there. The Cassandra tests run on the host against a process-local AegisFS
cluster and a published Cassandra port.

## What each live suite covers

**Cassandra.** Migrations and readiness; insert, update and query results; the same event delivered
twice; events out of order (an older version never overwrites a newer one), including concurrently;
invalid input being refused; reconcile finding files written directly with the `aegisfs` CLI;
a striped file; an append; a file removed from every node being marked stale; `--confirm` reporting a
disagreement with live AegisFS; stopping and restarting the Cassandra container with events queued
(spooled, then replayed, nothing lost); a 3,000-entry inventory; and AegisFS `create` and `get`
working while Cassandra is stopped. The secure run adds the login, certificate and no-fallback checks
listed in [integrationSecurity.md](integrationSecurity.md).

**HDFS.** Real uploads and downloads: a text file, 1 MiB of random binary, an empty file, a file with
appended content, a 5 MB striped file (the copy in HDFS is the full reassembled file and comes back
byte-identical), and a 100 MiB file in both directions. Safety: a destination that already exists is
refused and then replaced with `--overwrite`; a missing source; invalid names and paths; HDFS
refusing permission; a redirect to an unlisted host refused with nothing sent; transport errors.
Interruption: SIGTERM and SIGKILL in the middle of an upload leave no destination file, and cleanup
removes the leftover temporary file; with both DataNodes killed during a 400 MiB export, the export
fails, its temporary file is removed, the destination is never created, and a retry after recovery
works. Batch runs, including a failing item and resuming an interrupted run. AegisFS `create` and `get`
keep working while HDFS is unreachable.

Every successful transfer in these tests is checked byte for byte, not only by reported size.

## CI

`.github/workflows/integrations.yml` runs separately from the core workflow, so the storage
engine's checks never depend on the integrations.

| Job | What it does |
| --- | --- |
| `unit` | Installs the driver, then runs the shared, HDFS and Cassandra unit tests, the Compose profile check and a scan for committed private keys or credential directories. |
| `cassandra-live` | Builds `aegisfs`, then runs `runLive.sh`: a real Cassandra (plaintext and secure) and a three-node AegisFS cluster. |
| `hdfs-live` | Builds the integrations image, starts HDFS and AegisFS nodes, and runs `runLive.sh`. |

The core workflow (`ci.yml`) still runs the C++ tests, AddressSanitizer, ThreadSanitizer, the
smoke and reliability checks, and the ten-node Compose and network-fault checks, unchanged.

CI runs a newer Python (3.14) than a typical laptop. That mattered once: from Python 3.13 on, TLS
verification is strict, and a throwaway test CA without key-usage extensions failed on CI while passing
locally. The test certificates are now generated correctly.

## Known gaps

- The live suites run on one machine, not across hosts.
- The live Cassandra tests run against a single-node, replication-factor-1 cluster.
- Delegation-token authentication to HDFS is tested only against the fake server, and Kerberos is not
  supported.
- The Cassandra live tests were not run inside the integrations Docker image's Python.
- Linearizability of AegisFS is still not tested; see [invariants.md](invariants.md).
