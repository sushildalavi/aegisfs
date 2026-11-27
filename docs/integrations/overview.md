# Optional integrations

Two tools connect aegisfs to systems people already run: a Cassandra index of file metadata and a
connector that copies files to and from HDFS. Neither is part of the storage engine. No C++ code
calls either one, and the cluster behaves the same without them. The only C++ support they needed is
the read-only `members` command.

- [Cassandra metadata index](cassandra.md)
- [HDFS import and export](hdfs.md)

## How they fit

Both are one Python package, `extensions/integrations/`, run through the `aegisfs-integrations`
launcher. They reach the cluster only by running the `aegisfs` client, so TLS, the cluster token,
retries and striping behave as they do for any other client. They never read segments or manifests.

```
        +-------------------------+
        |   aegisfs storage nodes |   source of truth
        +------------+------------+
                     |  aegisfs client
          +----------+-----------+
          |                      |
   Cassandra indexer       HDFS connector
          |                      |
      Cassandra            WebHDFS (HDFS)
```

**Cassandra** records what aegisfs reported about a file, with the time it was observed. It is not
authoritative. The REST gateway can queue "this file changed" events in a local spool, and
`index --reconcile` scans the nodes, which also finds files written directly with the CLI. If
Cassandra is down, work waits in the spool.

**HDFS** is an external system you copy files to or from, not a storage tier or a replica. Both
directions read the copy back and compare SHA-256 before reporting success. Transfers never delete
the source or overwrite a destination unless `--overwrite` is given.

## Security rules shared by both

- Each integration refuses an unprotected connection unless a development switch is set by name.
  Nothing falls back to plaintext.
- Passwords and tokens are read from files named in the configuration, never from flags or inline
  JSON. Error messages are redacted.
- The Cassandra spool files are created with mode 0600 and the HDFS journal directory with 0700.
  They hold file names and hashes, not file contents or credentials.

The connection options are described in [cassandra.md](cassandra.md#configuration) and
[hdfs.md](hdfs.md#security-options). Cluster security is covered in
[SECURITY.md](../../SECURITY.md).

## Independence, and how it is checked

| Claim | Check |
|---|---|
| A plain `docker compose up` starts only the core services | `scripts/integrations/checkProfiles.sh` |
| aegisfs reads and writes keep working while Cassandra is stopped | `testCassandraLive.py` |
| aegisfs reads and writes keep working while HDFS is unreachable | `tests/integrations/hdfs/testLive.py` |
| A missing driver or service disables only its own area, which exits with code 3 | `testCli.py` and the area unit tests |

## Tests

| Suite | Location | Needs |
|---|---|---|
| Shared configuration, client wrapper and command dispatch | `tests/integrations/shared/` | nothing |
| HDFS connector against an in-process fake WebHDFS server | `testWebHdfs`, `testTransfer`, `testTls` | `openssl` for the TLS tests |
| Cassandra index against an in-memory back end | `testCassandra{Model,Store,Spool,Schema,Indexer}` | `cassandra-driver` |
| Gateway, including the index hook | `tests/extensions/testExtensions.py` | nothing |
| Cassandra live, plaintext and secure | `tests/integrations/cassandra/runLive.sh` | Docker |
| HDFS live, including both DataNodes killed during an export | `tests/integrations/hdfs/runLive.sh` | Docker |

```
python3 -m unittest discover -s tests/integrations/shared -p "test*.py"
(cd tests/integrations/hdfs && python3 -m unittest testWebHdfs testTransfer testTls)
python3 -m venv .venv && .venv/bin/pip install -r extensions/integrations/requirements.txt
(cd tests/integrations/cassandra && ../../../.venv/bin/python -m unittest testCassandraModel \
  testCassandraStore testCassandraSpool testCassandraSchema testCassandraIndexer)
```

A live test skips when its service is unreachable, and fails instead when
`AEGISFS_INT_CASSANDRA_LIVE=1` or `AEGISFS_INT_HDFS_LIVE=1` is set, which the `runLive.sh` scripts
do. The scripts never delete Docker volumes. `cassandraDown.sh --purge` and `hdfsDown.sh --purge`
do. The HDFS live tests run inside the Compose network because WebHDFS redirects to DataNode host
names that resolve only there.

`.github/workflows/integrations.yml` runs separately from the core workflow, with jobs for the unit
tests (plus the profile check and a scan for committed private keys), live Cassandra and live HDFS.

## Not covered

- Separate physical machines. All measurements are from one laptop; see
  [benchmarks](../benchmarks.md#optional-integrations).
- A Cassandra cluster of more than one node, mutual TLS to Cassandra, or restricted Cassandra roles.
- Kerberos, which is not supported, and HDFS delegation tokens or HTTPS against a real cluster.
- HDFS federation, high-availability failover, encryption zones and erasure coding.
