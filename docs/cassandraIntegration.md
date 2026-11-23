# Cassandra metadata index

An optional index of what AegisFS reports about its files, kept in Apache Cassandra. It lets you
look a file up by name, list a name prefix, find striped files, and see which entries look stale,
without asking every storage node. AegisFS does not need it: the storage nodes, the client, the
gateway and the demo never read or write Cassandra, and everything here is in a separate package
(`extensions/integrations/aegisfsIntegrations/cassandra`).

**The index is an observation, not the authority.** Each entry says "at `observed_at`, AegisFS
reported this". AegisFS decides what a file contains and whether it is healthy. Every query result
is labelled that way, and if AegisFS reports a failure (a checksum mismatch, a missing file) the
index never overrides it.

## What it stores

Only what AegisFS can actually tell the indexer. Anything it cannot tell is `null`, not a guess.

| Field | Source | Notes |
|---|---|---|
| `name`, `file_id`, `version` | `listStore` | `version` is AegisFS's own file version. |
| `stored_size` | `listStore` | Size of the stored object. For a striped file this is the small index object, not the file. |
| `size`, `size_basis` | see below | Logical size, only when known. |
| `striped`, `striped_basis` | chunk files seen, or a read | `null` when neither is known. |
| `chunk_files` | `listStore` | Number of `<name>.stripe.*` files seen. Informational. |
| `sha256`, `verified_at`, `verified_size` | a full read that was hashed | `null` unless `--verify-checksums` actually read the file. Cleared when a newer version is observed. |
| `verify_state` | indexer | `verified`, `unverified`, `skipped_too_large`, `failed`. |
| `replicas`, `replicas_source`, `replicas_observed_at` | `listStore` or `ls` | Map of node address to "holds the file". `listStore` only knows nodes that listed the file; `ls` also reports designated replicas that do not hold it. |
| `status`, `status_reason` | indexer | `indexed`, `incomplete`, `stale`. |
| `observed_at`, `source` | indexer | When AegisFS was looked at, and by what (`reconcile`, `index --name`, `hook:create`, ...). |

AegisFS does not report a checksum, so the index has none until a file is read. A file that is
deleted does not exist as a concept in AegisFS (there is no delete operation), so there is no
"deleted" state: `stale` means "not listed by any node in the latest complete scan".

### Size and striped files

Files above `--stripe-mib` (default 256 MiB) are split by the client into chunk files named
`<name>.stripe.<client>.<time>.<n>`, and the real name holds a small index. `listStore` reports the
index's size under the real name, and the chunks as separate files. The indexer therefore:

- never creates entries for chunk files; it counts them (`chunk_files`) on the file they belong to
  and counts chunks with no matching file as `orphan_chunk_files` in the reconcile report;
- marks a file `striped=true` (`striped_basis=inferred_from_chunks`) when its chunks are listed;
- leaves `size` null for a striped file until the file is read (`--verify-checksums`), then sets
  `size` to the number of bytes read and `size_basis=read`;
- for a file with no chunks listed, and when every node answered the scan, uses the listed size as
  `size` with `size_basis=listStore_no_chunks`. That is a listing, not a read. If a striped file's
  chunks were all missing it would look the same, so treat `listStore_no_chunks` as "probably".
  `striped` stays `null` in that case until a read sets `striped=false` (`verified_get`).

## Schema

Cassandra is queried by partition key, so there is one table per question. No query uses
`ALLOW FILTERING` and no secondary indexes are created (a unit test checks both). The migrations
are in `extensions/integrations/cassandra/migrations/`.

| Table | Primary key | Answers |
|---|---|---|
| `files_by_name` | `(name)` | exact lookup; the full record. The only table written with lightweight transactions. |
| `files_by_namespace` | `((namespace, bucket), name)` | listing a namespace and a name prefix. Summary columns only. |
| `namespace_children` | `((parent), child)` | which sub-namespaces exist, so a prefix listing can descend without scanning. |
| `files_by_attr` | `((attr, bucket), name)` | `striped=true`, `status=stale`, `status=incomplete`. |
| `index_meta` | `(key)` | `bucket_count`, fixed when the index is first used. |
| `schema_migrations` | `(version)` | applied migrations with their file checksum. |

The namespace of `a/b/c.txt` is `a/b` (everything before the last `/`); a name with no `/` is in
the root namespace, written `/` because Cassandra does not allow an empty partition key. A name's
bucket is `crc32(name) % bucket_count` (default 16, at most 256). Buckets keep any one partition to
a bounded size; a directory with a million files puts about 62,000 in each bucket partition. A
prefix is a clustering-key range inside each bucket, so it is not a scan. The cost is that
listing a namespace reads every bucket (16 small queries by default).

`bucket_count` cannot change once data exists. The value in `index_meta` wins over configuration
(`cassandra migrate` prints a note if they differ).

### Migrations

`cassandra migrate` creates the keyspace if `create_keyspace` is true (default), creates
`schema_migrations`, then applies files `NNN_name.cql` in order and records each one after all of
its statements ran. Running it again does nothing. A migration file that was edited after being
applied, a gap in the numbering, or a database that has migrations the files do not include, are
errors. Add a new migration instead of editing an old one. Statements must be safe to run twice
(`IF NOT EXISTS`), because a crash between a statement and its record means it can run again.

## Ordering and idempotency

An observation replaces the stored row only if it is not older than it, where "older" means a lower
`version`, or the same `version` with an earlier `observed_at`. The AegisFS version always wins:
an observation of version 4 that arrives after version 5 is rejected even if its timestamp is later.
This is implemented as read, merge in Python, then a lightweight transaction:
`UPDATE ... IF version = <read> AND observed_at = <read>` (or `INSERT ... IF NOT EXISTS`). If another
writer got in first, the row is read again and merged again, up to 8 times. CQL cannot compare a
pair of columns in one condition, which is why the comparison is done on the read value and the
transaction only guards "nobody changed it since". One consequence: a write costs a read plus a
transaction (about 4 ms median on the benchmark machine below).

- A repeated event gives the same row, reported as `unchanged`.
- Arrival order does not matter: a unit test applies every permutation of a set of events and
  checks they end on the same row.
- A newer version drops the old checksum, size and striped information, because they described the
  old bytes. A same-version observation keeps whatever it does not know (it will not erase a
  checksum, and a guess from a listing does not replace a value learned by a read).
- A "not listed" observation marks the existing row `stale` (or `incomplete`, see below) and keeps
  its version. It never creates a row for a file that was never indexed.
- `observed_at` is the indexer host's clock, in milliseconds. An observation more than 5 minutes in
  the future is rejected so one wrong clock cannot freeze an entry. Clock differences smaller than
  that between two indexer hosts can reorder two observations of the same version.

After the main row is written, the namespace summary row and attribute rows are written with the
observation time as the write timestamp. These tables are written a moment after `files_by_name`
and are not part of the transaction, so a crash between the two can leave them behind. Queries by
attribute re-read each hit from `files_by_name` and drop it if it no longer has the attribute
(`dropped_inconsistent` in the result). Replaying the same observation (which the spool does)
rewrites the derived rows and repairs the gap. A reconcile skips entries whose summary already
matches, so it does not repair them.

## Keeping the index in step

### `cassandra index --reconcile`

Runs `aegisfs members` and `listStore` on every alive node, and compares the result with the index:

- files AegisFS lists that the index lacks are created; files whose version, stored size, size,
  striped flag, status or replica count differ are updated; matching files are left alone;
- files in the index that AegisFS no longer lists are marked `stale`, never dropped;
- if some nodes did not answer, a missing file cannot be told from one on the silent nodes, so it
  is marked `incomplete` ("not listed, but some nodes did not answer the scan") instead of `stale`,
  and files under their expected replica count are `incomplete`;
- a file seen on fewer than `expected_replicas` (default 3) nodes is `incomplete`;
- it first replays the retry queue.

With `--verify-checksums [--max-bytes N]` it also reads, through `aegisfs get`, each file that is
new, has a new version, or has no checksum for its current version, hashes the result itself,
checks the hash against the one the client printed, and records `sha256`, `verified_at` and
`verified_size`. If the version changed while the file was being read, the checksum is discarded
and `verify_state=failed`. Files larger than `--max-bytes` are skipped
(`verify_state=skipped_too_large`); the size compared is what AegisFS lists (the sum of chunk sizes
for a striped file). The exit code is 1 if any observation was invalid or any read failed.

The report counts: `scanned_files`, `created`, `updated`, `unchanged`, `marked_stale`,
`marked_incomplete_scan`, `already_missing`, `incomplete_files`, `skipped_newer`, `spooled`,
`invalid`, `verified`, `verify_skipped`, `verify_failed`, `chunk_files_seen`, `orphan_chunk_files`,
`unreachable_nodes`, `seconds`.

### `cassandra index --name X`

Indexes one file using `aegisfs ls` (designated replicas and whether each holds the file) and
`listStore` on the replicas that hold it. It does not set `size` unless `--verify-checksums` reads
the file, because a single-node listing cannot rule out stripe chunks held elsewhere. If Cassandra
is down the observation is queued and the command still exits 0 with `outcome=spooled`.

### Retry queue

If Cassandra cannot be reached, the work is appended to a spool directory (`spool_dir`) and the
AegisFS operation that caused it is unaffected, because the indexer only reads from AegisFS and
nothing in AegisFS calls it.

- `pending.jsonl`: one JSON event per line, written with one append and `fsync` under a file lock.
  A crash can leave one torn last line, which is moved to `rejected.jsonl` on the next drain.
- Events are either a full `observation`, or a `touch` ("this file was changed"), which the drain
  resolves by looking the file up in AegisFS and recording what it sees at that time.
- `cassandra index --drain` replays the queue. If Cassandra is unavailable it stops and keeps
  everything not yet applied. An event that can never succeed (invalid observation, or a touch
  for a file AegisFS did not list in 5 attempts) goes to `rejected.jsonl` with the reason. A batch
  left by a drain that was killed is picked up by the next drain. Replays are idempotent.
- `cassandra index --watch SECONDS [--reconcile-every SECONDS]` loops: replay the queue, and
  reconcile on a timer. This is what the `cassandra-indexer` Compose service runs.
- `cassandra status` shows queue depth and the rejected count.

The spool lives on one host's disk. If that disk is lost the queued events are lost, and the next
reconcile finds the files again.

### The `observe` hook

```python
from aegisfsIntegrations.cassandra import observe
observe("create", "reports/2026/q1.csv", spool_dir="/var/lib/aegisfs-index/spool")
```

For code that calls AegisFS (for example the gateway). It only appends a `touch` event to the local
spool and returns `True` (queued) or `False`. It does not use the network, does not wait for
Cassandra or AegisFS, and never raises. Nothing in this repository calls it yet; wiring it into the
gateway is a separate change. Metadata always comes from AegisFS at drain time, never from the
caller, so the hook cannot put wrong values in the index.

## Queries

All take `--contact-points`, `--port`, `--keyspace` and `--spool-dir` as overrides, and the global
`--json`.

| Command | What it does |
|---|---|
| `cassandra status` | connectivity, schema version, bucket count, counts of stale/incomplete/striped entries, queue depth, effective settings (no password). `--full-count` also counts every entry by walking all namespaces. Exit 3 if Cassandra is unreachable. |
| `cassandra find --name X` | exact lookup |
| `cassandra find --striped` | entries known to be striped |
| `cassandra list --prefix P [--limit N] [--no-recursive]` | names starting with `P`, grouped by namespace; `truncated` says whether the limit cut it short |
| `cassandra stale` | entries marked `stale` or `incomplete`, with the reason |
| `cassandra show --name X` | everything stored: size and its basis, checksum and when it was verified, replicas, source |

Every result carries `index: {authoritative: false, observed_at, note}`.

`--confirm` (on `find --name` and `show`) asks AegisFS now and compares version, file id, stored size
and the set of replicas holding the file, printing every difference. With `--read` it also reads the
file and compares the hash with the recorded checksum, when the version is unchanged. The exit code
is 1 on any disagreement. If AegisFS reports an error (cannot be reached, cannot read, checksum
mismatch), that is returned as `aegisfs_error` and the text says the entry does not override it.

## Configuration

Precedence: command-line flag, then environment variable `AEGISFS_INT_CASSANDRA_<KEY>`, then the
JSON file (`--config` or `AEGISFS_INTEGRATIONS_CONFIG`, section `cassandra`), then the default.

| Key | Default | Meaning |
|---|---|---|
| `contact_points` | `127.0.0.1` | comma separated hosts |
| `port` | `9042` | native port |
| `local_dc` | none | local datacenter for the load-balancing policy |
| `keyspace` | `aegisfs_index` | lowercase letters, digits, underscores |
| `username`, `password_file` | none | both or neither. The password is read only from the file. |
| `tls` | `true` | TLS to Cassandra |
| `tls_ca_file` | system store | CA certificate to trust |
| `tls_verify` | `true` | verify the certificate and hostname |
| `allow_insecure` | `false` | must be `true` to turn `tls` or `tls_verify` off. Development only. |
| `read_consistency`, `write_consistency` | `LOCAL_QUORUM` | |
| `serial_consistency` | `LOCAL_SERIAL` | for the lightweight transactions |
| `create_keyspace` | `true` | whether `migrate` creates the keyspace |
| `replication_strategy` | `SimpleStrategy` | or `NetworkTopologyStrategy` |
| `replication_factor` | `1` | for `SimpleStrategy` |
| `replication_dc_factors` | none | for `NetworkTopologyStrategy`, e.g. `dc1:3,dc2:3` |
| `bucket_count` | `16` | partition buckets; fixed on first use |
| `expected_replicas` | `3` | fewer replicas listed means `incomplete` |
| `spool_dir` | `.aegisfs-index-spool` | retry queue directory |
| `workers` | `8` | parallel writes during reconcile |
| `connect_timeout`, `request_timeout` | `5`, `10` seconds | |
| `migrations_dir` | bundled | |

The defaults are safe for a real cluster except `replication_factor=1`, which exists so the
single-node development container works. For a real cluster use at least `SimpleStrategy` with
factor 3 or `NetworkTopologyStrategy` with per-datacenter factors, and set TLS and credentials.
The AegisFS side is configured by the shared `aegisfs` settings (`AEGISFS_INT_AEGISFS_ENTRY`,
`AEGISFS_INT_AEGISFS_BIN`, and the `AEGISFS_TLS_*` / `AEGISFS_TOKEN_FILE` variables the `aegisfs`
client reads).

## Failure behaviour

| Situation | Result |
|---|---|
| Cassandra down, `index --name` | queued, exit 0, `outcome=spooled` |
| Cassandra down, `index --reconcile` / `--drain` / queries | exit 3 `unavailable`; the queue is untouched |
| Cassandra fails part way through a reconcile | the remaining work is queued (`spooled` in the report) |
| AegisFS unreachable during a reconcile | the command fails before writing anything; no entry is marked stale |
| some AegisFS nodes unreachable | missing files become `incomplete`, not `stale` |
| wrong password, or a TLS certificate that does not verify | exit 3 with a message that names the user or the certificate problem and never the password. Nothing is queued: a bad setting is not an outage. The client never falls back to plaintext. |
| invalid observation | rejected (never retried), counted as `invalid` |
| keyspace missing or schema older than the code | clear error asking for `cassandra migrate` |
| `cassandra-driver` not installed | exit 3 with the install command |

## Running it

```
# Cassandra (Apache Cassandra 5.0.5, one node, 1.5 GB limit, data in a named volume)
scripts/integrations/cassandraUp.sh              # starts Cassandra and applies the schema
scripts/integrations/cassandraUp.sh --indexer    # also starts the indexer service
scripts/integrations/cassandraDown.sh            # stop; data volume kept
scripts/integrations/cassandraDown.sh --purge    # stop and delete the data and the spool volume

# From a shell (venv with cassandra-driver)
python3 -m venv extensions/integrations/.venv-cass
extensions/integrations/.venv-cass/bin/pip install -r extensions/integrations/requirements.txt
export AEGISFS_INT_CASSANDRA_TLS=false AEGISFS_INT_CASSANDRA_ALLOW_INSECURE=true   # dev container
export AEGISFS_INT_AEGISFS_ENTRY=127.0.0.1:9100
I="extensions/integrations/.venv-cass/bin/python extensions/integrations/aegisfs-integrations"
$I cassandra migrate
$I cassandra index --reconcile --verify-checksums --max-bytes 104857600
$I cassandra list --prefix reports/
$I cassandra show --name reports/q1.csv --confirm
```

The Compose file `deploy/docker/compose.cassandra.yaml` has `cassandra` (published only on
`127.0.0.1:9042`), `cassandra-init` (runs `cassandra migrate` once Cassandra is healthy) and
`cassandra-indexer` (`index --watch 5 --reconcile-every 300`, spool on a volume). All three are in
the `cassandra` profile, so `docker compose -f deploy/docker/compose.yaml up` is unchanged. Use it
merged with the core file, which supplies the `aegis` network:

```
docker compose -p aegis-cass -f deploy/docker/compose.yaml -f deploy/docker/compose.cassandra.yaml \
  --profile cassandra up -d cassandra cassandra-init
```

The indexer service reaches AegisFS at `node0:9100` with the TLS CA and token from
`${AEGISFS_CREDS_DIR:-.dev}`; it needs the core AegisFS stack running to do anything useful, and
keeps retrying until it is. The plain `cassandra` service has no TLS and no login; see Limitations. The `cassandra-secure` service (Tests, above) is for testing TLS and login.

## Tests

```
tests/integrations/cassandra/runLive.sh            # default phase, then secure phase; stops what it started
tests/integrations/cassandra/runLive.sh default    # plaintext development container only
tests/integrations/cassandra/runLive.sh secure     # TLS + password container only
```

Unit tests (`testCassandraModel/Store/Spool/Schema/Indexer.py`) use an in-memory back end and a fake
AegisFS and need nothing running. `testCassandraLive.py` uses the real Cassandra container and a real
process-local AegisFS cluster; it skips with a message if they are not reachable, and fails instead
if `AEGISFS_INT_CASSANDRA_LIVE=1`. It covers migrations, round trips, repeated and out-of-order events
(including concurrent ones), invalid input, reconcile of files created with the `aegisfs` CLI
(including a striped file and checksum verification against a locally computed SHA-256), appends,
files removed from every node's disk, `--confirm` disagreement, stopping and restarting the Cassandra
container with work queued, AegisFS `create`/`get` while Cassandra is stopped, and a 3,000-entry
inventory.

### Secure variant (TLS and password login)

`scripts/integrations/cassandraUp.sh --secure` starts the `cassandra-secure` Compose service: the same
Cassandra 5.0.5 image with `PasswordAuthenticator` and client TLS that is required (plaintext
connections are refused; authorization stays off). `deploy/docker/cassandra/secureCreds.sh` generates
a throwaway CA, a server certificate for `127.0.0.1`/`localhost`, an unrelated second CA, and random
passwords into `deploy/docker/cassandra/.dev/`, which Git ignores. `secureSetup.py` then creates the
login `aegis_index` and replaces the default superuser's well-known password with a random one. It
publishes on the same host port as the plain container, so run one at a time.

In secure mode the whole live suite runs over TLS with that login, and `SecureTest` additionally checks:

- the right user, `password_file` and CA connect;
- a wrong password is refused, with a message that does not contain the password (checked in the
  library error, the CLI's stderr and its JSON output);
- a wrong CA, and no CA at all (system trust store), fail certificate verification; the error is a
  security error, nothing is queued, and the CLI exits 3;
- `tls_verify=false` is rejected without `allow_insecure`, and works (encrypted, unverified) with it;
- plaintext (`tls=false`, `allow_insecure=true`) is refused by the TLS-only server;
- `migrate`, `index --name` and `find --name --confirm` work through the CLI on the secure connection.

In default mode, against the plaintext container, `PlaintextServerTest` checks that TLS configured as
required does not fall back to plaintext, and that `tls=false` is a configuration error unless
`allow_insecure=true`, both in `Settings` and on the command line.

`tests/integrations/cassandra/benchIndex.py` records latency, throughput and reconcile time as raw
CSV in `benchmarks/integrations/cassandra/`, with the hardware and versions in `environment.json`.
These are one laptop (Apple M3, 8 cores) talking to one Cassandra node in Docker over loopback with
replication factor 1; they say nothing about a real cluster, and other work was running on the
machine. In the committed run (5 runs, a fresh keyspace each): creating an entry took a median of
3.9 ms (p95 16 ms, 1,000 samples), a lookup by name 1.1 ms (p95 6.4 ms, 1,000 samples), a 100-row
prefix listing 17.6 ms (p95 50 ms, 200 samples), concurrent lookups reached about 500 per second with
1 client, 1,600 with 4 and 2,200 with 16, and reconciling a 3,000-file inventory took a median of
4.5 s the first time, 1.7 s with nothing changed and 1.1 s with one change.

## Limitations

- It indexes what `listStore` and `ls` report. It has no checksum from AegisFS; checksums exist only
  where the indexer read the whole file, and reading costs a full download.
- The index lags AegisFS. Between reconciles an entry can be old; `observed_at` says how old, and
  `--confirm` checks live.
- Logical size of a striped file is unknown until it is read. `size_basis=listStore_no_chunks` is a
  listing-based assumption (see above).
- `striped` is inferred from chunk files and stays unknown for a file without them until it is read.
- Reconcile reads the whole inventory and the whole index; it is meant for thousands to low
  millions of files, not for continuous use on a huge namespace. The no-change reconcile of 3,000
  files took about 1.7 s in the benchmark.
- `list` and `status --full-count` walk the namespace tree and read all buckets; `list` has no
  paging beyond `--limit`. `find --striped` and `stale` return at most `--limit` entries (per attribute for `stale`).
- Attribute tables are updated after the main row (see above) and a reconcile does not repair
  them; replaying the queue does.
- The development container (`cassandra`) is one node with replication factor 1, no TLS and no
  authentication. TLS and password login were tested live against `cassandra-secure` (see Tests):
  one node, a throwaway CA, server-side TLS only. Not tested: client certificates (mutual TLS),
  `NetworkTopologyStrategy` or multiple datacenters, authorization (roles with limited permissions),
  and a Cassandra that was not started from this Compose file. The CQL login `aegis_index` used in
  the test is not restricted, because authorization is off there.
- The test credentials are generated locally and are not for anything real.
- The tests ran with `cassandra-driver` 3.29.3 on Python 3.9. The integrations image installs the
  newest 3.x driver (3.30.1 when built) on Ubuntu 24.04's Python 3.12; there `migrate` and the
  indexer's `status` ran against the plain container, but the test suite was not run in the image.
- One indexer per spool directory is assumed. Two drains over one directory are safe but may
  replay events twice.
- Nothing in the gateway calls `observe` yet.
