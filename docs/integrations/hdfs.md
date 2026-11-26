# HDFS import and export

An optional connector that copies files between AegisFS and an HDFS cluster over WebHDFS. It is a
set of `hdfs` commands in the integrations CLI (`aegisfs-integrations hdfs ...`). AegisFS itself
does not depend on it, and the connector reaches AegisFS only through the normal `aegisfs` client,
so striping, replication, TLS and the cluster token behave exactly as they do for any other client.

It copies whole files. It does not mount HDFS, keep the two systems in sync, or move metadata
(HDFS permissions, owners, timestamps and replication settings are not carried over).

## Commands

| Command | What it does |
| --- | --- |
| `hdfs status` | Checks that the NameNode answers WebHDFS, shows the user, auth mode, safe mode (when the NameNode's JMX page can be read) and whether the AegisFS entry node is healthy. |
| `hdfs list <dir>` | Lists a directory. |
| `hdfs stat <path> [--checksum]` | Shows file status. `--checksum` adds HDFS's own composite checksum, which cannot be compared with SHA-256. |
| `hdfs exists <path>` | Exit 0 if the path exists, 1 if not. |
| `hdfs export <aegisName> <hdfsPath> [--overwrite]` | Copies one AegisFS file to HDFS, verified. |
| `hdfs import <hdfsPath> <aegisName> [--stripe-mib N]` | Copies one HDFS file into AegisFS, verified. |
| `hdfs export-all --prefix P --to <hdfsDir> [--overwrite] [--dry-run]` | Exports every AegisFS file whose name starts with `P`, with a resume journal. |
| `hdfs import-all <hdfsDir> --prefix P [--recursive] [--dry-run] [--stripe-mib N]` | Imports every file in an HDFS directory under the AegisFS name prefix `P`, with a resume journal. |
| `hdfs cleanup <dir> [--older-than-hours H] [--dry-run]` | Removes leftover temporary files this tool created (see below). |

Global flags (before the command): `--namenode-url`, `--hdfs-user`, plus the CLI-wide `--json`,
`--config` and `--entry`. Exit codes follow the CLI: 0 success, 1 failed, 2 usage or configuration
error, 3 HDFS not reachable. `--stripe-mib` is passed to the AegisFS client; files larger than that
(default 256 MiB) are striped by the client, as with `aegisfs create`.

Run the tool through its launcher (`extensions/integrations/aegisfs-integrations`).

## How WebHDFS reads and writes work, and how redirects are handled

A WebHDFS `CREATE` or `OPEN` is two requests. The first goes to the NameNode, which answers
`307 Temporary Redirect` with a `Location` naming a DataNode. The data then moves to or from that
DataNode. This client never lets the HTTP library follow a redirect. It reads the `Location`,
checks it, and only then connects. A redirect is refused when:

- its scheme differs from the scheme of the configured NameNode URL (so an `https` NameNode can
  never send the client to plain `http`, and the reverse is refused too);
- the URL contains user information (`user:pass@host`), control characters or an invalid port;
- its host is neither the NameNode host nor in `allowed_redirect_hosts`;
- the same URL is visited twice (loop), or more than `max_redirects` (default 3) hops occur.

A refused redirect stops the transfer before any file bytes are sent. The check is by host name;
ports are not restricted.

Credentials (the `user.name` parameter, or a delegation token) are attached only to the request
sent to the NameNode. The DataNode URL is requested exactly as the NameNode wrote it, and nothing
is added to it. Error messages print URLs without their query string, so a token cannot appear in
a message.

When the NameNode returns DataNode host names (as the compose cluster does), put those names in
`allowed_redirect_hosts`. The default list is empty, so only the NameNode host itself is accepted.

Request and response bodies move in 1 MiB chunks (`chunk_bytes`). Nothing holds a whole file in
memory. A download that drops mid-way resumes with the `offset` parameter, up to `retries` times,
and never delivers a byte twice.

## Verification and finalization

### Export (AegisFS to HDFS)

1. Read the logical file out of AegisFS into a local temporary file (`aegisfs get`, which
   reassembles striped files and includes appended data), and compute its SHA-256.
2. Check the destination. If it exists the export stops, unless `--overwrite` was given.
   A directory is never replaced.
3. Upload to a temporary name in the destination directory, `.<name>.aegis-tmp-<16 hex>`, using
   `overwrite=false`.
4. Check HDFS reports the expected length, then read the file back from HDFS in a stream and
   compare its SHA-256 with step 1.
5. `RENAME` the temporary file into place. Without `--overwrite` this is the plain HDFS rename,
   which fails if the destination exists, so two racing exporters cannot silently replace each
   other. With `--overwrite` it uses the `OVERWRITE` rename option, which replaces in one step.
6. Check the final length. Only then is success reported, with the destination, byte count, SHA-256,
   seconds and MB/s.

On any failure, including Ctrl-C or SIGTERM, the temporary HDFS file is deleted. The delete call
only accepts names matching the temp pattern above; it will not delete any other path. If that
delete itself fails, the error output lists `temp_left_in_hdfs`. After SIGKILL, a power loss or a
lost network there is no chance to clean up: the destination is still never half-written (it only
appears through the rename), and `hdfs cleanup` removes the leftover temp. The AegisFS source is
never modified.

Parent directories of the destination are created implicitly by the HDFS create.

A rename whose answer is lost is reported as a failure even if HDFS performed it; the destination
is then complete and verified, and a rerun with the same arguments will say it already exists.

### Import (HDFS to AegisFS)

1. Check the source is a regular file. Refuse if the AegisFS name already exists: AegisFS `create`
   is first-create-only, and this tool never overwrites or appends.
2. Stream the file to a local temporary file while computing SHA-256, resuming a dropped
   connection if needed. The byte count must equal the length HDFS reported, and the file's length
   and modification time must be unchanged after the read.
3. `create` it in AegisFS through the normal client. `ALREADY_EXISTS` here (someone created the
   name after step 1) is reported and nothing is overwritten.
4. Read it back through `aegisfs get` and compare SHA-256.
5. If AegisFS committed fewer than the full replica count (`DEGRADED`), the import is reported as
   `DEGRADED` with exit code 1, not as OK, even though the data was read back correctly.

The HDFS source is never deleted. Local temporary files are removed. AegisFS has no delete
command, so if the create succeeded but the read-back failed or mismatched (`UNVERIFIED` or
`MISMATCH` in the output), the file remains in AegisFS and the error says so.

## Batch copies and resume

`export-all` and `import-all` keep a JSON-lines journal, `job-<id>.jsonl`, in `state_dir`. The job
id is a hash of the direction, NameNode URL, and the source and destination arguments, so repeating
the same command resumes the same job. Each item is recorded as `started`, then `complete`,
`failed`, `degraded` or `skipped`; each line is flushed and fsynced. A torn last line is ignored.
A lock prevents two runs of one job at once.

On a rerun:

- an item recorded `complete` is re-verified before it is skipped (export: HDFS length and SHA-256
  of the destination; import: read back through AegisFS and compare SHA-256);
- if the destination of a completed item is gone, it is transferred again;
- if it exists but no longer matches, the item fails with an explanation and is not overwritten
  (`--overwrite` on `export-all` skips the journal check and exports every item again, replacing
  the destinations);
- an item whose last record is `started` (a crash) and whose destination exists is accepted only
  if its bytes are identical to the source;
- any other destination that already exists is a failure, never silently replaced;
- a `degraded` import is not retried: the name now exists in AegisFS and imports never overwrite.

The command prints one row per item and exits 1 if any item failed or degraded. `--dry-run` lists
what would be transferred without touching anything. `export-all` needs a non-empty `--prefix`.
Names are relative to the prefix, and sub-directories in names are kept. `export-all` finds files
by asking every alive AegisFS node what it stores, so a file held only by unreachable nodes is
missed (a note is printed). Stripe chunk files are skipped; the striped file itself is exported as
one logical file. `import-all` skips temp files made by this tool.

## Safety rules applied to names

HDFS paths must be absolute, with no `.` or `..` parts, empty parts, trailing slash, control
characters, `:` or backslash. AegisFS names follow the storage nodes' rules (no leading `/`, no
control characters, no `.` or `..` parts, at most 1024 bytes) plus: no empty parts and no
`.stripe.` text. With `root` set, relative HDFS paths are taken to be below it and an absolute path
outside it is refused. All of this is checked before any request is made.

## Security options

- **Transport.** `https://` NameNode URLs verify the certificate and host name against the system
  CAs, or against `ca_file`. There is no option to turn verification off. Plain `http://` is
  refused unless `allow_insecure_http` is set (`AEGISFS_INT_HDFS_ALLOW_INSECURE_HTTP=1`); every
  command then prints a warning. Use it for development clusters only.
- **Authentication.** `auth=simple` (default) sends `user.name`, which HDFS accepts without
  proof; it is only as strong as the cluster's own setting. `auth=token` sends a delegation token
  read from `token_file` as the `delegation` parameter. **Kerberos (SPNEGO) is not supported.**
  The token mode is only tested against the in-process fake server, because the test cluster runs
  without Kerberos and cannot issue tokens.
- **Secrets.** The token is read from a file, never from flags or the JSON config, and does not
  appear in error messages.
- **Deletion.** The only HDFS delete the tool can issue is for its own temp names. It never
  deletes user data or the AegisFS source.

## Configuration reference

Precedence: command-line flag, then environment `AEGISFS_INT_HDFS_<KEY>`, then the `"hdfs"` object
in the JSON config file, then the default.

| Key | Default | Meaning |
| --- | --- | --- |
| `namenode_url` | none (required) | `https://host:port` of the NameNode's WebHDFS endpoint. Flag `--namenode-url`. |
| `auth` | `simple` | `simple` or `token`. |
| `user` | login name | User for `simple`. Flag `--hdfs-user`. |
| `token_file` | none | File holding the delegation token (`auth=token`). |
| `ca_file` | system CAs | CA bundle for https. |
| `allow_insecure_http` | false | Permit `http://` (development only). |
| `allowed_redirect_hosts` | empty | Comma-separated DataNode host names accepted in redirects, besides the NameNode host. |
| `root` | none | HDFS directory fence; also the base for relative paths. |
| `state_dir` | `~/.aegisfs-integrations/hdfs` | Where batch journals live. |
| `tmp_dir` | system temp | Where local temporary copies go. They need room for one whole file. |
| `timeout_seconds` | 60 | Socket timeout per request. |
| `retries` | 3 | Retries for idempotent calls and download resumes. |
| `retry_backoff_seconds` | 0.5 | Base for exponential backoff. |
| `max_redirects` | 3 | Redirect hops per request. |
| `chunk_bytes` | 1048576 | Streaming chunk size. |

Retries cover connection errors and HTTP 502/503/504, for read-only and idempotent calls
(status, list, mkdirs, delete of our temp, open). `CREATE` and `RENAME` are not retried; a failed
export is cleaned up and rerun instead. Error mapping: `FileNotFoundException` and 404 become "not
found", `FileAlreadyExistsException` "exists", `AccessControlException` and HTTP 401/403 "access
denied" (exit 1), `SafeModeException`, `StandbyException` and HTTP 5xx "unavailable" (exit 3).

## Deployment for testing

`deploy/docker/compose.hdfs.yaml` adds, under the `hdfs` profile only, one NameNode and two
DataNodes from `apache/hadoop:3.4.1` (replication 2, 16 MiB block size, heaps of 512/256 MiB,
memory limits of 1 GiB and 768 MiB), plus a `connector` container built from
`deploy/docker/integrations.Dockerfile`, joined to the `aegis` network of the core file. WebHDFS is
also published on `127.0.0.1:9870` for inspection (`AEGISFS_HDFS_NN_PORT` to change).

```
scripts/integrations/hdfsUp.sh              # HDFS + connector + AegisFS node0-node2
scripts/integrations/hdfsUp.sh --no-aegis   # HDFS and connector only
scripts/integrations/hdfsDown.sh            # stop, keep volumes
scripts/integrations/hdfsDown.sh --purge    # stop and delete this project's volumes
scripts/integrations/hdfsCleanup.sh         # empty /aegisfs-test in HDFS
tests/integrations/hdfs/runLive.sh          # up, unit + live tests, DataNode-kill test, down
tests/integrations/hdfs/runBench.sh         # benchmark; writes benchmarks/integrations/hdfs/
```

Notes that matter when you adapt it:

- The `apache/hadoop` images are published for `linux/amd64` only. On Apple silicon they run under
  emulation (the compose file pins the platform so this is explicit). It works; do not read
  performance from it.
- The NameNode redirects to DataNodes by host name (`datanode1`, `datanode2`); those resolve only
  inside the compose network. That is why the connector runs in the network and the live tests run
  inside it with `docker compose exec`. The compose file sets
  `AEGISFS_INT_HDFS_ALLOWED_REDIRECT_HOSTS=datanode1,datanode2`.
- Volumes are created root-owned, so a one-shot `hdfs-init` service sets the owner for uid 1000.
- `hdfsUp.sh` makes `/aegisfs-test` in HDFS, owned by the user `aegisfs` that the connector
  authenticates as, and sets `hdfs.root` to it, so the connector cannot write elsewhere.
- The connector mounts the repository's `extensions/integrations` and `tests/integrations`
  read-only, so Python changes are picked up without rebuilding. The image build compiles the
  `aegisfs` client and takes several minutes the first time.
- Compose project name is `aegis-hdfs`. The three AegisFS nodes are published on host ports
  9600-9602 (`AEGISFS_PORT0..2`), and use the throwaway credentials in `.dev-compose/`.
- The compose cluster uses `simple` authentication over plain HTTP. HTTPS to HDFS and Kerberos
  are not exercised by the live tests.

## Tests

`tests/integrations/hdfs/`:

- `testWebHdfs.py`, `testTransfer.py`, `testTls.py`: unit tests against an in-process fake WebHDFS
  server (`fakeWebHdfs.py`) and a fake AegisFS client. They cover redirect handling and every
  redirect refusal, scheme downgrade, loops, error mapping, retries, chunking, resume, journal
  and batch resume, temp cleanup, overwrite refusal, name checks and configuration refusals. The
  TLS tests use a throwaway certificate and need `openssl`.
- `testLive.py`: against the real HDFS and AegisFS containers. It skips with a message when HDFS is
  not reachable, and fails instead when `AEGISFS_INT_HDFS_LIVE=1`.
- `interruptDataNode.sh`: kills both DataNodes in the middle of a 400 MiB export.
- `benchTransfer.py`, `measureRss.py`: benchmark and peak-memory helper.

## Limitations

- Whole-file copies only; no incremental sync, no directory mirroring beyond the batch commands,
  no metadata carry-over, no deletes.
- Transfers stage whole files in local temporary storage (`tmp_dir`): an export needs room for one
  copy of the file, an import for two (the download and the read-back). Memory stays bounded; disk
  use does not.
- The export of a file is not atomic with respect to concurrent appends to it in AegisFS: it
  exports whatever `get` returned.
- Imports cannot overwrite or append, and cannot be undone by this tool.
- Kerberos is not supported. Delegation-token auth is untested against a real cluster. HTTPS is
  tested only against the fake server.
- HDFS federation, HA failover (no retry against a second NameNode), encryption zones and erasure
  coding were not tested.
- The benchmark numbers below come from one laptop with a small emulated HDFS.

## Measured performance

Raw samples are in `benchmarks/integrations/hdfs/transfers.csv`, medians in `summary.csv`, and the
hardware and versions in `environment.json`. One run on an Apple M3 laptop (8 cores, 16 GB),
Docker Desktop, 1 NameNode and 2 DataNodes (replication 2, `linux/amd64` image under emulation),
3 AegisFS nodes, everything on the same machine. 1 MiB and 10 MiB: 5 samples; 100 MiB: 3 samples;
1 KiB x 30 files: 5 samples. Other work was running on the machine, and the small-file runs varied
a lot (see the min and max columns in `summary.csv`), so treat these as rough.

| Scenario | Median wall time | Median MB/s | Peak RSS (Python process) |
| --- | --- | --- | --- |
| export 100 MiB | 3.07 s (min 2.63, max 3.39) | 34 | 25 MiB |
| import 100 MiB | 1.88 s (min 1.87, max 2.34) | 56 | 25 MiB |
| export 10 MiB | 0.48 s | 22 | 25 MiB |
| import 10 MiB | 0.52 s | 20 | 25 MiB |
| export 1 MiB | 0.35 s | 3.0 | 24 MiB |
| import 1 MiB | 0.33 s | 3.2 | 24 MiB |
| export 30 x 1 KiB, one process per file | 15.4 s (0.51 s per file; min 8.5, max 33) | | |
| import 30 x 1 KiB, one process per file | 14.6 s (0.49 s per file; min 8.9, max 33) | | |
| export 30 x 1 KiB, one `export-all` | 12.7 s (0.42 s per file; min 4.2, max 14.6) | | |
| import 30 x 1 KiB, one `import-all` | 7.5 s (0.25 s per file; min 4.2, max 13.9) | | |

For comparison, native `aegisfs create` / `get` of the same files: 100 MiB in 0.41 s / 0.62 s,
1 MiB in 0.04 s, and 30 small `get` calls in 1.2 s. These are not like for like. A native create
writes once to AegisFS. An export reads the file out of AegisFS, uploads it, reads it back from
HDFS to hash it, and renames it; an import does the mirror image. So an export or import moves each
byte several times by design, in return for the verification described above, and the HDFS side
runs under CPU emulation. The per-file cost of small files is dominated by fixed work (several
WebHDFS round trips, the AegisFS client process, journal fsyncs in batch mode), not by bytes; batch
mode did not beat one process per file in the export case here, and its journal writes are a likely
reason.

The peak memory figure is the resident-set high-water mark of the Python process (VmHWM). It is
the same for 1 MiB and 100 MiB, which is what bounded streaming should look like. It does not
include the `aegisfs` client process.
