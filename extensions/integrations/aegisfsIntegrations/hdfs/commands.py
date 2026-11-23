"""`hdfs` sub-commands for the integrations CLI.

    hdfs status                       reachability, user, safe mode
    hdfs list <dir>                   list a directory
    hdfs stat <path> [--checksum]     file status
    hdfs exists <path>                exit 0 if present, 1 if not
    hdfs export <aegisName> <hdfsPath> [--overwrite]
    hdfs import <hdfsPath> <aegisName>
    hdfs export-all --prefix P --to <hdfsDir> [--overwrite] [--dry-run]
    hdfs import-all <hdfsDir> --prefix P [--recursive] [--dry-run]
    hdfs cleanup <dir> [--older-than-hours H] [--dry-run]

Run the tool through its launcher script (aegisfs-integrations).
"""
import os
import shutil
import signal
import sys
import tempfile
import time

from ..aegisfsClient import AegisError, is_stripe_chunk, sha256_file
from ..cli import CommandFailed, IntegrationUnavailable, emit
from . import paths, settings as hdfsSettings
from .journal import Journal, JournalBusy
from .transfer import (REDO, TransferFailed, batch_job_id, export_file, export_items, import_file,
                       run_batch, walk_hdfs)
from .webHdfs import (HdfsAccessDenied, HdfsError, HdfsNotFound, HdfsProtocolError,
                      HdfsUnavailable, WebHdfsClient, is_temp_name)


def register(areas):
    parser = areas.add_parser("hdfs", help="import and export files between AegisFS and HDFS "
                                           "(WebHDFS)")
    parser.add_argument("--namenode-url", help="NameNode WebHDFS base URL (https://host:port)")
    parser.add_argument("--hdfs-user", help="user name for simple authentication")
    sub = parser.add_subparsers(dest="hdfs_command", metavar="<command>")

    def add(name, helptext, fn):
        p = sub.add_parser(name, help=helptext, description=helptext)
        p.set_defaults(handler=fn)
        return p

    add("status", "check that the NameNode answers and show the user and safe mode", cmd_status)
    p = add("list", "list a directory", cmd_list)
    p.add_argument("directory")
    p = add("stat", "show a file or directory status", cmd_stat)
    p.add_argument("path")
    p.add_argument("--checksum", action="store_true", help="also fetch HDFS's own file checksum")
    p = add("exists", "exit 0 if the path exists, 1 if it does not", cmd_exists)
    p.add_argument("path")

    p = add("export", "copy an AegisFS file to HDFS, verified", cmd_export)
    p.add_argument("aegis_name")
    p.add_argument("hdfs_path")
    p.add_argument("--overwrite", action="store_true",
                   help="replace an existing destination (default: refuse)")
    p = add("import", "copy an HDFS file into AegisFS, verified", cmd_import)
    p.add_argument("hdfs_path")
    p.add_argument("aegis_name")
    p.add_argument("--stripe-mib", type=int, help="client stripe size for the create")

    p = add("export-all", "export every AegisFS file under a name prefix, with a resume journal",
            cmd_export_all)
    p.add_argument("--prefix", required=True, help="AegisFS name prefix to export")
    p.add_argument("--to", required=True, dest="to_dir", help="HDFS directory to write into")
    p.add_argument("--overwrite", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p = add("import-all", "import every file in an HDFS directory, with a resume journal",
            cmd_import_all)
    p.add_argument("hdfs_dir")
    p.add_argument("--prefix", required=True, help="AegisFS name prefix for the imported files")
    p.add_argument("--recursive", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--stripe-mib", type=int)

    p = add("cleanup", "remove this tool's own leftover temporary files from a directory",
            cmd_cleanup)
    p.add_argument("directory")
    p.add_argument("--older-than-hours", type=float, default=24.0)
    p.add_argument("--dry-run", action="store_true")


# ---- shared setup ----

def _client(args, cfg):
    cfgsettings, options = hdfsSettings.load(cfg, getattr(args, "namenode_url", None),
                                             getattr(args, "hdfs_user", None))
    if cfgsettings.namenode_url.startswith("http://"):
        print("WARNING: using plain http:// to the NameNode. Data and the user name cross the "
              "network unprotected. This is for development clusters only.", file=sys.stderr)
    return WebHdfsClient(cfgsettings), options


def _path(value, options):
    try:
        return paths.resolve_hdfs_path(value, options.root)
    except paths.InvalidName as exc:
        raise CommandFailed("invalid HDFS path: %s" % exc)


def _guard(fn):
    """Turn HDFS errors into the CLI's exit codes."""
    def wrapper(args, cfg, aegis):
        try:
            return fn(args, cfg, aegis)
        except HdfsAccessDenied as exc:
            raise CommandFailed("HDFS denied the request: %s" % exc)
        except HdfsNotFound as exc:
            raise CommandFailed("not found in HDFS: %s" % exc)
        except HdfsUnavailable as exc:
            raise IntegrationUnavailable("HDFS is not usable: %s" % exc)
        except HdfsProtocolError as exc:
            raise CommandFailed("HDFS protocol problem: %s" % exc)
        except HdfsError as exc:
            raise CommandFailed("HDFS error: %s" % exc)
        except TransferFailed as exc:
            if exc.payload.get("hdfs_unavailable"):
                raise IntegrationUnavailable("HDFS is not usable: %s" % exc)
            raise CommandFailed(str(exc), exc.payload)
        except paths.InvalidName as exc:
            raise CommandFailed("invalid name: %s" % exc)
        except JournalBusy as exc:
            raise CommandFailed(str(exc))
    wrapper.__name__ = fn.__name__
    return wrapper


def _terminate_as_interrupt():
    """Make SIGTERM unwind like Ctrl-C, so a stopped transfer still removes its temp files."""
    def handler(signum, frame):
        raise KeyboardInterrupt()
    try:
        signal.signal(signal.SIGTERM, handler)
    except ValueError:
        pass  # not the main thread


def _fmt_result(r):
    return ("%s: %s -> %s\n  bytes=%s sha256=%s\n  seconds=%s MB/s=%s%s" % (
        r.get("status"), r["source"], r["destination"], r.get("bytes"), r.get("sha256"),
        r.get("seconds"), r.get("mb_per_s"),
        "\n  note: destination already held identical bytes; nothing was rewritten"
        if r.get("adopted_existing") else ""))


# ---- single-item commands ----

@_guard
def cmd_status(args, cfg, aegis):
    client, options = _client(args, cfg)
    root = client.namenode_reachable()
    payload = {"namenode": "%s://%s:%d" % (client.scheme, client.host, client.port),
               "reachable": True, "user": client.settings.user or "(delegation token)",
               "auth": "token" if client.settings.token else "simple",
               "root_owner": root.get("owner"), "safemode": client.safemode(),
               "root_fence": options.root, "aegisfs_healthy": aegis.health()}
    emit(args, payload)
    return 0


@_guard
def cmd_list(args, cfg, aegis):
    client, options = _client(args, cfg)
    path = _path(args.directory, options)
    entries = client.list(path)
    rows = [{"name": e["pathSuffix"], "type": e["type"], "length": e["length"],
             "owner": e["owner"], "modified_ms": e["modificationTime"]} for e in entries]
    lines = ["%-9s %12d  %s" % (r["type"], r["length"], r["name"]) for r in rows]
    emit(args, {"path": path, "entries": rows}, "\n".join(lines) if lines else "(empty)")
    return 0


@_guard
def cmd_stat(args, cfg, aegis):
    client, options = _client(args, cfg)
    path = _path(args.path, options)
    info = client.status(path)
    payload = {"path": path, "type": info["type"], "length": info["length"],
               "owner": info["owner"], "group": info["group"],
               "permission": info["permission"], "replication": info["replication"],
               "block_size": info["blockSize"], "modified_ms": info["modificationTime"]}
    if args.checksum and info["type"] == "FILE":
        payload["hdfs_checksum"] = client.checksum(path)
    emit(args, payload)
    return 0


@_guard
def cmd_exists(args, cfg, aegis):
    client, options = _client(args, cfg)
    path = _path(args.path, options)
    present = client.exists(path)
    emit(args, {"path": path, "exists": present}, "yes" if present else "no")
    return 0 if present else 1


@_guard
def cmd_export(args, cfg, aegis):
    _terminate_as_interrupt()
    client, options = _client(args, cfg)
    dest = _path(args.hdfs_path, options)
    result = export_file(aegis, client, args.aegis_name, dest, args.overwrite, options.tmp_dir)
    emit(args, result, _fmt_result(result))
    return 0


@_guard
def cmd_import(args, cfg, aegis):
    _terminate_as_interrupt()
    client, options = _client(args, cfg)
    if args.stripe_mib is not None:
        aegis.stripe_mib = args.stripe_mib
    source = _path(args.hdfs_path, options)
    result = import_file(aegis, client, source, args.aegis_name, options.tmp_dir)
    emit(args, result, _fmt_result(result))
    return 0


# ---- batch ----

def _report_batch(args, direction, rows, journal_path, dry=False):
    failed = [r for r in rows if r["state"] in ("failed", "degraded")]
    counts = {}
    for r in rows:
        counts[r["state"]] = counts.get(r["state"], 0) + 1
    payload = {"direction": direction, "journal": journal_path, "dry_run": dry,
               "counts": counts, "results": rows}
    lines = ["%-9s %12s  %s%s" % (r["state"], r["bytes"] if r["bytes"] is not None else "-",
                                   r["item"], ("  (" + r["detail"] + ")") if r["detail"] else "")
             for r in rows]
    lines.append("summary: " + ", ".join("%s=%d" % kv for kv in sorted(counts.items()))
                 if counts else "summary: nothing to do")
    if journal_path:
        lines.append("journal: %s" % journal_path)
    emit(args, payload, "\n".join(lines))
    if failed:
        raise CommandFailed("%d of %d items did not complete" % (len(failed), len(rows)))
    return 0


@_guard
def cmd_export_all(args, cfg, aegis):
    _terminate_as_interrupt()
    client, options = _client(args, cfg)
    dest_dir = _path(args.to_dir, options)
    prefix = args.prefix
    if not prefix:
        raise CommandFailed("--prefix must not be empty")
    found, unreachable = aegis.inventory()
    names = sorted(n for n in found if n.startswith(prefix) and not is_stripe_chunk(n))
    items = export_items(aegis, names, dest_dir, prefix)
    if unreachable:
        print("note: could not list %d node(s); files held only there are not included"
              % len(unreachable), file=sys.stderr)
    if args.dry_run:
        rows = [{"item": k, "state": "planned", "bytes": None, "sha256": None,
                 "detail": "-> " + d} for k, _, d in items]
        return _report_batch(args, "export", rows, None, dry=True)

    def do_one(source, destination, adopt):
        paths.resolve_hdfs_path(destination, options.root)
        return export_file(aegis, client, source, destination, args.overwrite, options.tmp_dir,
                           adopt_identical=adopt)

    def verify(destination, record):
        try:
            info = client.status(destination)
            if info.get("length") != record.get("bytes"):
                return "recorded complete but %s is now %s bytes" % (destination, info.get("length"))
            sha, _ = client.sha256(destination, info.get("length"))
        except HdfsNotFound:
            return REDO
        except HdfsError as exc:
            return "recorded complete but %s cannot be verified: %s" % (destination, exc)
        if sha != record.get("sha256"):
            return "recorded complete but %s no longer matches the recorded sha256" % destination
        return None

    job = batch_job_id("export", client.settings.namenode_url, prefix, dest_dir)
    with Journal(options.state_dir, job) as journal:
        rows = run_batch(journal, items, do_one, verify, args.overwrite)
        return _report_batch(args, "export", rows, journal.path)


@_guard
def cmd_import_all(args, cfg, aegis):
    _terminate_as_interrupt()
    client, options = _client(args, cfg)
    source_dir = _path(args.hdfs_dir, options)
    prefix = args.prefix
    if not prefix:
        raise CommandFailed("--prefix must not be empty")
    if args.stripe_mib is not None:
        aegis.stripe_mib = args.stripe_mib
    items = []
    for rel, _length in walk_hdfs(client, source_dir, args.recursive):
        items.append((source_dir.rstrip("/") + "/" + rel, source_dir.rstrip("/") + "/" + rel,
                      prefix + rel))
    if args.dry_run:
        rows = [{"item": k, "state": "planned", "bytes": None, "sha256": None,
                 "detail": "-> " + d} for k, _, d in items]
        return _report_batch(args, "import", rows, None, dry=True)

    def do_one(source, destination, adopt):
        return import_file(aegis, client, source, destination, options.tmp_dir,
                           adopt_identical=adopt)

    def verify(destination, record):
        workdir = tempfile.mkdtemp(prefix="aegis-hdfs-", dir=options.tmp_dir)
        try:
            aegis.get(destination, os.path.join(workdir, "v"))
            if sha256_file(os.path.join(workdir, "v")) != record.get("sha256"):
                return "recorded complete but %s no longer matches the recorded sha256" % destination
        except AegisError as exc:
            if exc.status == "NOT_FOUND":
                return REDO
            return "recorded complete but %s cannot be read from AegisFS: %s" % (destination, exc)
        finally:
            shutil.rmtree(workdir, ignore_errors=True)
        return None

    job = batch_job_id("import", client.settings.namenode_url, source_dir, prefix)
    with Journal(options.state_dir, job) as journal:
        rows = run_batch(journal, items, do_one, verify, False)
        return _report_batch(args, "import", rows, journal.path)


@_guard
def cmd_cleanup(args, cfg, aegis):
    client, options = _client(args, cfg)
    directory = _path(args.directory, options)
    cutoff_ms = (time.time() - args.older_than_hours * 3600) * 1000
    removed, kept, errors = [], [], []
    for entry in client.list(directory):
        name = entry["pathSuffix"]
        if entry["type"] != "FILE" or not is_temp_name(name):
            continue
        full = directory.rstrip("/") + "/" + name
        if entry["modificationTime"] > cutoff_ms:
            kept.append(full)
            continue
        if args.dry_run:
            removed.append(full)
            continue
        try:
            client.delete_temp(full)
            removed.append(full)
        except HdfsError as exc:
            errors.append("%s: %s" % (full, exc))
    payload = {"directory": directory, "dry_run": args.dry_run, "removed": removed,
               "too_recent": kept, "errors": errors}
    emit(args, payload, "%s %d temp file(s); %d too recent; %d error(s)" % (
        "would remove" if args.dry_run else "removed", len(removed), len(kept), len(errors)))
    if errors:
        raise CommandFailed("%d temp file(s) could not be removed" % len(errors))
    return 0
