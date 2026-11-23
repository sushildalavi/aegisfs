"""`aegisfs-integrations cassandra ...` sub-commands."""
import json
import time

from ..aegisfsClient import AegisError
from ..cli import CommandFailed, EXIT_FAILED, EXIT_OK, IntegrationUnavailable, emit
from ..config import ConfigError
from . import indexer, queries, schema
from .errors import DriverMissing, SchemaError, SecurityError, StoreUnavailable
from .model import ObservationError, utcnow_ms
from .settings import Settings
from .spool import Spool
from .store import ATTRS, IndexStore

CONNECTION_FLAGS = (
    ("--contact-points", "contact_points", "comma separated Cassandra hosts"),
    ("--port", "port", "Cassandra native port"),
    ("--keyspace", "keyspace", "keyspace holding the index"),
    ("--spool-dir", "spool_dir", "directory for the local retry queue"),
)


def register(areas):
    area = areas.add_parser("cassandra", help="optional Cassandra metadata index",
                            description="Index of what AegisFS reports about its files. AegisFS "
                                        "remains the source of truth.")
    sub = area.add_subparsers(dest="command", metavar="<command>")
    area.set_defaults(handler=lambda args, cfg, aegis: (area.print_help(), 2)[1])

    def add(name, helptext, handler):
        p = sub.add_parser(name, help=helptext, description=helptext)
        for flag, dest, text in CONNECTION_FLAGS:
            p.add_argument(flag, dest="o_" + dest, help=text)
        p.set_defaults(handler=handler)
        return p

    add("migrate", "create the keyspace (if allowed) and apply schema migrations", cmd_migrate)
    p = add("status", "connectivity, schema version, counts and retry-queue depth", cmd_status)
    p.add_argument("--full-count", action="store_true", help="also count every indexed file (walks all namespaces)")
    p = add("index", "bring the index in step with AegisFS", cmd_index)
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--reconcile", action="store_true", help="scan AegisFS and update the index")
    mode.add_argument("--name", help="index one file")
    mode.add_argument("--drain", action="store_true", help="send queued work to Cassandra")
    mode.add_argument("--watch", type=float, metavar="SECONDS", help="drain every SECONDS until stopped")
    p.add_argument("--reconcile-every", type=float, default=0, metavar="SECONDS",
                   help="with --watch, also reconcile this often (0 = never)")
    p.add_argument("--iterations", type=int, default=0, help=argparse_suppress())
    p.add_argument("--verify-checksums", action="store_true",
                   help="read files from AegisFS and record their SHA-256")
    p.add_argument("--max-bytes", type=int, help="with --verify-checksums, skip files larger than this")
    p = add("find", "look up one file by exact name, or list striped files", cmd_find)
    p.add_argument("--name", help="exact file name")
    p.add_argument("--striped", action="store_true", help="files known to be striped")
    p.add_argument("--limit", type=int, default=100)
    add_confirm(p)
    p = add("list", "list files under a name prefix", cmd_list)
    p.add_argument("--prefix", default="", help="names starting with this (default: everything)")
    p.add_argument("--limit", type=int, default=100)
    p.add_argument("--no-recursive", action="store_true", help="do not descend into sub-namespaces")
    p = add("stale", "list entries marked stale or incomplete", cmd_stale)
    p.add_argument("--limit", type=int, default=100)
    p = add("show", "everything the index holds about one file", cmd_show)
    p.add_argument("--name", required=True)
    add_confirm(p)


def argparse_suppress():
    import argparse
    return argparse.SUPPRESS


def add_confirm(p):
    p.add_argument("--confirm", action="store_true", help="also ask AegisFS now and report any disagreement")
    p.add_argument("--read", action="store_true",
                   help="with --confirm, read the file and compare its SHA-256 with the indexed one")


# ---------------------------------------------------------------- helpers
def _overrides(args):
    out = {}
    for _, dest, _ in CONNECTION_FLAGS:
        value = getattr(args, "o_" + dest, None)
        if value is not None:
            out[dest] = value
    return out


def settings_for(args, cfg):
    try:
        return Settings.from_config(cfg, _overrides(args))
    except ConfigError:
        raise


def open_store(settings):
    try:
        return IndexStore.open(settings)
    except (DriverMissing, SecurityError) as exc:
        raise IntegrationUnavailable(str(exc))
    except StoreUnavailable as exc:
        raise IntegrationUnavailable(str(exc))
    except SchemaError as exc:
        raise CommandFailed(str(exc))


def try_open_store(settings):
    """Like open_store, but returns None when Cassandra is down (work then goes to the spool)."""
    try:
        return IndexStore.open(settings)
    except (DriverMissing, SecurityError) as exc:
        raise IntegrationUnavailable(str(exc))
    except StoreUnavailable:
        return None
    except SchemaError as exc:
        raise CommandFailed(str(exc))


def _times(obj):
    return json.loads(json.dumps(obj, default=str))


# ---------------------------------------------------------------- commands
def cmd_migrate(args, cfg, aegis):
    settings = settings_for(args, cfg)
    try:
        result = schema.run_migrate(settings)
    except (DriverMissing, SecurityError) as exc:
        raise IntegrationUnavailable(str(exc))
    except StoreUnavailable as exc:
        raise IntegrationUnavailable(str(exc))
    except SchemaError as exc:
        raise CommandFailed(str(exc))
    human = "keyspace=%s schema_version=%d applied=%s already_applied=%s bucket_count=%d" % (
        result["keyspace"], result["schema_version"], result["applied"] or "none", result["already"] or "none",
        result["bucket_count"])
    if result["bucket_count"] != settings.bucket_count:
        human += ("\nnote: the index already uses bucket_count=%d; the configured value %d is ignored"
                  % (result["bucket_count"], settings.bucket_count))
    emit(args, result, human)
    return EXIT_OK


def cmd_status(args, cfg, aegis):
    settings = settings_for(args, cfg)
    spool = Spool(settings.spool_dir)
    payload = {"connected": False, "keyspace": settings.keyspace,
               "spool": {"dir": settings.spool_dir, "pending_events": spool.depth(),
                         "rejected_events": spool.rejected_count()},
               "settings": settings.public_summary()}
    try:
        store = IndexStore.open(settings)
    except (DriverMissing, SecurityError) as exc:
        payload["error"] = str(exc)
        emit(args, queries.labelled(payload), _status_text(payload))
        raise IntegrationUnavailable(str(exc))
    except StoreUnavailable as exc:
        payload["error"] = str(exc)
        emit(args, queries.labelled(payload), _status_text(payload))
        raise IntegrationUnavailable(str(exc))
    except SchemaError as exc:
        payload.update(connected=True, error=str(exc))
        emit(args, queries.labelled(payload), _status_text(payload))
        raise CommandFailed(str(exc))
    try:
        payload["connected"] = True
        payload["schema_version"] = store.backend.schema_version()
        payload["bucket_count"] = store.bucket_count
        payload["counts"] = {
            "stale": store.count_attribute("status=stale"),
            "incomplete": store.count_attribute("status=incomplete"),
            "striped": store.count_attribute("striped=true"),
        }
        if args.full_count:
            payload["counts"]["files"] = store.count_files()
    except StoreUnavailable as exc:
        payload["connected"] = False
        payload["error"] = str(exc)
        emit(args, queries.labelled(payload), _status_text(payload))
        raise IntegrationUnavailable(str(exc))
    finally:
        store.close()
    emit(args, queries.labelled(payload, utcnow_ms()), _status_text(payload))
    return EXIT_OK


def _status_text(p):
    lines = ["connected=%s keyspace=%s" % (str(p["connected"]).lower(), p["keyspace"])]
    if "schema_version" in p:
        lines.append("schema_version=%s bucket_count=%s" % (p["schema_version"], p.get("bucket_count")))
    if "counts" in p:
        lines.append("counts: " + " ".join("%s=%s" % kv for kv in sorted(p["counts"].items())))
    lines.append("spool: pending=%d rejected=%d dir=%s" % (
        p["spool"]["pending_events"], p["spool"]["rejected_events"], p["spool"]["dir"]))
    if p.get("error"):
        lines.append("error: %s" % p["error"])
    lines.append(queries.LABEL)
    return "\n".join(lines)


def cmd_index(args, cfg, aegis):
    settings = settings_for(args, cfg)
    spool = Spool(settings.spool_dir)
    if args.max_bytes is not None and not args.verify_checksums:
        raise CommandFailed("--max-bytes only makes sense with --verify-checksums")
    if args.max_bytes is not None and args.max_bytes < 0:
        raise CommandFailed("--max-bytes must not be negative")
    if args.name:
        return _index_one(args, settings, spool, aegis)
    if args.drain:
        return _drain(args, settings, spool, aegis)
    if args.watch is not None:
        return _watch(args, settings, spool, aegis)
    store = open_store(settings)
    try:
        report = indexer.reconcile(store, spool, aegis, settings, verify=args.verify_checksums,
                                   max_bytes=args.max_bytes)
    except StoreUnavailable as exc:
        raise IntegrationUnavailable(str(exc))
    finally:
        store.close()
    emit(args, queries.labelled({"reconcile": report}, utcnow_ms()), _reconcile_text(report))
    return EXIT_FAILED if report["invalid"] or report["verify_failed"] else EXIT_OK


def _reconcile_text(r):
    keys = ("scanned_files", "created", "updated", "unchanged", "marked_stale", "marked_incomplete_scan",
            "already_missing", "incomplete_files", "skipped_newer", "spooled", "invalid", "verified",
            "verify_skipped", "verify_failed", "chunk_files_seen", "orphan_chunk_files", "seconds")
    lines = ["reconcile: " + " ".join("%s=%s" % (k, r[k]) for k in keys)]
    if r["unreachable_nodes"]:
        lines.append("warning: nodes that did not answer the scan: %s" % ", ".join(r["unreachable_nodes"]))
    if r["drained"]:
        lines.append("replayed the retry queue first: %s" % r["drained"])
    for problem in r["problems"][:20]:
        lines.append("problem: %s" % problem)
    return "\n".join(lines)


def _index_one(args, settings, spool, aegis):
    name = args.name
    try:
        obs = indexer.lookup_one(aegis, name, settings, utcnow_ms(), "index --name")
    except AegisError as exc:
        raise CommandFailed("AegisFS could not describe %s: %s" % (name, exc))
    if obs is None:
        raise CommandFailed("AegisFS reports no replica holding %s; nothing was indexed" % name)
    note = None
    if args.verify_checksums:
        estimate = obs.stored_size
        obs, note = indexer.verify_observation(aegis, obs, args.max_bytes, estimate)
    store = try_open_store(settings)
    try:
        outcome = indexer.submit(store, spool, obs)
    except ObservationError as exc:
        raise CommandFailed("not indexed, the observation is invalid: %s" % exc)
    finally:
        if store is not None:
            store.close()
    payload = queries.labelled({"name": name, "outcome": outcome, "version": obs.version,
                                "status": obs.status, "verify_state": obs.verify_state,
                                "verify_note": note}, obs.observed_at)
    human = "%s: %s (version %s, status %s)" % (name, outcome, obs.version, obs.status)
    if outcome == "spooled":
        human += "\nCassandra is not reachable; the update is queued in %s and `index --drain` will send it." % spool.dir
    if note and obs.verify_state != "verified":
        human += "\nchecksum not recorded: %s" % note
    emit(args, payload, human)
    return EXIT_OK


def _drain(args, settings, spool, aegis):
    if spool.depth() == 0:
        emit(args, queries.labelled({"drained": {"done": 0, "remaining": 0}}), "retry queue is empty")
        return EXIT_OK
    store = open_store(settings)
    try:
        report, details = indexer.drain(store, spool, aegis, settings)
    finally:
        store.close()
    payload = {"drained": {"done": report.done, "retried": report.retried, "rejected": report.rejected,
                           "remaining": report.remaining, "stopped_early": report.stopped_early,
                           "error": report.error, **details}}
    human = "drained: done=%d retried=%d rejected=%d remaining=%d" % (
        report.done, report.retried, report.rejected, report.remaining)
    if report.stopped_early:
        human += "\nstopped early: %s" % report.error
    emit(args, queries.labelled(payload, utcnow_ms()), human)
    return EXIT_FAILED if report.stopped_early else EXIT_OK


def _watch(args, settings, spool, aegis):
    interval = max(args.watch, 0.5)
    next_reconcile = time.time() + args.reconcile_every if args.reconcile_every else None
    count = 0
    while True:
        count += 1
        try:
            store = try_open_store(settings)
            if store is None:
                print("cassandra not reachable; %d event(s) waiting" % spool.depth(), flush=True)
            else:
                try:
                    if spool.depth():
                        report, _ = indexer.drain(store, spool, aegis, settings)
                        print("drained done=%d remaining=%d" % (report.done, report.remaining), flush=True)
                    if next_reconcile is not None and time.time() >= next_reconcile:
                        rep = indexer.reconcile(store, spool, aegis, settings,
                                                verify=args.verify_checksums, max_bytes=args.max_bytes)
                        print(_reconcile_text(rep), flush=True)
                        next_reconcile = time.time() + args.reconcile_every
                finally:
                    store.close()
        except (StoreUnavailable, AegisError, CommandFailed) as exc:
            print("will retry: %s" % exc, flush=True)
        if args.iterations and count >= args.iterations:
            return EXIT_OK
        time.sleep(interval)


def cmd_find(args, cfg, aegis):
    if bool(args.name) == bool(args.striped):
        raise CommandFailed("give exactly one of --name or --striped")
    settings = settings_for(args, cfg)
    store = open_store(settings)
    try:
        try:
            if args.name:
                row = store.get(args.name)
                payload = {"name": args.name, "found": row is not None, "entry": queries.row_view(row)}
                human = _entry_text(row, args.name)
                return _finish_entry(args, settings, aegis, row, payload, human)
            rows, dropped, truncated = store.by_attribute("striped=true", args.limit)
        except StoreUnavailable as exc:
            raise IntegrationUnavailable(str(exc))
    finally:
        store.close()
    payload = {"attribute": "striped=true", "count": len(rows), "truncated": truncated,
               "dropped_inconsistent": dropped, "entries": [queries.summary_view(r) for r in rows]}
    emit(args, queries.labelled(payload), _rows_text(rows, truncated))
    return EXIT_OK


def cmd_show(args, cfg, aegis):
    settings = settings_for(args, cfg)
    store = open_store(settings)
    try:
        try:
            row = store.get(args.name)
        except StoreUnavailable as exc:
            raise IntegrationUnavailable(str(exc))
    finally:
        store.close()
    payload = {"name": args.name, "found": row is not None, "entry": queries.row_view(row)}
    return _finish_entry(args, settings, aegis, row, payload, _entry_text(row, args.name, detail=True))


def _finish_entry(args, settings, aegis, row, payload, human):
    if row is None:
        emit(args, queries.labelled(payload), human)
        if args.confirm:
            raise CommandFailed("no index entry to confirm for %s" % args.name)
        return EXIT_OK
    code = EXIT_OK
    if args.confirm:
        result = queries.confirm(aegis, row, settings, read=args.read)
        payload["confirm"] = result
        human += "\nconfirm (live AegisFS, checked_at %s): %s" % (
            result["checked_at"], "agrees" if result["agrees"] else "DISAGREES")
        for diff in result["differences"]:
            human += "\n  - " + diff
        rc = result.get("read_check")
        if rc and not rc.get("aegisfs_error"):
            human += "\n  read check: sha256=%s size=%s matches_index_checksum=%s" % (
                rc["sha256"], rc["size"], rc["matches_index_checksum"])
        if result["aegisfs_error"]:
            human += ("\nAegisFS reports a failure. The index entry does not override it: treat the "
                      "file as unavailable or corrupt until AegisFS says otherwise.")
        if not result["agrees"]:
            code = EXIT_FAILED
    elif args.read:
        raise CommandFailed("--read needs --confirm")
    emit(args, queries.labelled(payload, row["observed_at"]), human)
    return code


def cmd_list(args, cfg, aegis):
    if args.limit < 1:
        raise CommandFailed("--limit must be at least 1")
    settings = settings_for(args, cfg)
    store = open_store(settings)
    try:
        try:
            rows, truncated = store.namespace_listing(args.prefix, not args.no_recursive, args.limit)
        except StoreUnavailable as exc:
            raise IntegrationUnavailable(str(exc))
    finally:
        store.close()
    payload = {"prefix": args.prefix, "count": len(rows), "truncated": truncated,
               "entries": [queries.summary_view(r) for r in rows]}
    emit(args, queries.labelled(payload), _rows_text(rows, truncated))
    return EXIT_OK


def cmd_stale(args, cfg, aegis):
    settings = settings_for(args, cfg)
    store = open_store(settings)
    try:
        try:
            out, dropped, truncated = [], 0, False
            for attr in ("status=stale", "status=incomplete"):
                rows, d, t = store.by_attribute(attr, args.limit)
                out.extend(rows)
                dropped += d
                truncated = truncated or t
        except StoreUnavailable as exc:
            raise IntegrationUnavailable(str(exc))
    finally:
        store.close()
    payload = {"count": len(out), "truncated": truncated, "dropped_inconsistent": dropped,
               "entries": [queries.summary_view(r) for r in out]}
    emit(args, queries.labelled(payload), _rows_text(out, truncated, with_reason=True))
    return EXIT_OK


# ---------------------------------------------------------------- text output
def _size_text(row):
    if row.get("size") is not None:
        return "%d (%s)" % (row["size"], row.get("size_basis"))
    return "unknown (listed object %s bytes)" % row.get("stored_size")


def _rows_text(rows, truncated, with_reason=False):
    lines = []
    for r in rows:
        size = r.get("size")
        cols = [r["name"], "v%s" % r["version"], "size=%s" % ("?" if size is None else size),
                r["status"]]
        if r.get("striped"):
            cols.append("striped")
        if with_reason and r.get("status_reason"):
            cols.append("(%s)" % r["status_reason"])
        cols.append("observed " + queries.iso(r["observed_at"]))
        lines.append("  ".join(cols))
    lines.append("%d entr%s%s" % (len(rows), "y" if len(rows) == 1 else "ies", " (truncated)" if truncated else ""))
    lines.append(queries.LABEL)
    return "\n".join(lines)


def _entry_text(row, name, detail=False):
    if row is None:
        return "%s: no index entry\n%s" % (name, queries.LABEL)
    lines = ["%s" % row["name"],
             "  file_id=%s version=%s status=%s%s" % (row["file_id"], row["version"], row["status"],
                                                   " (%s)" % row["status_reason"] if row["status_reason"] else ""),
             "  size: %s" % _size_text(row),
             "  striped: %s" % ("unknown" if row["striped"] is None else "%s (%s)" % (row["striped"], row["striped_basis"])),
             "  sha256: %s" % (row["sha256"] if row["sha256"] else "not recorded (verify_state=%s)" % row["verify_state"])]
    if row["sha256"]:
        lines.append("  verified_at: %s (version %s, %s bytes read)" % (
            queries.iso(row["verified_at"]), row["version"], row["verified_size"]))
    if detail:
        reps = row.get("replicas")
        if reps is None:
            lines.append("  replicas: not recorded")
        else:
            lines.append("  replicas (%s, observed %s): %s" % (
                row["replicas_source"], queries.iso(row["replicas_observed_at"]),
                ", ".join("%s=%s" % (a, "present" if p else "missing") for a, p in sorted(reps.items()))))
        lines.append("  chunk_files_seen: %s  source: %s  first_indexed: %s" % (
            row["chunk_files"], row["source"], queries.iso(row["first_indexed_at"])))
    lines.append("  observed_at: %s" % queries.iso(row["observed_at"]))
    lines.append(queries.LABEL)
    return "\n".join(lines)
