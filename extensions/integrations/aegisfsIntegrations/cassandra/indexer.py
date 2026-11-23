"""Turning what AegisFS reports into index observations, and keeping the index in step.

Nothing in this module changes anything in AegisFS. It reads inventory and file contents through
the AegisClient and writes only to the index (or to the local spool when the index is down).
"""
import os
import tempfile
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

from ..aegisfsClient import AegisError, is_stripe_chunk, sha256_file
from .errors import StoreUnavailable
from .model import (Observation, ObservationError, STATUS_INCOMPLETE, STATUS_INDEXED, STATUS_STALE,
                    SIZE_LISTSTORE, SIZE_READ, STRIPED_CHUNKS, STRIPED_VERIFIED, VERIFY_FAILED,
                    VERIFY_SKIPPED, parent_of_chunk, utcnow_ms)


# ---------------------------------------------------------------- building observations
def split_inventory(entries):
    """Separate real files from stripe chunk files.

    Returns (files, chunk_count, chunk_bytes, orphan_chunks) where the chunk maps are keyed by the
    name of the file the chunks belong to, and orphan_chunks counts chunk files whose index file
    is not in the inventory.
    """
    files = {}
    chunk_count = defaultdict(int)
    chunk_bytes = defaultdict(int)
    chunks = []
    for name, entry in entries.items():
        if is_stripe_chunk(name):
            chunks.append(entry)
        else:
            files[name] = entry
    orphans = 0
    for chunk in chunks:
        parent = parent_of_chunk(chunk.name)
        if parent in files:
            chunk_count[parent] += 1
            chunk_bytes[parent] += chunk.size
        else:
            orphans += 1
    return files, chunk_count, chunk_bytes, orphans


def observation_from_entry(entry, settings, now, source, chunk_files=0, unreachable=0):
    """Observation for a file seen in a full inventory scan."""
    replicas = {addr: True for addr in sorted(set(entry.seen_on))}
    obs = Observation(
        name=entry.name, observed_at=now, source=source, file_id=entry.file_id, version=entry.version,
        stored_size=entry.size, replicas=replicas, replicas_source="listStore",
        chunk_files=chunk_files)
    if chunk_files > 0:
        obs.striped, obs.striped_basis = True, STRIPED_CHUNKS
    elif unreachable == 0:
        # No stripe chunks were listed anywhere, so the listed size is taken as the file size.
        # This is a listing, not a read: an index whose chunks are all missing would look the same.
        obs.size, obs.size_basis = entry.size, SIZE_LISTSTORE
    if len(replicas) < settings.expected_replicas:
        obs.status = STATUS_INCOMPLETE
        obs.status_reason = "listed on %d of %d expected replicas" % (len(replicas), settings.expected_replicas)
        if unreachable:
            obs.status_reason += " (%d node(s) did not answer the scan)" % unreachable
    return obs


def absent_observation(name, now, source, partial_scan):
    if partial_scan:
        return Observation(name=name, kind="absent", observed_at=now, source=source,
                           status=STATUS_INCOMPLETE,
                           status_reason="not listed, but some nodes did not answer the scan")
    return Observation(name=name, kind="absent", observed_at=now, source=source, status=STATUS_STALE,
                       status_reason="not listed by any node in the latest complete scan")


def lookup_one(aegis, name, settings, now, source):
    """Observation for one file using `ls` (designated replicas and whether each holds it) and
    listStore on the replicas that hold it. Returns None if no replica holds the file."""
    replicas_info = aegis.placement(name)
    best = None
    chunk_files = 0
    for rep in replicas_info:
        if not rep.present:
            continue
        try:
            listing = aegis.list_store(rep.address)
        except AegisError:
            continue
        for entry in listing:
            if entry.name == name and (best is None or (entry.version, entry.size) > (best.version, best.size)):
                best = entry
            elif entry.name.startswith(name) and is_stripe_chunk(entry.name) and \
                    parent_of_chunk(entry.name) == name:
                chunk_files += 1
    if best is None:
        return None
    replicas = {r.address: bool(r.present) for r in replicas_info if r.address}
    obs = Observation(name=name, observed_at=now, source=source, file_id=best.file_id,
                      version=best.version, stored_size=best.size, replicas=replicas,
                      replicas_source="ls", chunk_files=chunk_files or None)
    if chunk_files:
        obs.striped, obs.striped_basis = True, STRIPED_CHUNKS
    present = sum(1 for v in replicas.values() if v)
    if present < settings.expected_replicas:
        obs.status = STATUS_INCOMPLETE
        obs.status_reason = "%d of %d expected replicas hold the file" % (present, settings.expected_replicas)
    return obs


# ---------------------------------------------------------------- checksum verification
def verify_observation(aegis, obs, max_bytes, estimated_size, tmp_dir=None):
    """Read the file from AegisFS, hash it, and return an observation carrying the checksum.

    Returns (observation, note). If the file is too big, cannot be read, or changed while being
    read, the returned observation carries no checksum and verify_state says why.
    """
    probe = Observation(**{**obs.__dict__})
    if max_bytes is not None and estimated_size is not None and estimated_size > max_bytes:
        probe.verify_state = VERIFY_SKIPPED
        return probe, "about %d bytes is over --max-bytes %d" % (estimated_size, max_bytes)
    fd, path = tempfile.mkstemp(prefix="aegis-verify-", dir=tmp_dir)
    os.close(fd)
    try:
        try:
            fields = aegis.get(obs.name, path)
        except AegisError as exc:
            probe.verify_state = VERIFY_FAILED
            return probe, "read failed: %s" % exc
        digest = sha256_file(path)
        size = os.path.getsize(path)
        reported = fields.get("sha256")
        if reported and reported != digest:
            probe.verify_state = VERIFY_FAILED
            return probe, "checksum reported by the client (%s) differs from the file written" % reported
        if max_bytes is not None and size > max_bytes:
            probe.verify_state = VERIFY_SKIPPED
            return probe, "file turned out to be %d bytes, over --max-bytes" % size
        # Make sure the version did not move while the file was being read.
        try:
            after = lookup_one(aegis, obs.name, _Defaults, utcnow_ms(), "verify")
        except AegisError:
            after = None
        if after is None or after.version != obs.version:
            probe.verify_state = VERIFY_FAILED
            return probe, "file changed while it was being read; checksum discarded"
        striped = "stripes" in fields
        probe.striped, probe.striped_basis = striped, STRIPED_VERIFIED
        probe.size, probe.size_basis = size, SIZE_READ
        probe.sha256, probe.verified_size = digest, size
        probe.verified_at = utcnow_ms()
        probe.verify_state = "verified"
        return probe, "ok"
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass


class _Defaults:
    expected_replicas = 1


# ---------------------------------------------------------------- submitting
def submit(store, spool, obs):
    """Write an observation to the index, or queue it if the index cannot be reached.

    Returns the outcome: created/updated/unchanged/rejected/ignored, or "spooled".
    Raises ObservationError for an invalid observation (that is a bug in the caller; it is not queued).
    """
    obs.validate()
    if store is not None:
        try:
            return store.apply(obs).outcome
        except StoreUnavailable:
            pass
    spool.append({"type": "observation", "obs": obs.to_json()})
    return "spooled"


# ---------------------------------------------------------------- drain
def drain(store, spool, aegis, settings, max_events=None):
    """Replay queued events. Returns (DrainReport, details)."""
    cache = {"inv": None}
    details = {"created": 0, "updated": 0, "unchanged": 0, "rejected_older": 0, "ignored": 0}

    def inventory():
        if cache["inv"] is None:
            entries, unreachable = aegis.inventory()
            files, counts, bytes_, _ = split_inventory(entries)
            cache["inv"] = (files, counts, len(unreachable))
        return cache["inv"]

    def tally(outcome):
        key = {"rejected": "rejected_older"}.get(outcome, outcome)
        details[key] = details.get(key, 0) + 1

    def handle(event):
        kind = event.get("type")
        if kind == "observation":
            try:
                obs = Observation.from_json(event.get("obs"))
                result = store.apply(obs)
            except ObservationError as exc:
                return "invalid observation: %s" % exc
            tally(result.outcome)
            return "done"
        if kind == "touch":
            name = event.get("name")
            if not isinstance(name, str) or not name:
                return "touch event without a name"
            if is_stripe_chunk(name):
                name = parent_of_chunk(name)
            try:
                files, counts, unreachable = inventory()
            except AegisError:
                return "retry"
            entry = files.get(name)
            if entry is None:
                event["attempts"] = int(event.get("attempts", 0)) + 1
                if event["attempts"] >= 5:
                    return "AegisFS did not list this file after 5 attempts"
                return "retry"
            obs = observation_from_entry(entry, settings, utcnow_ms(), "hook:%s" % event.get("op", "event"),
                                         counts.get(name, 0), unreachable)
            try:
                result = store.apply(obs)
            except ObservationError as exc:
                return "invalid observation: %s" % exc
            tally(result.outcome)
            return "done"
        return "unknown event type %r" % (kind,)

    report = spool.drain(handle, max_events)
    return report, details


# ---------------------------------------------------------------- reconcile
def _summary_matches(known, obs):
    replica_count = sum(1 for v in (obs.replicas or {}).values() if v) if obs.replicas is not None else None
    return (known["version"] == obs.version and known["stored_size"] == obs.stored_size
            and known["size"] == obs.size and known["status"] == obs.status
            and known["striped"] == obs.striped and known["replica_count"] == replica_count)


def reconcile(store, spool, aegis, settings, verify=False, max_bytes=None, progress=None):
    """Compare the full AegisFS inventory with the index and bring the index up to date.

    Files AegisFS no longer lists are marked stale (or incomplete when some nodes did not answer),
    never dropped. Returns a report dict.
    """
    started = time.time()
    report = {"drained": None, "scanned_files": 0, "chunk_files_seen": 0, "orphan_chunk_files": 0,
              "created": 0, "updated": 0, "unchanged": 0, "skipped_newer": 0, "marked_stale": 0,
              "marked_incomplete_scan": 0, "already_missing": 0, "incomplete_files": 0,
              "verified": 0, "verify_skipped": 0, "verify_failed": 0, "invalid": 0, "spooled": 0,
              "unreachable_nodes": [], "problems": []}
    if spool.depth():
        drained, _ = drain(store, spool, aegis, settings)
        report["drained"] = {"done": drained.done, "retried": drained.retried,
                             "rejected": drained.rejected, "remaining": drained.remaining}
    entries, unreachable = aegis.inventory()
    report["unreachable_nodes"] = list(unreachable)
    files, counts, chunk_bytes, orphans = split_inventory(entries)
    report["scanned_files"] = len(files)
    report["chunk_files_seen"] = len(entries) - len(files)
    report["orphan_chunk_files"] = orphans
    known = store.known_summaries()
    now = utcnow_ms()
    work = []
    for name, entry in files.items():
        obs = observation_from_entry(entry, settings, now, "reconcile", counts.get(name, 0), len(unreachable))
        prev = known.get(name)
        want_verify = verify and (prev is None or prev["version"] != obs.version or prev["sha256"] is None)
        if prev is not None and _summary_matches(prev, obs) and not want_verify:
            report["unchanged"] += 1
            if obs.status == STATUS_INCOMPLETE:
                report["incomplete_files"] += 1
            continue
        estimate = chunk_bytes[name] if counts.get(name) else entry.size
        work.append((obs, want_verify, estimate))
    for name, prev in known.items():
        if name in files:
            continue
        if prev["status"] == STATUS_STALE and not unreachable:
            report["already_missing"] += 1
            continue
        if prev["status"] == STATUS_INCOMPLETE and unreachable:
            report["already_missing"] += 1
            continue
        work.append((absent_observation(name, now, "reconcile", bool(unreachable)), False, None))

    def run(item):
        obs, want_verify, estimate = item
        note = None
        try:
            if want_verify:
                obs, note = verify_observation(aegis, obs, max_bytes, estimate)
            return obs, submit(store, spool, obs), note
        except ObservationError as exc:
            return obs, "invalid", str(exc)

    with ThreadPoolExecutor(max_workers=settings.workers) as pool:
        for obs, outcome, note in pool.map(run, work):
            if obs.kind == "absent":
                if outcome in ("updated", "created"):
                    report["marked_stale" if obs.status == STATUS_STALE else "marked_incomplete_scan"] += 1
                elif outcome == "rejected":
                    report["skipped_newer"] += 1
            elif outcome == "invalid":
                report["invalid"] += 1
                report["problems"].append("%s: %s" % (obs.name, note))
            elif outcome == "spooled":
                report["spooled"] += 1
            elif outcome == "rejected":
                report["skipped_newer"] += 1
            elif outcome in ("created", "updated", "unchanged"):
                report[outcome] += 1
            if obs.kind == "present":
                if obs.status == STATUS_INCOMPLETE:
                    report["incomplete_files"] += 1
                if obs.verify_state == "verified":
                    report["verified"] += 1
                elif obs.verify_state == VERIFY_SKIPPED:
                    report["verify_skipped"] += 1
                elif obs.verify_state == VERIFY_FAILED:
                    report["verify_failed"] += 1
                    report["problems"].append("%s: %s" % (obs.name, note))
    report["seconds"] = round(time.time() - started, 3)
    return report
