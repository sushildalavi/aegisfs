"""Reading the index, and checking an index entry against live AegisFS state."""
import os
import tempfile

from ..aegisfsClient import AegisError, sha256_file
from .indexer import lookup_one
from .model import iso, utcnow_ms

LABEL = ("Index observation, not authoritative. It records what AegisFS reported at observed_at; "
         "AegisFS is the source of truth and an entry here never overrides an AegisFS error.")


def labelled(payload, observed_at=None):
    payload = dict(payload)
    payload["index"] = {"authoritative": False, "note": LABEL}
    if observed_at is not None:
        payload["index"]["observed_at"] = iso(observed_at)
    return payload


def row_view(row):
    """A stored row as plain JSON-able values. Fields AegisFS did not report stay null."""
    if row is None:
        return None
    out = {}
    for key, value in row.items():
        if hasattr(value, "strftime"):
            value = iso(value)
        out[key] = value
    return out


def summary_view(row):
    out = row_view(row)
    out.pop("verified_at", None) if out.get("sha256") is None else None
    return out


def confirm(aegis, row, settings, read=False, tmp_dir=None):
    """Compare an index row with what AegisFS says now.

    Returns a dict with `agrees`, `differences`, `aegisfs_error` (set if AegisFS reported a
    failure) and the live values. A failure reported by AegisFS is reported as such: it is never
    softened because the index has an entry.
    """
    result = {"checked_at": iso(utcnow_ms()), "agrees": False, "differences": [], "aegisfs_error": None,
              "live": None, "read_check": None}
    try:
        live = lookup_one(aegis, row["name"], settings, utcnow_ms(), "confirm")
    except AegisError as exc:
        result["aegisfs_error"] = str(exc)
        result["differences"].append("AegisFS could not be asked: %s" % exc)
        return result
    if live is None:
        result["differences"].append("AegisFS reports no replica holding this file")
        return result
    result["live"] = {"file_id": live.file_id, "version": live.version, "stored_size": live.stored_size,
                      "replicas": live.replicas, "status": live.status}
    diffs = result["differences"]
    if live.version != row["version"]:
        diffs.append("version: index %s, AegisFS %s" % (row["version"], live.version))
    if row["file_id"] and live.file_id != row["file_id"]:
        diffs.append("file_id: index %s, AegisFS %s" % (row["file_id"], live.file_id))
    if live.stored_size != row["stored_size"]:
        diffs.append("stored_size: index %s, AegisFS %s" % (row["stored_size"], live.stored_size))
    if row.get("replicas") is not None:
        mine = {a for a, present in row["replicas"].items() if present}
        theirs = {a for a, present in live.replicas.items() if present}
        if mine != theirs:
            diffs.append("replicas holding the file: index %s, AegisFS %s" % (sorted(mine), sorted(theirs)))
    if read:
        result["read_check"] = _read_check(aegis, row, live, tmp_dir)
        if result["read_check"].get("aegisfs_error"):
            result["aegisfs_error"] = result["read_check"]["aegisfs_error"]
            diffs.append("AegisFS could not read the file: %s" % result["aegisfs_error"])
        elif result["read_check"].get("matches_index_checksum") is False:
            diffs.append("sha256 of the bytes AegisFS returned differs from the indexed checksum")
    result["agrees"] = not diffs
    return result


def _read_check(aegis, row, live, tmp_dir):
    fd, path = tempfile.mkstemp(prefix="aegis-confirm-", dir=tmp_dir)
    os.close(fd)
    try:
        try:
            aegis.get(row["name"], path)
        except AegisError as exc:
            return {"aegisfs_error": str(exc)}
        digest = sha256_file(path)
        out = {"sha256": digest, "size": os.path.getsize(path), "matches_index_checksum": None}
        if row.get("sha256") and live.version == row["version"]:
            out["matches_index_checksum"] = (digest == row["sha256"])
        return out
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass
