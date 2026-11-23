"""Export (AegisFS -> HDFS) and import (HDFS -> AegisFS) of single files, and the batch drivers.

Nothing here reports success before the bytes have been checked:

  export: read the logical file out of AegisFS, upload it to a temporary name next to the target,
          check its size, read it back from HDFS and compare SHA-256 with the AegisFS bytes, and
          only then RENAME it into place. The AegisFS source is never touched.
  import: stream the HDFS file to local disk while hashing, create it in AegisFS through the
          normal client (so striping and replication are the cluster's), then read it back
          through the client and compare SHA-256. The HDFS source is never touched.

A failed export removes its own temporary HDFS file. A failed import removes its local temp files.
AegisFS has no delete command, so an import that fails after the create cannot undo it; the error
says so.
"""
import hashlib
import os
import shutil
import tempfile
import time
import uuid

from ..aegisfsClient import AegisError, sha256_file
from . import paths
from .journal import job_id
from .webHdfs import HdfsError, HdfsExists, HdfsNotFound, HdfsUnavailable, is_temp_name


class TransferFailed(Exception):
    """A transfer did not complete. `payload` holds what is known about it."""

    def __init__(self, message, payload=None):
        super().__init__(message)
        self.payload = payload or {}


def _fail(message, exc, payload):
    """A TransferFailed for an HDFS error, marked when HDFS itself could not be reached."""
    if isinstance(exc, HdfsUnavailable):
        payload["hdfs_unavailable"] = True
    return TransferFailed(message, payload)


def _rate(nbytes, seconds):
    return round(nbytes / 1e6 / seconds, 3) if seconds > 0 else 0.0


def new_temp_path(hdfs_path):
    parent, base = paths.parent_and_base(hdfs_path)
    name = ".%s.aegis-tmp-%s" % (base, uuid.uuid4().hex[:16])
    return (parent.rstrip("/") + "/" + name) if parent != "/" else "/" + name


def _cleanup_remote_temp(hdfs, temp_path, payload):
    try:
        hdfs.delete_temp(temp_path)
    except HdfsNotFound:
        pass
    except HdfsError as exc:
        payload["temp_left_in_hdfs"] = temp_path
        payload["temp_cleanup_error"] = str(exc)


def export_file(aegis, hdfs, aegis_name, hdfs_path, overwrite=False, tmp_dir=None,
                adopt_identical=False):
    """Copy one AegisFS file to HDFS. Returns a result dict or raises TransferFailed."""
    paths.check_aegis_name(aegis_name)
    started = time.monotonic()
    payload = {"direction": "export", "source": aegis_name, "destination": hdfs_path}
    existing = None
    try:
        existing = hdfs.status(hdfs_path)
    except HdfsNotFound:
        pass
    except HdfsError as exc:
        raise _fail("cannot check the destination: %s" % exc, exc, payload)
    if existing is not None:
        if existing.get("type") == "DIRECTORY":
            raise TransferFailed("destination %s is a directory" % hdfs_path, payload)
        if not overwrite and not adopt_identical:
            raise TransferFailed("destination %s already exists (use --overwrite to replace it)"
                                 % hdfs_path, payload)

    workdir = tempfile.mkdtemp(prefix="aegis-hdfs-", dir=tmp_dir)
    temp_path = None
    try:
        local = os.path.join(workdir, "source")
        try:
            aegis.get(aegis_name, local)
        except AegisError as exc:
            raise TransferFailed("cannot read %s from AegisFS: %s" % (aegis_name, exc), payload)
        size = os.path.getsize(local)
        digest = sha256_file(local)
        payload.update(bytes=size, sha256=digest)

        if existing is not None and not overwrite:
            # Resume case: the target is already there. Accept it only if it is identical.
            try:
                remote_sha, remote_len = hdfs.sha256(hdfs_path, existing.get("length"))
            except HdfsError as exc:
                raise _fail("cannot read the existing destination: %s" % exc, exc, payload)
            if remote_sha != digest or remote_len != size:
                raise TransferFailed("destination %s already exists and differs from the "
                                     "AegisFS file (use --overwrite to replace it)" % hdfs_path,
                                     payload)
            seconds = time.monotonic() - started
            payload.update(status="OK", adopted_existing=True, seconds=round(seconds, 3),
                           mb_per_s=_rate(size, seconds))
            return payload

        temp_path = new_temp_path(hdfs_path)
        try:
            try:
                hdfs.upload(local, temp_path, size)
                info = hdfs.status(temp_path)
                if info.get("length") != size:
                    raise TransferFailed("HDFS reports %s bytes for the upload, expected %d"
                                         % (info.get("length"), size), payload)
                remote_sha, remote_len = hdfs.sha256(temp_path, size)
                if remote_len != size or remote_sha != digest:
                    raise TransferFailed("read-back from HDFS does not match the AegisFS bytes "
                                         "(got %d bytes, sha256 %s)" % (remote_len, remote_sha),
                                         payload)
                try:
                    moved = hdfs.rename(temp_path, hdfs_path, overwrite=overwrite)
                except HdfsError as exc:
                    raise _fail("could not move the verified upload into place: %s" % exc, exc,
                                payload)
                if not moved:
                    raise TransferFailed("HDFS refused to rename the verified upload to %s "
                                         "(the destination probably appeared meanwhile)"
                                         % hdfs_path, payload)
                temp_path = None
                final = hdfs.status(hdfs_path)
                if final.get("length") != size:
                    raise TransferFailed("after the rename HDFS reports %s bytes at %s, expected %d"
                                         % (final.get("length"), hdfs_path, size), payload)
            except HdfsExists as exc:
                raise _fail("destination already exists: %s" % exc, exc, payload)
            except HdfsError as exc:
                raise _fail("HDFS error: %s" % exc, exc, payload)
        except BaseException:
            if temp_path is not None:
                _cleanup_remote_temp(hdfs, temp_path, payload)
            raise
        seconds = time.monotonic() - started
        payload.update(status="OK", seconds=round(seconds, 3), mb_per_s=_rate(size, seconds))
        return payload
    except TransferFailed as exc:
        exc.payload.update({k: v for k, v in payload.items() if k not in exc.payload})
        raise
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def import_file(aegis, hdfs, hdfs_path, aegis_name, tmp_dir=None, adopt_identical=False):
    """Copy one HDFS file into AegisFS. Returns a result dict or raises TransferFailed."""
    paths.check_aegis_name(aegis_name)
    started = time.monotonic()
    payload = {"direction": "import", "source": hdfs_path, "destination": aegis_name}
    try:
        info = hdfs.status(hdfs_path)
    except HdfsNotFound:
        raise TransferFailed("HDFS source %s does not exist" % hdfs_path, payload)
    except HdfsError as exc:
        raise _fail("cannot read the HDFS source: %s" % exc, exc, payload)
    if info.get("type") != "FILE":
        raise TransferFailed("HDFS source %s is not a file" % hdfs_path, payload)
    expected = info.get("length")

    present = aegis.exists(aegis_name)
    if present and not adopt_identical:
        raise TransferFailed("AegisFS name %s already exists; imports only create new files "
                             "and never overwrite or append" % aegis_name, payload)

    workdir = tempfile.mkdtemp(prefix="aegis-hdfs-", dir=tmp_dir)
    try:
        local = os.path.join(workdir, "source")
        digest = hashlib.sha256()
        try:
            with open(local, "wb") as out:
                def sink(block):
                    out.write(block)
                    digest.update(block)
                total = hdfs.download(hdfs_path, sink, expected)
        except HdfsError as exc:
            raise _fail("reading %s from HDFS failed: %s" % (hdfs_path, exc), exc, payload)
        if total != expected:
            raise TransferFailed("read %d bytes from HDFS but it reported %s" % (total, expected),
                                 payload)
        try:
            again = hdfs.status(hdfs_path)
        except HdfsError as exc:
            raise _fail("cannot re-check the HDFS source: %s" % exc, exc, payload)
        if (again.get("length"), again.get("modificationTime")) != \
                (info.get("length"), info.get("modificationTime")):
            raise TransferFailed("the HDFS source changed while it was being read", payload)
        sha = digest.hexdigest()
        payload.update(bytes=total, sha256=sha)

        verify = os.path.join(workdir, "readback")
        if present:
            try:
                aegis.get(aegis_name, verify)
            except AegisError as exc:
                raise TransferFailed("%s exists in AegisFS but cannot be read: %s"
                                     % (aegis_name, exc), payload)
            if sha256_file(verify) != sha:
                raise TransferFailed("AegisFS name %s already exists and differs from the HDFS "
                                     "file; imports never overwrite" % aegis_name, payload)
            seconds = time.monotonic() - started
            payload.update(status="OK", adopted_existing=True, seconds=round(seconds, 3),
                           mb_per_s=_rate(total, seconds))
            return payload

        try:
            fields = aegis.create_degraded_ok(local, aegis_name)
        except AegisError as exc:
            if exc.status == "ALREADY_EXISTS":
                raise TransferFailed("AegisFS name %s already exists (created by someone else "
                                     "during the import); nothing was overwritten" % aegis_name,
                                     payload)
            raise _fail("AegisFS create failed: %s" % exc, exc, payload)
        payload["aegisfs_status"] = fields.get("status")
        try:
            aegis.get(aegis_name, verify)
        except AegisError as exc:
            payload["status"] = "UNVERIFIED"
            raise TransferFailed("the create succeeded but reading %s back failed: %s. The file "
                                 "now exists in AegisFS and this tool cannot remove it"
                                 % (aegis_name, exc), payload)
        if sha256_file(verify) != sha:
            payload["status"] = "MISMATCH"
            raise TransferFailed("the bytes read back from AegisFS differ from the HDFS source. "
                                 "The file now exists in AegisFS and this tool cannot remove it",
                                 payload)
        seconds = time.monotonic() - started
        payload.update(seconds=round(seconds, 3), mb_per_s=_rate(total, seconds))
        if fields.get("status") != "OK":
            payload["status"] = "DEGRADED"
            raise TransferFailed("imported and verified, but AegisFS committed fewer than the "
                                 "full number of replicas (status %s)" % fields.get("status"),
                                 payload)
        payload["status"] = "OK"
        return payload
    except TransferFailed as exc:
        exc.payload.update({k: v for k, v in payload.items() if k not in exc.payload})
        raise
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


# ---- batch ----

def walk_hdfs(hdfs, directory, recursive):
    """Yield (relative_path, length) for regular files, skipping our own temp files."""
    stack = [("", directory)]
    while stack:
        rel, current = stack.pop()
        for entry in sorted(hdfs.list(current), key=lambda e: e["pathSuffix"]):
            name = entry["pathSuffix"]
            if not name:
                continue
            child_rel = (rel + "/" + name) if rel else name
            if entry["type"] == "DIRECTORY":
                if recursive:
                    stack.append((child_rel, current.rstrip("/") + "/" + name))
            elif not is_temp_name(name):
                yield child_rel, entry.get("length", 0)


def _result_row(item, state, record=None, detail=""):
    record = record or {}
    return {"item": item, "state": state, "bytes": record.get("bytes"),
            "sha256": record.get("sha256"), "detail": detail}


REDO = "redo"  # verify_complete returns this when the destination is simply gone


def run_batch(journal, items, do_one, verify_complete, overwrite):
    """Run `items` (a list of (key, source, destination)) with resume.

    `do_one(source, destination, adopt)` performs one transfer. `verify_complete(destination,
    record)` re-checks something the journal calls complete. It returns None when the destination
    still matches, REDO when the destination is missing (so it is transferred again), or an error
    text when it exists but no longer matches (reported, never overwritten).
    """
    rows = []
    previous = journal.last_states()
    for key, source, destination in items:
        last = previous.get(key)
        if last and last["state"] in ("complete", "skipped") and not overwrite:
            problem = verify_complete(destination, last)
            if problem == REDO:
                last = None
            elif problem is None:
                journal.record(key, "skipped", bytes=last.get("bytes"), sha256=last.get("sha256"),
                               detail="already complete and re-verified")
                rows.append(_result_row(key, "skipped", last, "already complete, re-verified"))
                continue
            else:
                journal.record(key, "failed", detail=problem)
                rows.append(_result_row(key, "failed", None, problem))
                continue
        adopt = bool(last and last["state"] == "started")
        journal.record(key, "started", source=source, destination=destination)
        try:
            record = do_one(source, destination, adopt)
        except TransferFailed as exc:
            state = "degraded" if exc.payload.get("status") == "DEGRADED" else "failed"
            journal.record(key, state, detail=str(exc), bytes=exc.payload.get("bytes"),
                           sha256=exc.payload.get("sha256"))
            rows.append(_result_row(key, state, exc.payload, str(exc)))
            continue
        except paths.InvalidName as exc:
            journal.record(key, "failed", detail=str(exc))
            rows.append(_result_row(key, "failed", None, str(exc)))
            continue
        journal.record(key, "complete", bytes=record["bytes"], sha256=record["sha256"],
                       source=source, destination=destination)
        note = "matched an identical existing destination" if record.get("adopted_existing") else ""
        rows.append(_result_row(key, "complete", record, note))
    return rows


def export_items(aegis, names, destination_dir, prefix):
    items = []
    for name in names:
        rel = name[len(prefix):].lstrip("/") if prefix else name
        if not rel:
            rel = name.rsplit("/", 1)[-1]
        items.append((name, name, destination_dir.rstrip("/") + "/" + rel))
    return items


def batch_job_id(direction, namenode_url, source, destination):
    return job_id(direction, namenode_url, source, destination)
