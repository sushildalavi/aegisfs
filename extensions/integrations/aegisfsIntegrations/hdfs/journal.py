"""Append-only JSON-lines journal for batch transfers.

One file per job under the state directory. Each line records one state change for one item. On
the next run the last line for an item says whether it finished. A line is flushed and fsynced
before the next step starts, and a torn last line (a crash mid-write) is ignored on load.

Only one process may run a job at a time; the job file is locked while a run is open.
"""
import fcntl
import hashlib
import json
import os
import time


class JournalBusy(Exception):
    """Another run holds this job's lock."""


def job_id(*parts):
    text = json.dumps(list(parts), sort_keys=True)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


class Journal:
    def __init__(self, state_dir, job):
        self.state_dir = state_dir
        self.job = job
        self.path = os.path.join(state_dir, "job-%s.jsonl" % job)
        self._lock = None
        self._file = None

    def open(self):
        os.makedirs(self.state_dir, mode=0o700, exist_ok=True)
        self._lock = open(self.path + ".lock", "w")
        try:
            fcntl.flock(self._lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self._lock.close()
            self._lock = None
            raise JournalBusy("another run of this job is in progress (%s)" % self.path)
        self._file = open(self.path, "a", encoding="utf-8")
        return self

    def close(self):
        if self._file:
            self._file.close()
            self._file = None
        if self._lock:
            fcntl.flock(self._lock, fcntl.LOCK_UN)
            self._lock.close()
            self._lock = None

    def __enter__(self):
        return self.open()

    def __exit__(self, *exc):
        self.close()

    def last_states(self):
        """Map item -> its most recent record."""
        out = {}
        try:
            handle = open(self.path, "r", encoding="utf-8")
        except FileNotFoundError:
            return out
        with handle:
            for line in handle:
                try:
                    record = json.loads(line)
                except ValueError:
                    continue  # a torn final line
                if isinstance(record, dict) and "item" in record:
                    out[record["item"]] = record
        return out

    def record(self, item, state, **fields):
        entry = {"ts": round(time.time(), 3), "item": item, "state": state}
        entry.update(fields)
        self._file.write(json.dumps(entry, sort_keys=True) + "\n")
        self._file.flush()
        os.fsync(self._file.fileno())
        return entry
