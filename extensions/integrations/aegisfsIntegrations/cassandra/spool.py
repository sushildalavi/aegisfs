"""Durable local queue of index work that could not be sent to Cassandra yet.

The spool is a directory holding JSON-lines files:
  pending.jsonl         work waiting to be sent
  draining-<pid>-*.jsonl  the batch a drain is working on (renamed from pending.jsonl)
  rejected.jsonl        events that can never be applied, kept with the reason for a person to look at

Each event is one line, written with a single O_APPEND write and fsync under an exclusive lock, so
a crash leaves at most one torn last line, which is moved to rejected.jsonl on the next drain.
Replaying an event twice is harmless because the index merge is idempotent.
"""
import fcntl
import json
import os
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass

from .errors import StoreUnavailable

PENDING = "pending.jsonl"
REJECTED = "rejected.jsonl"
LOCK = ".lock"


@dataclass
class DrainReport:
    done: int = 0
    retried: int = 0
    rejected: int = 0
    remaining: int = 0
    stopped_early: bool = False
    error: str = ""


class Spool:
    def __init__(self, directory):
        self.dir = directory

    def _ensure(self):
        os.makedirs(self.dir, exist_ok=True)

    @contextmanager
    def _locked(self):
        self._ensure()
        fd = os.open(os.path.join(self.dir, LOCK), os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)

    def _append_lines(self, filename, lines):
        path = os.path.join(self.dir, filename)
        new_file = not os.path.exists(path)
        fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
        try:
            os.write(fd, "".join(line + "\n" for line in lines).encode("utf-8"))
            os.fsync(fd)
        finally:
            os.close(fd)
        if new_file:
            self._fsync_dir()

    def _fsync_dir(self):
        fd = os.open(self.dir, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def append(self, event):
        """Queue one event durably. Returns when the line is on disk."""
        event = dict(event)
        event.setdefault("id", uuid.uuid4().hex)
        event.setdefault("queued_at", time.time())
        line = json.dumps(event, sort_keys=True, separators=(",", ":"))
        with self._locked():
            self._append_lines(PENDING, [line])
        return event["id"]

    def _draining_files(self):
        try:
            names = os.listdir(self.dir)
        except FileNotFoundError:
            return []
        return sorted(n for n in names if n.startswith("draining-") and n.endswith(".jsonl"))

    @staticmethod
    def _count_lines(path):
        try:
            with open(path, "rb") as handle:
                return sum(1 for line in handle if line.strip())
        except FileNotFoundError:
            return 0

    def depth(self):
        """Events waiting, including a batch an interrupted drain left behind."""
        total = self._count_lines(os.path.join(self.dir, PENDING))
        for name in self._draining_files():
            total += self._count_lines(os.path.join(self.dir, name))
        return total

    def rejected_count(self):
        return self._count_lines(os.path.join(self.dir, REJECTED))

    def peek(self, limit=20):
        """First few pending events (for display)."""
        out = []
        for name in self._draining_files() + [PENDING]:
            try:
                with open(os.path.join(self.dir, name), "r", encoding="utf-8") as handle:
                    for line in handle:
                        if line.strip():
                            out.append(line.strip())
                            if len(out) >= limit:
                                return out
            except FileNotFoundError:
                continue
        return out

    def _claim_batches(self):
        """Move pending.jsonl to a private draining file and return every batch this drain owns,
        including batches left by a drain whose process no longer exists."""
        mine = []
        with self._locked():
            for name in self._draining_files():
                pid = _pid_of(name)
                if pid is not None and pid != os.getpid() and _alive(pid):
                    continue
                mine.append(name)
            pending = os.path.join(self.dir, PENDING)
            if os.path.exists(pending) and os.path.getsize(pending) > 0:
                name = "draining-%d-%d.jsonl" % (os.getpid(), time.time_ns())
                os.rename(pending, os.path.join(self.dir, name))
                self._fsync_dir()
                mine.append(name)
        return mine

    def drain(self, handler, max_events=None):
        """Call handler(event) for each queued event.

        handler returns "done", "retry" (keep it for later) or "reject" (move to rejected.jsonl),
        or raises StoreUnavailable, which stops the drain and keeps everything not yet done.
        """
        report = DrainReport()
        processed = 0
        for name in self._claim_batches():
            path = os.path.join(self.dir, name)
            with open(path, "r", encoding="utf-8", errors="replace") as handle:
                lines = [line.rstrip("\n") for line in handle if line.strip()]
            keep, reject = [], []
            stop = False
            for idx, line in enumerate(lines):
                if stop or (max_events is not None and processed >= max_events):
                    keep.append(line)
                    continue
                try:
                    event = json.loads(line)
                    if not isinstance(event, dict):
                        raise ValueError("not an object")
                except ValueError as exc:
                    reject.append(json.dumps({"reason": "unreadable line: %s" % exc, "line": line[:2000]}))
                    report.rejected += 1
                    continue
                try:
                    verdict = handler(event)
                except StoreUnavailable as exc:
                    report.stopped_early = True
                    report.error = str(exc)
                    stop = True
                    keep.append(line)
                    continue
                processed += 1
                if verdict == "done":
                    report.done += 1
                elif verdict == "retry":
                    keep.append(json.dumps(event, sort_keys=True, separators=(",", ":")))
                    report.retried += 1
                else:
                    reject.append(json.dumps({"reason": verdict if isinstance(verdict, str) else "rejected",
                                              "event": event}))
                    report.rejected += 1
            with self._locked():
                if reject:
                    self._append_lines(REJECTED, reject)
                if keep:
                    self._append_lines(PENDING, keep)
                os.unlink(path)
                self._fsync_dir()
        report.remaining = self.depth()
        return report


def _pid_of(name):
    try:
        return int(name.split("-")[1])
    except (IndexError, ValueError):
        return None


def _alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True
