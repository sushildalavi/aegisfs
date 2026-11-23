"""A hook other code can call after a successful AegisFS operation.

    from aegisfsIntegrations.cassandra.hook import observe
    observe("create", "reports/2026/q1.csv", spool_dir="/var/lib/aegisfs-index/spool")

It only appends a small "this file was touched" event to the local spool and returns. It never
talks to Cassandra or to AegisFS, never blocks on either, and never raises. The indexer
(`cassandra index --drain`, or `--watch`) later looks the file up in AegisFS and records what it
actually sees, so the hook cannot put wrong metadata in the index.
"""
import os

from .model import utcnow_ms, iso
from .spool import Spool

DEFAULT_ENV = "AEGISFS_INT_CASSANDRA_SPOOL_DIR"


def observe(op, name, spool_dir=None):
    """Queue a request to index `name`. Returns True if it was queued, False if it could not be
    (for example the spool directory is not writable). Never raises."""
    try:
        if not isinstance(name, str) or not name:
            return False
        directory = spool_dir or os.environ.get(DEFAULT_ENV) or ".aegisfs-index-spool"
        Spool(directory).append({"type": "touch", "op": str(op)[:32], "name": name,
                                 "event_at": iso(utcnow_ms()), "attempts": 0})
        return True
    except Exception:
        return False
