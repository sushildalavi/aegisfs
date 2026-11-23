"""IndexStore: the rules for writing and reading the index, on top of a back end.

Write path (apply): validate, read the row, merge, then write it with a lightweight transaction
that only succeeds if the row still has the (version, observed_at) that was read. If another
writer got in first, read again and redo the merge. After the main row is written, the
denormalised rows (namespace listing, attribute lookup) are written with the observation time as
their write timestamp, so a late write from an older observation cannot overwrite a newer one.
"""
import threading
from dataclasses import dataclass
from typing import List, Optional

from .errors import SchemaError, StoreUnavailable
from .model import (Observation, ROOT_NAMESPACE, ancestors_of, attrs_of, merge, namespace_of,
                    utcnow_ms)

MAX_CAS_ATTEMPTS = 8
ATTRS = ("striped=true", "status=stale", "status=incomplete")
HIGH_SENTINEL = "\U0010ffff"


@dataclass
class ApplyResult:
    outcome: str  # created | updated | unchanged | rejected | ignored
    reason: Optional[str] = None
    row: Optional[dict] = None


class IndexStore:
    def __init__(self, backend, bucket_count=None):
        self.backend = backend
        stored = backend.meta("bucket_count")
        if stored is None:
            raise SchemaError("the index has no bucket_count recorded; run 'cassandra migrate'")
        self.bucket_count = int(stored)
        self.configured_bucket_count = bucket_count
        self._edges = set()
        self._edge_lock = threading.Lock()

    @classmethod
    def open(cls, settings):
        from . import connection
        from .backend import CassandraBackend
        cluster, session = connection.connect(settings)
        try:
            return cls(CassandraBackend(session, settings, cluster), settings.bucket_count)
        except Exception:
            cluster.shutdown()
            raise

    def close(self):
        self.backend.close()

    # ---- writes ----
    def _new_edges(self, namespace):
        edges = []
        with self._edge_lock:
            for edge in ancestors_of(namespace):
                if edge not in self._edges:
                    edges.append(edge)
        return edges

    def apply(self, obs):
        """Store one observation. Raises ObservationError for invalid input (never retry that)
        and StoreUnavailable when Cassandra cannot be used right now (retry later)."""
        obs.validate()
        existing = None
        for attempt in range(MAX_CAS_ATTEMPTS):
            existing = self.backend.get_row(obs.name, serial=attempt > 0)
            result = merge(existing, obs, self.bucket_count)
            if result.outcome in ("rejected", "ignored"):
                return ApplyResult(result.outcome, result.reason, existing)
            if result.outcome == "unchanged":
                # Rewrite the derived rows: harmless if present, repairs them if a crash left a gap.
                self._write_denorm(existing, existing)
                return ApplyResult("unchanged", None, existing)
            if existing is None:
                won = self.backend.insert_row(result.row)
            else:
                won = self.backend.update_row(result.row, existing)
            if won:
                self._write_denorm(result.row, existing)
                return ApplyResult(result.outcome, None, result.row)
        raise StoreUnavailable("too many concurrent writers for %r; try again" % obs.name)

    def _write_denorm(self, row, old_row):
        edges = self._new_edges(row["namespace"])
        self.backend.write_denorm(row, old_row, edges)
        with self._edge_lock:
            self._edges.update(edges)

    # ---- reads ----
    def get(self, name):
        return self.backend.get_row(name)

    def namespace_listing(self, prefix="", recursive=True, limit=100):
        """Names starting with `prefix`. Returns (summaries, truncated).

        Results are grouped by namespace (the part of the name before the last '/'): the
        namespace that holds the prefix first, then sub-namespaces in name order.
        """
        if limit < 1:
            raise ValueError("limit must be at least 1")
        base = namespace_of(prefix) if "/" in prefix else ROOT_NAMESPACE
        out: List[dict] = []
        state = {"truncated": False}

        def collect(namespace, start, end):
            rows = []
            for bucket in range(self.bucket_count):
                rows.extend(self.backend.namespace_rows(namespace, bucket, start, end, limit + 1))
            rows.sort(key=lambda r: r["name"])
            for row in rows:
                if len(out) >= limit:
                    state["truncated"] = True
                    return False
                out.append(row)
            return True

        def descend(namespace):
            if not collect(namespace, None, None):
                return False
            for child in self.backend.children(namespace):
                if not descend(child):
                    return False
            return True

        if prefix:
            if not collect(base, prefix, prefix + HIGH_SENTINEL):
                return out, True
        elif not collect(base, None, None):
            return out, True
        if recursive:
            for child in self.backend.children(base):
                if not child.startswith(prefix):
                    continue
                if not descend(child):
                    break
        return out, state["truncated"]

    def by_attribute(self, attr, limit=100):
        """Rows listed under an attribute, each re-read from the main table and dropped if the
        main row no longer has that attribute. Returns (rows, dropped, truncated)."""
        if attr not in ATTRS:
            raise ValueError("attribute must be one of %s" % ", ".join(ATTRS))
        rows, dropped, truncated = [], 0, False
        for bucket in range(self.bucket_count):
            for entry in self.backend.attr_rows(attr, bucket, limit + 1):
                full = self.backend.get_row(entry["name"])
                if full is None or attr not in attrs_of(full):
                    dropped += 1
                    continue
                if len(rows) >= limit:
                    return rows, dropped, True
                rows.append(full)
        return rows, dropped, truncated

    def count_attribute(self, attr):
        return sum(self.backend.count_attr(attr, b) for b in range(self.bucket_count))

    def count_files(self):
        """Walks the namespace tree, so cost grows with the number of namespaces. Not for hot paths."""
        total = 0
        stack = [ROOT_NAMESPACE]
        while stack:
            ns = stack.pop()
            total += sum(self.backend.count_namespace(ns, b) for b in range(self.bucket_count))
            stack.extend(self.backend.children(ns))
        return total

    def known_summaries(self):
        """Every indexed file's summary, by walking all namespaces. Used by reconcile."""
        found = {}
        stack = [ROOT_NAMESPACE]
        while stack:
            ns = stack.pop()
            for bucket in range(self.bucket_count):
                for row in self.backend.namespace_rows(ns, bucket, None, None, 10_000_000):
                    found[row["name"]] = row
            stack.extend(self.backend.children(ns))
        return found
