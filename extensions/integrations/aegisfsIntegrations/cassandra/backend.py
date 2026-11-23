"""The two storage back ends under IndexStore: Cassandra, and an in-memory one for unit tests.

Both expose the same small set of primitives, so the ordering and merge logic in store.py is
tested against the in-memory one and run against Cassandra in production.
"""
import threading

from .errors import SchemaError, StoreUnavailable
from .model import ROW_FIELDS

_ROW_COLUMNS = [c for c in ROW_FIELDS]
_SELECT_ROW = "SELECT %s FROM files_by_name WHERE name = ?" % ", ".join(_ROW_COLUMNS)
_GUARD = ("version", "observed_at")
_SUMMARY_COLUMNS = ("name", "file_id", "version", "stored_size", "size", "striped", "status",
                    "replica_count", "sha256", "verified_at", "observed_at")


def summary_of(row):
    out = {k: row.get(k) for k in _SUMMARY_COLUMNS if k != "replica_count"}
    out["replica_count"] = sum(1 for v in (row.get("replicas") or {}).values() if v) \
        if row.get("replicas") is not None else None
    return out


def _utc_micros(dt):
    from datetime import timezone
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1_000_000)


class CassandraBackend:
    def __init__(self, session, settings, cluster=None):
        from cassandra.query import UNSET_VALUE
        from cassandra import ConsistencyLevel
        self.session = session
        self.cluster = cluster
        self.settings = settings
        self._unset = UNSET_VALUE
        p = session.prepare
        try:
            self._get = p(_SELECT_ROW)
            self._get_serial = p(_SELECT_ROW)
            self._get_serial.consistency_level = getattr(ConsistencyLevel, settings.serial_consistency)
            cols = ", ".join(_ROW_COLUMNS)
            marks = ", ".join("?" for _ in _ROW_COLUMNS)
            self._insert = p("INSERT INTO files_by_name (%s) VALUES (%s) IF NOT EXISTS" % (cols, marks))
            sets = ", ".join("%s = ?" % c for c in _ROW_COLUMNS if c != "name")
            self._update = p("UPDATE files_by_name SET %s WHERE name = ? IF version = ? AND observed_at = ?" % sets)
            self._put_summary = p(
                "INSERT INTO files_by_namespace (namespace, bucket, name, file_id, version, stored_size, "
                "size, striped, status, replica_count, sha256, verified_at, observed_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?")
            self._put_edge = p("INSERT INTO namespace_children (parent, child) VALUES (?, ?)")
            self._put_attr = p("INSERT INTO files_by_attr (attr, bucket, name, file_id, version, status, "
                               "observed_at) VALUES (?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?")
            self._del_attr = p("DELETE FROM files_by_attr USING TIMESTAMP ? WHERE attr = ? AND bucket = ? "
                               "AND name = ?")
            scols = ", ".join(_SUMMARY_COLUMNS)
            base = "SELECT %s FROM files_by_namespace WHERE namespace = ? AND bucket = ?" % scols
            self._ns_all = p(base + " LIMIT ?")
            self._ns_range = p(base + " AND name >= ? AND name < ? LIMIT ?")
            self._children = p("SELECT child FROM namespace_children WHERE parent = ?")
            self._attr_rows = p("SELECT name, file_id, version, status, observed_at FROM files_by_attr "
                                "WHERE attr = ? AND bucket = ? LIMIT ?")
            self._ns_count = p("SELECT COUNT(*) FROM files_by_namespace WHERE namespace = ? AND bucket = ?")
            self._attr_count = p("SELECT COUNT(*) FROM files_by_attr WHERE attr = ? AND bucket = ?")
            self._meta_get = p("SELECT value FROM index_meta WHERE key = ?")
        except Exception as exc:
            raise _translate(exc, settings, prepare=True)

    # ---- helpers ----
    def _exec(self, stmt, params=(), profile=None):
        try:
            if profile:
                return self.session.execute(stmt, params, execution_profile=profile)
            return self.session.execute(stmt, params)
        except Exception as exc:
            raise _translate(exc, self.settings)

    def _exec_async(self, stmt, params):
        try:
            return self.session.execute_async(stmt, params)
        except Exception as exc:
            raise _translate(exc, self.settings)

    @staticmethod
    def _wait(futures, settings):
        for fut in futures:
            try:
                fut.result()
            except Exception as exc:
                raise _translate(exc, settings)

    def _row_dict(self, record):
        return {c: getattr(record, c) for c in _ROW_COLUMNS}

    def _values(self, row, prior=None):
        """Bind values, using UNSET for a null that was null before (avoids writing tombstones)."""
        out = []
        for col in _ROW_COLUMNS:
            if col == "name":
                continue
            value = row.get(col)
            if value is None and (prior is None or prior.get(col) is None):
                value = self._unset
            out.append(value)
        return out

    # ---- primitives ----
    def get_row(self, name, serial=False):
        rs = self._exec(self._get_serial if serial else self._get, (name,),
                        None if serial else "read")
        record = rs.one()
        return self._row_dict(record) if record else None

    def insert_row(self, row):
        values = [row["name"]] + self._values(row)
        rs = self._exec(self._insert, values)
        return bool(rs.one()[0])

    def update_row(self, row, expected):
        params = self._values(row, expected) + [row["name"], expected["version"], expected["observed_at"]]
        rs = self._exec(self._update, params)
        return bool(rs.one()[0])

    def write_denorm(self, row, old_row, edges):
        ts = _utc_micros(row["observed_at"])
        futures = []
        s = summary_of(row)
        futures.append(self._exec_async(self._put_summary, (
            row["namespace"], row["bucket"], row["name"], s["file_id"], s["version"], s["stored_size"],
            s["size"], s["striped"], s["status"], s["replica_count"], s["sha256"], s["verified_at"],
            s["observed_at"], ts)))
        for parent, child in edges:
            futures.append(self._exec_async(self._put_edge, (parent, child)))
        from .model import attrs_of
        new_attrs = set(attrs_of(row))
        old_attrs = set(attrs_of(old_row)) if old_row else set()
        for attr in sorted(new_attrs):
            futures.append(self._exec_async(self._put_attr, (
                attr, row["bucket"], row["name"], row["file_id"], row["version"], row["status"],
                row["observed_at"], ts)))
        for attr in sorted(old_attrs - new_attrs):
            futures.append(self._exec_async(self._del_attr, (ts, attr, row["bucket"], row["name"])))
        self._wait(futures, self.settings)

    def namespace_rows(self, namespace, bucket, start, end, limit):
        if start is None:
            rs = self._exec(self._ns_all, (namespace, bucket, limit), "read")
        else:
            rs = self._exec(self._ns_range, (namespace, bucket, start, end, limit), "read")
        out = []
        for r in rs:
            d = {c: getattr(r, c) for c in _SUMMARY_COLUMNS}
            out.append(d)
        return out

    def children(self, parent):
        return sorted(r.child for r in self._exec(self._children, (parent,), "read"))

    def attr_rows(self, attr, bucket, limit):
        return [dict(name=r.name, file_id=r.file_id, version=r.version, status=r.status,
                     observed_at=r.observed_at)
                for r in self._exec(self._attr_rows, (attr, bucket, limit), "read")]

    def count_namespace(self, namespace, bucket):
        return self._exec(self._ns_count, (namespace, bucket), "read").one()[0]

    def count_attr(self, attr, bucket):
        return self._exec(self._attr_count, (attr, bucket), "read").one()[0]

    def meta(self, key):
        r = self._exec(self._meta_get, (key,), "read").one()
        return r.value if r else None

    def ping(self):
        self._exec("SELECT release_version FROM system.local", (), "read")

    def schema_version(self):
        from .schema import current_version
        try:
            return current_version(self.session)
        except Exception as exc:
            raise _translate(exc, self.settings)

    def close(self):
        if self.cluster is not None:
            self.cluster.shutdown()


def _translate(exc, settings, prepare=False):
    """Turn driver errors into StoreUnavailable (retry later) or SchemaError (needs migrate)."""
    from ..config import redact
    name = type(exc).__name__
    text = redact(str(exc), [settings.password])
    transient = {"NoHostAvailable", "OperationTimedOut", "Unavailable", "ReadTimeout", "WriteTimeout",
                 "ReadFailure", "WriteFailure", "CoordinationFailure", "ConnectionShutdown",
                 "ConnectionException", "ConnectionBusy", "OverloadedErrorMessage", "Timeout",
                 "ServerError", "IsBootstrappingErrorMessage", "TruncateError"}
    if name in transient:
        return StoreUnavailable("%s: %s" % (name, text))
    if name in ("InvalidRequest", "AlreadyExists", "SyntaxException") and prepare:
        return SchemaError("the schema is not ready (%s); run 'cassandra migrate'" % text)
    return exc


class MemoryBackend:
    """In-memory stand-in with the same primitives. Used by unit tests only.

    `fail` can be set to an exception to simulate an outage on the next call.
    """

    def __init__(self):
        self.rows = {}
        self.summary = {}   # (namespace, bucket) -> {name: dict}
        self.edges = {}     # parent -> set(child)
        self.attrs = {}     # (attr, bucket) -> {name: dict}
        self.meta_values = {"bucket_count": "16"}
        self.fail = None
        self.calls = []
        self.lock = threading.Lock()

    def _check(self, what):
        self.calls.append(what)
        if self.fail is not None:
            raise self.fail

    def get_row(self, name, serial=False):
        self._check("get_row")
        row = self.rows.get(name)
        return dict(row) if row else None

    def insert_row(self, row):
        self._check("insert_row")
        with self.lock:
            if row["name"] in self.rows:
                return False
            self.rows[row["name"]] = dict(row)
            return True

    def update_row(self, row, expected):
        self._check("update_row")
        with self.lock:
            cur = self.rows.get(row["name"])
            if cur is None or (cur["version"], cur["observed_at"]) != (expected["version"], expected["observed_at"]):
                return False
            self.rows[row["name"]] = dict(row)
            return True

    def write_denorm(self, row, old_row, edges):
        self._check("write_denorm")
        from .model import attrs_of
        with self.lock:
            self.summary.setdefault((row["namespace"], row["bucket"]), {})[row["name"]] = summary_of(row)
            for parent, child in edges:
                self.edges.setdefault(parent, set()).add(child)
            new_attrs = set(attrs_of(row))
            old_attrs = set(attrs_of(old_row)) if old_row else set()
            for attr in new_attrs:
                self.attrs.setdefault((attr, row["bucket"]), {})[row["name"]] = dict(
                    name=row["name"], file_id=row["file_id"], version=row["version"],
                    status=row["status"], observed_at=row["observed_at"])
            for attr in old_attrs - new_attrs:
                self.attrs.get((attr, row["bucket"]), {}).pop(row["name"], None)

    def namespace_rows(self, namespace, bucket, start, end, limit):
        self._check("namespace_rows")
        rows = self.summary.get((namespace, bucket), {})
        names = sorted(n for n in rows if start is None or start <= n < end)
        return [dict(rows[n]) for n in names[:limit]]

    def children(self, parent):
        self._check("children")
        return sorted(self.edges.get(parent, ()))

    def attr_rows(self, attr, bucket, limit):
        self._check("attr_rows")
        rows = self.attrs.get((attr, bucket), {})
        return [dict(rows[n]) for n in sorted(rows)[:limit]]

    def count_namespace(self, namespace, bucket):
        return len(self.summary.get((namespace, bucket), {}))

    def count_attr(self, attr, bucket):
        return len(self.attrs.get((attr, bucket), {}))

    def meta(self, key):
        return self.meta_values.get(key)

    def ping(self):
        self._check("ping")

    def schema_version(self):
        self._check("schema_version")
        return 2

    def close(self):
        pass
