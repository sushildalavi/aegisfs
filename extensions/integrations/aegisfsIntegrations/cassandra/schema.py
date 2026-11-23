"""Versioned CQL migrations.

Files named NNN_name.cql live in extensions/integrations/cassandra/migrations. The runner applies
the ones not yet recorded in the schema_migrations table, in order, and records each one only
after all of its statements ran. An applied file that has been edited since is an error: add a new
migration instead of changing an old one.
"""
import hashlib
import os
import re
from dataclasses import dataclass

from .errors import SchemaError, StoreUnavailable
from .model import utcnow_ms

_NAME = re.compile(r"^(\d{3,})_([A-Za-z0-9_]+)\.cql$")

CREATE_MIGRATIONS = (
    "CREATE TABLE IF NOT EXISTS schema_migrations ("
    " version int PRIMARY KEY, name text, checksum text, applied_at timestamp)")


@dataclass
class Migration:
    version: int
    name: str
    path: str
    checksum: str
    statements: list


def default_migrations_dir():
    here = os.path.dirname(os.path.abspath(__file__))  # .../aegisfsIntegrations/cassandra
    return os.path.normpath(os.path.join(here, "..", "..", "cassandra", "migrations"))


def split_statements(text):
    """Split CQL text on ';' outside quotes, dropping -- comments."""
    statements, current, quote = [], [], None
    for line in text.splitlines():
        if quote is None:
            stripped = line.lstrip()
            if stripped.startswith("--"):
                continue
        i = 0
        out = []
        while i < len(line):
            ch = line[i]
            if quote:
                out.append(ch)
                if ch == quote:
                    quote = None
            elif ch in ("'", '"'):
                quote = ch
                out.append(ch)
            elif ch == "-" and line.startswith("--", i):
                break
            elif ch == ";":
                current.append("".join(out))
                statement = " ".join(" ".join(current).split())
                if statement:
                    statements.append(statement)
                current, out = [], []
            else:
                out.append(ch)
            i += 1
        current.append("".join(out))
    tail = " ".join(" ".join(current).split())
    if tail:
        statements.append(tail)
    return statements


def load_migrations(directory=None):
    directory = directory or default_migrations_dir()
    if not os.path.isdir(directory):
        raise SchemaError("migrations directory not found: %s" % directory)
    found = []
    for entry in sorted(os.listdir(directory)):
        if not entry.endswith(".cql"):
            continue
        match = _NAME.match(entry)
        if not match:
            raise SchemaError("migration file name must look like 001_name.cql: %s" % entry)
        path = os.path.join(directory, entry)
        with open(path, "rb") as handle:
            raw = handle.read()
        found.append(Migration(int(match.group(1)), match.group(2), path,
                               hashlib.sha256(raw).hexdigest(), split_statements(raw.decode("utf-8"))))
    versions = [m.version for m in found]
    if len(set(versions)) != len(versions):
        raise SchemaError("two migration files share a version number")
    if versions != list(range(1, len(versions) + 1)):
        raise SchemaError("migration versions must be 1, 2, 3 ... with no gaps (found %s)" % versions)
    return found


def migrate(session, settings, migrations=None, execute=None):
    """Apply pending migrations. Returns {"applied": [...], "already": [...], "schema_version": N}."""
    execute = execute or session.execute
    migrations = migrations if migrations is not None else load_migrations(settings.migrations_dir)
    execute(CREATE_MIGRATIONS)
    recorded = {row.version: row for row in execute("SELECT version, name, checksum FROM schema_migrations")}
    unknown = sorted(set(recorded) - {m.version for m in migrations})
    if unknown:
        raise SchemaError("the database has migrations %s that these files do not include" % unknown)
    applied, already = [], []
    for mig in migrations:
        if mig.version in recorded:
            if recorded[mig.version].checksum != mig.checksum:
                raise SchemaError("migration %03d_%s was changed after it was applied" % (mig.version, mig.name))
            already.append(mig.version)
            continue
        for statement in mig.statements:
            execute(statement)
        execute("INSERT INTO schema_migrations (version, name, checksum, applied_at) "
                "VALUES (%s, %s, %s, %s) IF NOT EXISTS",
                (mig.version, mig.name, mig.checksum, utcnow_ms()))
        applied.append(mig.version)
    return {"applied": applied, "already": already,
            "schema_version": migrations[-1].version if migrations else 0}


def keyspace_cql(settings):
    return "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = %s" % (
        settings.keyspace, settings.replication_cql())


def current_version(session):
    try:
        rows = list(session.execute("SELECT version FROM schema_migrations"))
    except Exception as exc:
        if type(exc).__name__ == "InvalidRequest":
            return 0
        raise
    return max((r.version for r in rows), default=0)


def run_migrate(settings, connector=None, migrations=None):
    """Create the keyspace if allowed, then migrate and record the bucket layout."""
    from . import connection
    connector = connector or connection.connect
    if settings.create_keyspace:
        cluster, session = connector(settings, use_keyspace=False)
        try:
            session.execute(keyspace_cql(settings))
        finally:
            cluster.shutdown()
    cluster, session = connector(settings, use_keyspace=True)
    try:
        result = migrate(session, settings, migrations)
        result["bucket_count"] = ensure_bucket_count(session, settings.bucket_count)
        result["keyspace"] = settings.keyspace
        return result
    finally:
        cluster.shutdown()


def ensure_bucket_count(session, wanted):
    """Record the bucket count on first use. Returns the value in force."""
    session.execute("INSERT INTO index_meta (key, value) VALUES ('bucket_count', %s) IF NOT EXISTS",
                    (str(wanted),))
    row = session.execute("SELECT value FROM index_meta WHERE key = 'bucket_count'").one()
    return int(row.value)
