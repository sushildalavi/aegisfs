"""Migrations, query-model rules and settings validation (no Cassandra needed)."""
import glob
import os
import re
import ssl
import tempfile
import unittest

import cassSupport as s
from aegisfsIntegrations.config import Config, ConfigError
from aegisfsIntegrations.cassandra import connection, schema
from aegisfsIntegrations.cassandra.errors import SchemaError
from aegisfsIntegrations.cassandra.settings import Settings


def write(path, text):
    with open(path, "w") as handle:
        handle.write(text)


def read(path):
    with open(path) as handle:
        return handle.read()


class FakeSession:
    """Records statements; remembers rows inserted into schema_migrations."""

    def __init__(self):
        self.statements = []
        self.migrations = {}

    def execute(self, statement, params=None):
        self.statements.append(statement)
        if statement.startswith("SELECT version, name, checksum FROM schema_migrations"):
            from types import SimpleNamespace
            return [SimpleNamespace(version=v, name=n, checksum=c) for v, (n, c) in self.migrations.items()]
        if statement.startswith("INSERT INTO schema_migrations"):
            version, name, checksum, _ = params
            self.migrations.setdefault(version, (name, checksum))
        return []


class StatementSplitTest(unittest.TestCase):
    def test_splits_and_drops_comments(self):
        text = "-- header\nCREATE TABLE a (x int PRIMARY KEY); -- tail\n\nCREATE TABLE b (\n  y text, -- why; not a split\n  PRIMARY KEY (y)\n);\n"
        self.assertEqual(schema.split_statements(text), [
            "CREATE TABLE a (x int PRIMARY KEY)", "CREATE TABLE b ( y text, PRIMARY KEY (y) )"])

    def test_semicolon_inside_quotes_is_kept(self):
        self.assertEqual(schema.split_statements("INSERT INTO t (a) VALUES ('x;y');"),
                         ["INSERT INTO t (a) VALUES ('x;y')"])


class MigrationFilesTest(unittest.TestCase):
    def test_shipped_migrations_load_in_order(self):
        migrations = schema.load_migrations()
        self.assertEqual([m.version for m in migrations], list(range(1, len(migrations) + 1)))
        self.assertTrue(all(m.statements for m in migrations))

    def test_every_statement_is_idempotent_ddl(self):
        for mig in schema.load_migrations():
            for statement in mig.statements:
                self.assertRegex(statement, r"^CREATE TABLE IF NOT EXISTS ", statement)

    def test_no_query_model_shortcuts_anywhere(self):
        root = os.path.join(s.INTEGRATIONS)
        files = glob.glob(os.path.join(root, "cassandra", "migrations", "*.cql")) + \
            glob.glob(os.path.join(root, "aegisfsIntegrations", "cassandra", "*.py"))
        for path in files:
            text = read(path)
            code = "\n".join(l for l in text.splitlines() if not l.strip().startswith(("#", "--", '"""')))
            self.assertNotIn("ALLOW FILTERING", code.upper().replace("ALLOW  FILTERING", "ALLOW FILTERING"), path)
            self.assertNotRegex(code.upper(), r"CREATE\s+(CUSTOM\s+)?INDEX", path)

    def test_every_table_has_a_bounded_partition_key(self):
        for mig in schema.load_migrations():
            for st in mig.statements:
                if "files_by_namespace" in st or "files_by_attr" in st:
                    self.assertRegex(st, r"PRIMARY KEY \(\((namespace|attr), bucket\), name\)")

    def test_rejects_gaps_and_bad_names(self):
        with tempfile.TemporaryDirectory() as tmp:
            write(os.path.join(tmp, "001_a.cql"), "CREATE TABLE IF NOT EXISTS a (x int PRIMARY KEY);")
            write(os.path.join(tmp, "003_c.cql"), "CREATE TABLE IF NOT EXISTS c (x int PRIMARY KEY);")
            with self.assertRaises(SchemaError):
                schema.load_migrations(tmp)
        with tempfile.TemporaryDirectory() as tmp:
            write(os.path.join(tmp, "one.cql"), "x;")
            with self.assertRaises(SchemaError):
                schema.load_migrations(tmp)


class MigrateTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = self.tmp.name
        write(os.path.join(self.dir, "001_a.cql"), "CREATE TABLE IF NOT EXISTS a (x int PRIMARY KEY);")
        write(os.path.join(self.dir, "002_b.cql"), "CREATE TABLE IF NOT EXISTS b (x int PRIMARY KEY);\nCREATE TABLE IF NOT EXISTS c (x int PRIMARY KEY);")
        self.settings = s.settings()

    def tearDown(self):
        self.tmp.cleanup()

    def test_applies_once_and_records(self):
        session = FakeSession()
        migrations = schema.load_migrations(self.dir)
        first = schema.migrate(session, self.settings, migrations)
        self.assertEqual((first["applied"], first["schema_version"]), ([1, 2], 2))
        before = len(session.statements)
        second = schema.migrate(session, self.settings, migrations)
        self.assertEqual((second["applied"], second["already"]), ([], [1, 2]))
        created = [st for st in session.statements[before:] if st.startswith("CREATE TABLE IF NOT EXISTS a")]
        self.assertEqual(created, [])

    def test_edited_migration_is_refused(self):
        session = FakeSession()
        schema.migrate(session, self.settings, schema.load_migrations(self.dir))
        write(os.path.join(self.dir, "001_a.cql"), "CREATE TABLE IF NOT EXISTS a (x int, y int PRIMARY KEY);")
        with self.assertRaises(SchemaError):
            schema.migrate(session, self.settings, schema.load_migrations(self.dir))

    def test_database_ahead_of_files_is_refused(self):
        session = FakeSession()
        schema.migrate(session, self.settings, schema.load_migrations(self.dir))
        os.unlink(os.path.join(self.dir, "002_b.cql"))
        with self.assertRaises(SchemaError):
            schema.migrate(session, self.settings, schema.load_migrations(self.dir))

    def test_keyspace_statement_uses_configured_replication(self):
        self.assertIn("'SimpleStrategy', 'replication_factor': 1", schema.keyspace_cql(self.settings))
        nts = s.settings(replication_strategy="NetworkTopologyStrategy",
                         replication_dc_factors={"dc1": 3, "dc2": 2})
        self.assertIn("'dc1': 3, 'dc2': 2", schema.keyspace_cql(nts))


class SecurityErrorTextTest(unittest.TestCase):
    """The driver reports failed handshakes as plain errors whose text has the OpenSSL reason."""

    def exc(self, error):
        from cassandra.cluster import NoHostAvailable
        return NoHostAvailable("Unable to connect to any servers", {"127.0.0.1:9042": error})

    def test_certificate_failure_as_the_driver_words_it(self):
        err = PermissionError(1, "Tried connecting to [('127.0.0.1', 9042)]. Last error: [SSL: "
                                 "CERTIFICATE_VERIFY_FAILED] certificate verify failed: self signed (_ssl.c:1129)")
        text = connection._security_problem(self.exc(err), s.settings())
        self.assertIn("certificate verification failed", text)
        self.assertNotIn("127.0.0.1", text)

    def test_login_failure_never_contains_the_password(self):
        from cassandra import AuthenticationFailed
        st = s.settings(username="u", password="hunter2-secret")
        text = connection._security_problem(self.exc(AuthenticationFailed("Bad credentials hunter2-secret")), st)
        self.assertIn("refused the login", text)
        self.assertNotIn("hunter2-secret", text)

    def test_other_failures_and_odd_shapes_are_not_security_problems(self):
        from cassandra.cluster import NoHostAvailable
        self.assertIsNone(connection._security_problem(self.exc(ConnectionRefusedError("refused")), s.settings()))
        self.assertIsNone(connection._security_problem(NoHostAvailable("x", ["127.0.0.1"]), s.settings()))


class SettingsTest(unittest.TestCase):
    def cfg(self, data=None, env=None):
        return Config(data or {}, env or {})

    def test_tls_with_verification_is_the_default(self):
        st = Settings.from_config(self.cfg({"cassandra": {"tls_ca_file": "/x"}}))
        self.assertTrue(st.tls and st.tls_verify and not st.allow_insecure)

    def test_plaintext_needs_allow_insecure(self):
        with self.assertRaises(ConfigError):
            Settings.from_config(self.cfg(env={"AEGISFS_INT_CASSANDRA_TLS": "false"}))
        ok = Settings.from_config(self.cfg(env={"AEGISFS_INT_CASSANDRA_TLS": "false",
                                                "AEGISFS_INT_CASSANDRA_ALLOW_INSECURE": "true"}))
        self.assertFalse(ok.tls)

    def test_unverified_tls_needs_allow_insecure(self):
        with self.assertRaises(ConfigError):
            Settings.from_config(self.cfg(env={"AEGISFS_INT_CASSANDRA_TLS_VERIFY": "false"}))

    def test_password_only_from_file_and_never_in_summary(self):
        with tempfile.NamedTemporaryFile("w", delete=False) as handle:
            handle.write("s3cret\n")
        try:
            st = Settings.from_config(self.cfg({"cassandra": {"username": "u", "password_file": handle.name}}))
            self.assertEqual(st.password, "s3cret")
            self.assertNotIn("s3cret", repr(st))
            self.assertNotIn("s3cret", str(st.public_summary()))
        finally:
            os.unlink(handle.name)

    def test_username_without_password_is_an_error(self):
        with self.assertRaises(ConfigError):
            Settings.from_config(self.cfg({"cassandra": {"username": "u"}}))

    def test_bad_values(self):
        for key, value in [("read_consistency", "MOST"), ("serial_consistency", "ONE"), ("keyspace", "Bad-Name"),
                           ("bucket_count", "0"), ("bucket_count", "1000"), ("replication_factor", "0"),
                           ("replication_strategy", "Magic"), ("workers", "0")]:
            with self.assertRaises(ConfigError, msg=key):
                Settings.from_config(self.cfg({"cassandra": {"tls": False, "allow_insecure": True, key: value}}))
        with self.assertRaises(ConfigError):
            Settings.from_config(self.cfg({"cassandra": {"tls": False, "allow_insecure": True,
                                                         "replication_strategy": "NetworkTopologyStrategy"}}))

    def test_env_flag_file_precedence(self):
        data = {"cassandra": {"keyspace": "from_file", "tls": False, "allow_insecure": True}}
        self.assertEqual(Settings.from_config(self.cfg(data)).keyspace, "from_file")
        env = {"AEGISFS_INT_CASSANDRA_KEYSPACE": "from_env"}
        self.assertEqual(Settings.from_config(self.cfg(data, env)).keyspace, "from_env")
        self.assertEqual(Settings.from_config(self.cfg(data, env), {"keyspace": "from_flag"}).keyspace, "from_flag")

    def test_contact_points_list(self):
        st = Settings.from_config(self.cfg({"cassandra": {"tls": False, "allow_insecure": True,
                                                          "contact_points": "a, b ,c"}}))
        self.assertEqual(st.contact_points, ["a", "b", "c"])

    def test_tls_context_verifies_by_default(self):
        st = s.settings()
        ctx = connection.build_ssl_context(st)
        self.assertEqual(ctx.verify_mode, ssl.CERT_REQUIRED)
        self.assertTrue(ctx.check_hostname)
        loose = s.settings(tls_verify=False, allow_insecure=True)
        self.assertEqual(connection.build_ssl_context(loose).verify_mode, ssl.CERT_NONE)
        self.assertIsNone(connection.build_ssl_context(s.settings(tls=False, allow_insecure=True)))


if __name__ == "__main__":
    unittest.main()
