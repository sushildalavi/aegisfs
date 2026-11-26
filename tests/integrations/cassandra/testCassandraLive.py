"""Live tests: a real Cassandra container and a real (process-local) AegisFS cluster.

These skip with a message when Cassandra (or AegisFS) is not reachable. Set
AEGISFS_INT_CASSANDRA_LIVE=1 to turn a missing service into a failure instead of a skip.

Environment (the defaults match tests/integrations/cassandra/runLive.sh):
  AEGISFS_INT_CASSANDRA_CONTACT_POINTS (127.0.0.1)   AEGISFS_INT_CASSANDRA_PORT (9042)
  AEGISFS_INT_AEGISFS_ENTRY (127.0.0.1:9300)         AEGISFS_INT_AEGISFS_BIN (build/aegisfs)
  AEGIS_CASS_RUN_DIR (/tmp/aegis-cass)  AEGIS_CASS_BASE_PORT (9300)  AEGIS_CASS_NODES (3)
  AEGIS_CASS_CONTAINER (aegis-cass-cassandra-1)
The TLS variables AEGISFS_TLS_CA / AEGISFS_TOKEN_FILE must be exported for the aegisfs client.
Every run creates its own keyspace (aegis_it_<random>) and drops it afterwards.

Secure mode: set AEGIS_CASS_SECURE_DIR to the directory made by deploy/docker/cassandra/secureCreds.sh
and point the contact point at the Cassandra started with `cassandraUp.sh --secure`. Then the whole
suite runs over TLS with a password login, and the tests in SecureTest also run.
"""
import hashlib
import io
import json
import os
import random
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid
from contextlib import redirect_stderr, redirect_stdout

import cassSupport as s
from aegisfsIntegrations import cli
from aegisfsIntegrations.aegisfsClient import AegisClient, AegisError
from aegisfsIntegrations.config import Config
from aegisfsIntegrations.cassandra import indexer, model as m, schema
from aegisfsIntegrations.cassandra.errors import DriverMissing, SecurityError, StoreUnavailable
from aegisfsIntegrations.cassandra.settings import Settings
from aegisfsIntegrations.cassandra.spool import Spool
from aegisfsIntegrations.cassandra.store import IndexStore

REQUIRE = os.environ.get("AEGISFS_INT_CASSANDRA_LIVE") == "1"
ENTRY = os.environ.get("AEGISFS_INT_AEGISFS_ENTRY", "127.0.0.1:9300")
BIN = os.environ.get("AEGISFS_INT_AEGISFS_BIN", os.path.join(s.ROOT, "build", "aegisfs"))
RUN_DIR = os.environ.get("AEGIS_CASS_RUN_DIR", "/tmp/aegis-cass")
BASE_PORT = os.environ.get("AEGIS_CASS_BASE_PORT", "9300")
NODES = int(os.environ.get("AEGIS_CASS_NODES", "3"))
CONTAINER = os.environ.get("AEGIS_CASS_CONTAINER", "aegis-cass-cassandra-1")
KEYSPACE = "aegis_it_" + uuid.uuid4().hex[:8]
RUN = uuid.uuid4().hex[:8]  # prefix for file names so runs never collide

SECURE_DIR = os.environ.get("AEGIS_CASS_SECURE_DIR") or None
SERVER_NAME = os.environ.get("AEGISFS_INT_CASSANDRA_TLS_SERVER_NAME") or None  # when the contact point is a DNS name
STATE = {"cassandra": None, "aegis": None, "reason": ""}


def secure_values():
    """Settings that use TLS and the password login of the secure test container."""
    if not SECURE_DIR:
        return {}
    return dict(tls=True, allow_insecure=False, tls_ca_file=os.path.join(SECURE_DIR, "ca.pem"),
                username="aegis_index", password=slurp(os.path.join(SECURE_DIR, "index.pass")).decode().strip(),
                tls_server_name=SERVER_NAME)


def base_settings(**kw):
    values = dict(contact_points=[os.environ.get("AEGISFS_INT_CASSANDRA_CONTACT_POINTS", "127.0.0.1")],
                  port=int(os.environ.get("AEGISFS_INT_CASSANDRA_PORT", "9042")), keyspace=KEYSPACE,
                  tls=False, allow_insecure=True, workers=8, request_timeout=10.0, connect_timeout=3.0)
    values.update(secure_values())
    values.update(kw)
    return s.settings(**values)


def aegis_client(**kw):
    return AegisClient(binary=BIN, entry=ENTRY, **kw)


def setUpModule():
    try:
        schema.run_migrate(base_settings())
        STATE["cassandra"] = True
    except (StoreUnavailable, DriverMissing, SecurityError) as exc:
        STATE["cassandra"] = False
        STATE["reason"] = str(exc)
    if STATE["cassandra"]:
        try:
            STATE["aegis"] = aegis_client().health()
        except OSError:  # the binary cannot run here (for example a macOS binary inside Linux)
            STATE["aegis"] = False
    if REQUIRE and not STATE["cassandra"]:
        raise RuntimeError("AEGISFS_INT_CASSANDRA_LIVE=1 but Cassandra is not reachable: %s" % STATE["reason"])
    if REQUIRE and not STATE["aegis"]:
        raise RuntimeError("AEGISFS_INT_CASSANDRA_LIVE=1 but the AegisFS cluster at %s is not healthy" % ENTRY)


def tearDownModule():
    if STATE["cassandra"]:
        try:
            from aegisfsIntegrations.cassandra import connection
            cluster, session = connection.connect(base_settings(), use_keyspace=False)
            session.execute("DROP KEYSPACE IF EXISTS %s" % KEYSPACE)
            cluster.shutdown()
        except Exception as exc:  # leave it; it is a throwaway keyspace
            print("could not drop keyspace %s: %s" % (KEYSPACE, exc), file=sys.stderr)


class LiveCase(unittest.TestCase):
    needs_aegis = False

    def setUp(self):
        if not STATE["cassandra"]:
            self.skipTest("Cassandra is not reachable (%s); run tests/integrations/cassandra/runLive.sh "
                          "or set AEGISFS_INT_CASSANDRA_LIVE=1 to require it" % STATE["reason"])
        if self.needs_aegis and not STATE["aegis"]:
            self.skipTest("the AegisFS cluster at %s is not healthy" % ENTRY)
        self.tmp = tempfile.mkdtemp(prefix="aegis-cass-test-")
        self.spool = Spool(os.path.join(self.tmp, "spool"))
        self.settings = base_settings(spool_dir=os.path.join(self.tmp, "spool"))
        self.store = IndexStore.open(self.settings)
        self.names = "t%s/%s/" % (RUN, uuid.uuid4().hex[:6])  # a private namespace per test

    def tearDown(self):
        if getattr(self, "store", None):
            self.store.close()
        shutil.rmtree(getattr(self, "tmp", "/nonexistent"), ignore_errors=True)

    def name(self, leaf):
        return self.names + leaf

    def file(self, size, seed=None):
        path = os.path.join(self.tmp, uuid.uuid4().hex)
        rng = random.Random(seed)
        with open(path, "wb") as handle:
            handle.write(bytes(rng.getrandbits(8) for _ in range(min(size, 4096))) * (size // 4096 + 1))
            handle.truncate(size)
        return path

    def run_cli(self, *argv):
        env = {"AEGISFS_INT_CASSANDRA_CONTACT_POINTS": ",".join(self.settings.contact_points),
               "AEGISFS_INT_CASSANDRA_PORT": str(self.settings.port),
               "AEGISFS_INT_CASSANDRA_KEYSPACE": KEYSPACE, "AEGISFS_INT_CASSANDRA_TLS": "false",
               "AEGISFS_INT_CASSANDRA_ALLOW_INSECURE": "true",
               "AEGISFS_INT_CASSANDRA_SPOOL_DIR": self.settings.spool_dir,
               "AEGISFS_INT_AEGISFS_BIN": BIN, "AEGISFS_INT_AEGISFS_ENTRY": ENTRY}
        if SECURE_DIR:
            env.update({"AEGISFS_INT_CASSANDRA_TLS": "true", "AEGISFS_INT_CASSANDRA_ALLOW_INSECURE": "false",
                        "AEGISFS_INT_CASSANDRA_TLS_CA_FILE": os.path.join(SECURE_DIR, "ca.pem"),
                        "AEGISFS_INT_CASSANDRA_USERNAME": "aegis_index",
                        "AEGISFS_INT_CASSANDRA_PASSWORD_FILE": os.path.join(SECURE_DIR, "index.pass")})
            if SERVER_NAME:
                env["AEGISFS_INT_CASSANDRA_TLS_SERVER_NAME"] = SERVER_NAME
        saved = {k: os.environ.get(k) for k in env}
        os.environ.update(env)
        out, err = io.StringIO(), io.StringIO()
        try:
            with redirect_stdout(out), redirect_stderr(err):
                code = cli.main(["--json", *argv])
        finally:
            for k, v in saved.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v
        text = out.getvalue()
        try:
            data = json.loads(text) if text.strip() else None
        except ValueError:
            data = None
        return code, data, err.getvalue()


class MigrationTest(LiveCase):
    def test_migrations_applied_and_idempotent(self):
        again = schema.run_migrate(self.settings)
        self.assertEqual(again["applied"], [])
        self.assertEqual(again["schema_version"], 2)
        self.assertEqual(self.store.backend.schema_version(), 2)
        rows = self.store.backend.session.execute(
            "SELECT table_name FROM system_schema.tables WHERE keyspace_name = %s", (KEYSPACE,))
        tables = {r.table_name for r in rows}
        self.assertTrue({"files_by_name", "files_by_namespace", "namespace_children", "files_by_attr",
                         "index_meta", "schema_migrations"} <= tables, tables)

    def test_bucket_count_is_fixed_after_first_use(self):
        other = base_settings(bucket_count=32)
        result = schema.run_migrate(other)
        self.assertEqual(result["bucket_count"], 16)

    def test_status_command_reports_readiness(self):
        code, data, _ = self.run_cli("cassandra", "status")
        self.assertEqual(code, 0)
        self.assertTrue(data["connected"])
        self.assertEqual(data["schema_version"], 2)
        self.assertFalse(data["index"]["authoritative"])

    def test_missing_keyspace_is_a_clear_error(self):
        code, _, err = self.run_cli("cassandra", "status", "--keyspace", "aegis_it_nope")
        self.assertNotEqual(code, 0)
        self.assertIn("migrate", err)

    def test_wrong_port_is_unavailable(self):
        code, _, err = self.run_cli("cassandra", "status", "--port", "9")
        self.assertEqual(code, cli.EXIT_UNAVAILABLE)
        self.assertIn("no Cassandra node answered", err)


class PlaintextServerTest(LiveCase):
    """Only meaningful against the plaintext container (default mode)."""

    def setUp(self):
        if SECURE_DIR:
            self.skipTest("secure mode: the server requires TLS")
        super().setUp()

    def test_tls_required_does_not_fall_back_to_plaintext(self):
        strict = base_settings(tls=True, allow_insecure=False, tls_ca_file=None)
        with self.assertRaises((SecurityError, StoreUnavailable)):
            IndexStore.open(strict)
        # and no session was silently opened in plaintext
        from aegisfsIntegrations.cassandra import connection
        with self.assertRaises((SecurityError, StoreUnavailable)):
            connection.connect(strict)

    def test_plaintext_needs_the_explicit_insecure_switch(self):
        from aegisfsIntegrations.config import ConfigError
        for env in ({"AEGISFS_INT_CASSANDRA_TLS": "false"},
                    {"AEGISFS_INT_CASSANDRA_TLS": "false", "AEGISFS_INT_CASSANDRA_ALLOW_INSECURE": "false"}):
            with self.assertRaises(ConfigError):
                Settings.from_config(Config({}, env))
        ok = Settings.from_config(Config({}, {"AEGISFS_INT_CASSANDRA_TLS": "false",
                                              "AEGISFS_INT_CASSANDRA_ALLOW_INSECURE": "true",
                                              "AEGISFS_INT_CASSANDRA_KEYSPACE": KEYSPACE}))
        IndexStore.open(ok).close()
        # the command line refuses too, before it connects to anything
        code, _, err = self.run_cli_env({"AEGISFS_INT_CASSANDRA_TLS": "false",
                                         "AEGISFS_INT_CASSANDRA_ALLOW_INSECURE": "false"}, "cassandra", "status")
        self.assertEqual(code, cli.EXIT_USAGE)
        self.assertIn("allow_insecure", err)

    def run_cli_env(self, env, *argv):
        saved = {k: os.environ.get(k) for k in env}
        os.environ.update(env)
        out, err = io.StringIO(), io.StringIO()
        try:
            with redirect_stdout(out), redirect_stderr(err):
                code = cli.main(["--json", *argv])
        finally:
            for k, v in saved.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v
        return code, out.getvalue(), err.getvalue()


class SecureTest(LiveCase):
    """Only in secure mode: TLS with a throwaway CA, and a password login."""
    needs_aegis = False

    def setUp(self):
        if not SECURE_DIR:
            self.skipTest("not in secure mode; set AEGIS_CASS_SECURE_DIR and use cassandraUp.sh --secure")
        super().setUp()
        self.password = slurp(os.path.join(SECURE_DIR, "index.pass")).decode().strip()

    def write_password(self, text):
        path = os.path.join(self.tmp, "pw")
        with open(path, "w") as handle:
            handle.write(text)
        return path

    def test_right_user_password_file_and_ca_connect(self):
        from aegisfsIntegrations.cassandra import connection
        env = {"AEGISFS_INT_CASSANDRA_CONTACT_POINTS": ",".join(self.settings.contact_points),
               "AEGISFS_INT_CASSANDRA_PORT": str(self.settings.port),
               "AEGISFS_INT_CASSANDRA_TLS": "true", "AEGISFS_INT_CASSANDRA_TLS_CA_FILE": os.path.join(SECURE_DIR, "ca.pem"),
               "AEGISFS_INT_CASSANDRA_USERNAME": "aegis_index",
               "AEGISFS_INT_CASSANDRA_PASSWORD_FILE": os.path.join(SECURE_DIR, "index.pass"),
               "AEGISFS_INT_CASSANDRA_KEYSPACE": KEYSPACE}
        if SERVER_NAME:
            env["AEGISFS_INT_CASSANDRA_TLS_SERVER_NAME"] = SERVER_NAME
        st = Settings.from_config(Config({}, env))  # the password comes from the file
        self.assertEqual(st.password, self.password)
        cluster, session = connection.connect(st)
        try:
            user = session.execute("SELECT role FROM system_auth.roles WHERE role = 'aegis_index'").one()
            self.assertEqual(user.role, "aegis_index")
            self.assertTrue(cluster.connection_class is not None)
        finally:
            cluster.shutdown()

    def test_wrong_password_is_refused_and_never_echoed(self):
        wrong = "not-the-password-" + uuid.uuid4().hex
        st = base_settings(password=wrong)
        with self.assertRaises(SecurityError) as ctx:
            IndexStore.open(st)
        self.assertNotIn(wrong, str(ctx.exception))
        self.assertNotIn(self.password, str(ctx.exception))
        self.assertIn("refused the login", str(ctx.exception))
        # through the command line: exit code, stderr and JSON stay clean as well
        code, data, err = self.run_cli_with_password(wrong)
        self.assertEqual(code, cli.EXIT_UNAVAILABLE)
        combined = err + json.dumps(data)
        self.assertNotIn(wrong, combined)
        self.assertIn("refused the login", err)
        # a wrong password for migrate is refused too, and nothing is spooled
        self.assertEqual(self.spool.depth(), 0)

    def run_cli_with_password(self, password):
        path = self.write_password(password)
        env = {"AEGISFS_INT_CASSANDRA_CONTACT_POINTS": ",".join(self.settings.contact_points),
               "AEGISFS_INT_CASSANDRA_PORT": str(self.settings.port), "AEGISFS_INT_CASSANDRA_KEYSPACE": KEYSPACE,
               "AEGISFS_INT_CASSANDRA_TLS": "true", "AEGISFS_INT_CASSANDRA_TLS_CA_FILE": os.path.join(SECURE_DIR, "ca.pem"),
               "AEGISFS_INT_CASSANDRA_USERNAME": "aegis_index", "AEGISFS_INT_CASSANDRA_PASSWORD_FILE": path,
               "AEGISFS_INT_CASSANDRA_SPOOL_DIR": self.settings.spool_dir}
        if SERVER_NAME:
            env["AEGISFS_INT_CASSANDRA_TLS_SERVER_NAME"] = SERVER_NAME
        saved = {k: os.environ.get(k) for k in env}
        os.environ.update(env)
        out, err = io.StringIO(), io.StringIO()
        try:
            with redirect_stdout(out), redirect_stderr(err):
                code = cli.main(["--json", "cassandra", "status"])
        finally:
            for k, v in saved.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v
        try:
            data = json.loads(out.getvalue())
        except ValueError:
            data = None
        return code, data, err.getvalue()

    def test_wrong_or_missing_ca_fails_verification(self):
        from aegisfsIntegrations.cassandra import connection
        for label, ca in (("wrong CA", os.path.join(SECURE_DIR, "wrongCa.pem")), ("no CA given", None)):
            st = base_settings(tls_ca_file=ca)
            with self.assertRaises(SecurityError, msg=label) as ctx:
                connection.connect(st)
            self.assertIn("certificate verification failed", str(ctx.exception), label)
            self.assertNotIn(self.password, str(ctx.exception), label)
            with self.assertRaises(SecurityError, msg=label):
                IndexStore.open(st)

    def test_failed_verification_is_not_treated_as_an_outage(self):
        """A bad CA must stop the command, not queue work as if Cassandra were merely down."""
        import argparse
        from aegisfsIntegrations.cassandra import commands
        args = argparse.Namespace(o_contact_points=None, o_port=None, o_keyspace=None, o_spool_dir=None)
        cfg = Config({"cassandra": {"tls_ca_file": os.path.join(SECURE_DIR, "wrongCa.pem"),
                                    "username": "aegis_index",
                                    "password_file": os.path.join(SECURE_DIR, "index.pass"),
                                    "contact_points": self.settings.contact_points, "port": self.settings.port,
                                    "tls_server_name": SERVER_NAME, "keyspace": KEYSPACE, "spool_dir": self.settings.spool_dir}}, {})
        with self.assertRaises(cli.IntegrationUnavailable):
            commands.try_open_store(commands.settings_for(args, cfg))
        self.assertEqual(self.spool.depth(), 0)

    def test_unverified_tls_needs_the_insecure_switch_and_then_works(self):
        from aegisfsIntegrations.config import ConfigError
        with self.assertRaises(ConfigError):
            Settings.from_config(Config({}, {"AEGISFS_INT_CASSANDRA_TLS_VERIFY": "false"}))
        loose = base_settings(tls_verify=False, tls_ca_file=None, allow_insecure=True)
        IndexStore.open(loose).close()  # encrypted but not verified: only with the explicit switch

    def test_plaintext_is_refused_by_the_server_even_with_the_insecure_switch(self):
        plain = base_settings(tls=False, allow_insecure=True)
        with self.assertRaises((SecurityError, StoreUnavailable)):
            IndexStore.open(plain)

    def test_migrate_index_and_find_over_the_secure_connection(self):
        if not STATE["aegis"]:
            self.skipTest("the AegisFS cluster at %s is not healthy" % ENTRY)
        fresh = "aegis_sec_" + uuid.uuid4().hex[:8]
        code, data, err = self.run_cli("cassandra", "migrate", "--keyspace", fresh)
        self.assertEqual(code, 0, (data, err))
        self.assertEqual(data["schema_version"], 2)
        aegis = aegis_client()
        name = self.name("secure.bin")
        aegis.create(self.file(2048, seed=31), name)
        code, data, err = self.run_cli("cassandra", "index", "--name", name, "--keyspace", fresh)
        self.assertEqual((code, data["outcome"]), (0, "created"), (data, err))
        code, data, err = self.run_cli("cassandra", "find", "--name", name, "--keyspace", fresh, "--confirm")
        self.assertEqual(code, 0, (data, err))
        self.assertTrue(data["found"])
        self.assertTrue(data["confirm"]["agrees"])
        self.assertFalse(data["index"]["authoritative"])
        from aegisfsIntegrations.cassandra import connection
        cluster, session = connection.connect(base_settings(), use_keyspace=False)
        try:
            session.execute("DROP KEYSPACE IF EXISTS %s" % fresh)
        finally:
            cluster.shutdown()


class StoreTest(LiveCase):
    def test_roundtrip_of_every_field(self):
        name = self.name("a.bin")
        o = s.obs(name, 4, 0, stored_size=123, size=123, size_basis=m.SIZE_LISTSTORE, chunk_files=None,
                  replicas={"127.0.0.1:1": True, "127.0.0.1:2": False}, replicas_source="ls")
        o.observed_at = m.utcnow_ms()
        self.assertEqual(self.store.apply(o).outcome, "created")
        row = self.store.get(name)
        self.assertEqual((row["version"], row["stored_size"], row["size"], row["size_basis"]),
                         (4, 123, 123, m.SIZE_LISTSTORE))
        self.assertEqual(row["replicas"], {"127.0.0.1:1": True, "127.0.0.1:2": False})
        self.assertEqual(row["observed_at"], o.observed_at)
        self.assertIsNone(row["sha256"])
        self.assertEqual(row["verify_state"], "unverified")

    def test_update_and_checksum_lifecycle(self):
        name = self.name("c.bin")
        now = m.utcnow_ms()
        self.store.apply(_at(name, 1, now))
        verified = _at(name, 1, now + _ms(5), sha256="ab" * 32, verified_at=now + _ms(5), verified_size=10,
                       size=10, size_basis=m.SIZE_READ, verify_state="verified")
        self.assertEqual(self.store.apply(verified).outcome, "updated")
        self.assertEqual(self.store.get(name)["sha256"], "ab" * 32)
        newer = _at(name, 2, now + _ms(10), stored_size=40)
        self.assertEqual(self.store.apply(newer).outcome, "updated")
        row = self.store.get(name)
        self.assertEqual((row["version"], row["sha256"], row["verified_at"]), (2, None, None))

    def test_repeated_events_are_idempotent_even_in_parallel(self):
        name = self.name("rep.bin")
        o = _at(name, 3, m.utcnow_ms())
        results = []

        def work():
            results.append(self.store.apply(o).outcome)

        threads = [threading.Thread(target=work) for _ in range(12)]
        [t.start() for t in threads]
        [t.join() for t in threads]
        self.assertEqual(results.count("created"), 1, results)
        self.assertEqual(set(results) - {"created"}, {"unchanged"})
        self.assertEqual(self.store.count_files() >= 1, True)

    def test_out_of_order_events_never_overwrite_newer(self):
        name = self.name("ooo.bin")
        base = m.utcnow_ms()
        self.store.apply(_at(name, 5, base, stored_size=500))
        old_version = self.store.apply(_at(name, 4, base + _ms(50), stored_size=400))
        old_time = self.store.apply(_at(name, 5, base - _ms(50), stored_size=999))
        self.assertEqual((old_version.outcome, old_time.outcome), ("rejected", "rejected"))
        self.assertEqual(self.store.get(name)["stored_size"], 500)

    def test_shuffled_concurrent_versions_end_on_the_highest(self):
        name = self.name("shuffle.bin")
        base = m.utcnow_ms()
        events = [_at(name, v, base + _ms(v), stored_size=v * 10) for v in range(1, 31)]
        random.Random(7).shuffle(events)
        errors = []

        def work(chunk):
            for e in chunk:
                try:
                    self.store.apply(e)
                except Exception as exc:
                    errors.append(exc)

        threads = [threading.Thread(target=work, args=(events[i::6],)) for i in range(6)]
        [t.start() for t in threads]
        [t.join() for t in threads]
        self.assertEqual(errors, [])
        row = self.store.get(name)
        self.assertEqual((row["version"], row["stored_size"]), (30, 300))

    def test_invalid_metadata_is_rejected_and_not_stored(self):
        name = self.name("bad.bin")
        for bad in [_at(name, -1, m.utcnow_ms()), _at(name, 1, m.utcnow_ms() + _ms(10 * 60 * 1000)),
                    _at(name, 1, m.utcnow_ms(), sha256="XYZ", verified_at=m.utcnow_ms(), verified_size=1)]:
            with self.assertRaises(m.ObservationError):
                self.store.apply(bad)
        self.assertIsNone(self.store.get(name))

    def test_namespace_listing_and_attribute_queries(self):
        base = m.utcnow_ms()
        for i in range(25):
            self.store.apply(_at(self.name("d%d/f%02d" % (i % 3, i)), 1, base))
        self.store.apply(_at(self.name("big"), 1, base, striped=True, striped_basis=m.STRIPED_CHUNKS))
        self.store.apply(_at(self.name("weak"), 1, base, status="incomplete", status_reason="1 of 3"))
        rows, truncated = self.store.namespace_listing(self.names)
        self.assertEqual((len(rows), truncated), (27, False))
        rows, _ = self.store.namespace_listing(self.names + "d1/")
        self.assertEqual(len(rows), 8)
        rows, _ = self.store.namespace_listing(self.names + "d1/f0", recursive=False)
        self.assertEqual(sorted(r["name"] for r in rows), [self.name("d1/f01"), self.name("d1/f04"), self.name("d1/f07")])
        rows, truncated = self.store.namespace_listing(self.names, limit=5)
        self.assertEqual((len(rows), truncated), (5, True))
        striped, _, _ = self.store.by_attribute("striped=true", 1000)
        self.assertIn(self.name("big"), [r["name"] for r in striped])
        weak, _, _ = self.store.by_attribute("status=incomplete", 1000)
        self.assertIn(self.name("weak"), [r["name"] for r in weak])
        self.store.apply(_at(self.name("weak"), 1, base + _ms(10)))
        weak, _, _ = self.store.by_attribute("status=incomplete", 1000)
        self.assertNotIn(self.name("weak"), [r["name"] for r in weak])

    def test_unicode_and_odd_names(self):
        for leaf in ["naïve ☃.txt", "with space", "quote'name", 'dq"name', "percent%s"]:
            self.store.apply(_at(self.name(leaf), 1, m.utcnow_ms()))
            self.assertIsNotNone(self.store.get(self.name(leaf)), leaf)


def slurp(path):
    with open(path, "rb") as handle:
        return handle.read()


def _ms(n):
    from datetime import timedelta
    return timedelta(milliseconds=n)


def _at(name, version, when, **kw):
    kw.setdefault("source", "live-test")
    kw.setdefault("file_id", "ff%014x" % (abs(hash(name)) % 16**14))
    kw.setdefault("stored_size", 10)
    return m.Observation(name=name, version=version, observed_at=when, **kw)


class ReconcileLiveTest(LiveCase):
    needs_aegis = True

    def test_files_created_through_the_aegisfs_cli_are_discovered_and_verified(self):
        aegis = aegis_client()
        small = self.file(3000, seed=1)
        medium = self.file(200_000, seed=2)
        for leaf, path in (("one.bin", small), ("two.bin", medium)):
            aegis.create(path, self.name(leaf))
        striped_src = self.file(3_000_000, seed=3)
        aegis_client(stripe_mib=1).create(striped_src, self.name("striped.bin"))
        report = indexer.reconcile(self.store, self.spool, aegis, self.settings, verify=True)
        self.assertGreaterEqual(report["created"], 3, report)
        self.assertEqual(report["verify_failed"], 0, report)
        for leaf, path in (("one.bin", small), ("two.bin", medium), ("striped.bin", striped_src)):
            row = self.store.get(self.name(leaf))
            self.assertIsNotNone(row, leaf)
            digest = hashlib.sha256(slurp(path)).hexdigest()
            self.assertEqual(row["sha256"], digest, leaf)
            self.assertEqual(row["size"], os.path.getsize(path), leaf)
            self.assertEqual(row["size_basis"], "read")
            self.assertEqual(len(row["replicas"]), 3)
        self.assertTrue(self.store.get(self.name("striped.bin"))["striped"])
        self.assertFalse(self.store.get(self.name("one.bin"))["striped"])
        # stripe chunk files never appear as entries
        listed, _ = self.store.namespace_listing(self.names)
        self.assertEqual(sorted(r["name"] for r in listed),
                         sorted(self.name(x) for x in ("one.bin", "striped.bin", "two.bin")))
        # a second run finds nothing to do for these
        again = indexer.reconcile(self.store, self.spool, aegis, self.settings, verify=True)
        self.assertEqual((again["created"], again["verified"]), (0, 0))

    def test_unverified_striped_file_does_not_report_the_index_size(self):
        aegis = aegis_client()
        aegis_client(stripe_mib=1).create(self.file(2_500_000, seed=4), self.name("s.bin"))
        indexer.reconcile(self.store, self.spool, aegis, self.settings)
        row = self.store.get(self.name("s.bin"))
        self.assertTrue(row["striped"])
        self.assertIsNone(row["size"])
        self.assertLess(row["stored_size"], 10_000)  # the small index object

    def test_append_is_picked_up_and_checksum_resets(self):
        aegis = aegis_client()
        name = self.name("grow.bin")
        aegis.create(self.file(1000, seed=5), name)
        indexer.reconcile(self.store, self.spool, aegis, self.settings, verify=True)
        before = self.store.get(name)
        aegis.append(self.file(500, seed=6), name)
        report = indexer.reconcile(self.store, self.spool, aegis, self.settings)
        after = self.store.get(name)
        self.assertEqual(report["updated"] >= 1, True)
        self.assertGreater(after["version"], before["version"])
        self.assertIsNone(after["sha256"])
        self.assertEqual(after["size"], 1500)
        code, data, _ = self.run_cli("cassandra", "show", "--name", name, "--confirm")
        self.assertEqual(code, 0, data)
        self.assertTrue(data["confirm"]["agrees"])

    def test_index_one_file_uses_ls_placement(self):
        aegis = aegis_client()
        name = self.name("single.bin")
        aegis.create(self.file(2048, seed=7), name)
        code, data, _ = self.run_cli("cassandra", "index", "--name", name, "--verify-checksums")
        self.assertEqual(code, 0, data)
        self.assertEqual(data["outcome"], "created")
        row = self.store.get(name)
        self.assertEqual(row["replicas_source"], "ls")
        self.assertEqual(row["verify_state"], "verified")

    def test_confirm_reports_disagreement_and_aegisfs_errors(self):
        aegis = aegis_client()
        name = self.name("c.bin")
        aegis.create(self.file(1000, seed=8), name)
        indexer.reconcile(self.store, self.spool, aegis, self.settings, verify=True)
        # make the index claim something AegisFS does not say
        row = self.store.get(name)
        lie = dict(row, version=row["version"] + 5, observed_at=m.utcnow_ms())
        self.store.backend.update_row(lie, row)
        code, data, _ = self.run_cli("cassandra", "find", "--name", name, "--confirm")
        self.assertEqual(code, cli.EXIT_FAILED)
        self.assertFalse(data["confirm"]["agrees"])
        self.assertTrue(any("version" in d for d in data["confirm"]["differences"]))
        # a name AegisFS does not hold, indexed anyway
        ghost = self.name("ghost.bin")
        self.store.apply(_at(ghost, 1, m.utcnow_ms()))
        code, data, _ = self.run_cli("cassandra", "show", "--name", ghost, "--confirm")
        self.assertEqual(code, cli.EXIT_FAILED)
        self.assertIsNone(data["confirm"]["live"])

    def test_gone_file_is_marked_stale_after_a_complete_scan(self):
        aegis = aegis_client()
        keep, lose = self.name("keep.bin"), self.name("lose.bin")
        aegis.create(self.file(1500, seed=9), keep)
        aegis.create(self.file(1500, seed=10), lose)
        indexer.reconcile(self.store, self.spool, aegis, self.settings)
        file_id = self.store.get(lose)["file_id"]
        self.wipe_file_everywhere(file_id)
        report = indexer.reconcile(self.store, self.spool, aegis, self.settings)
        self.assertEqual(report["unreachable_nodes"], [])
        self.assertEqual(report["marked_stale"], 1, report)
        self.assertEqual(self.store.get(lose)["status"], "stale")
        self.assertEqual(self.store.get(keep)["status"], "indexed")
        code, data, _ = self.run_cli("cassandra", "stale", "--limit", "100000")
        self.assertIn(lose, [e["name"] for e in data["entries"]])
        self.assertNotIn(keep, [e["name"] for e in data["entries"]])

    def wipe_file_everywhere(self, file_id):
        """Remove one file's data from every node's directory, with the nodes stopped."""
        env = dict(os.environ, AEGISFS_RUN_DIR=RUN_DIR, AEGISFS_BASE_PORT=BASE_PORT)
        script = os.path.join(s.ROOT, "scripts", "cluster.sh")
        for i in range(NODES):
            subprocess.run(["bash", script, "kill", str(i)], env=env, capture_output=True)
        time.sleep(0.5)
        for i in range(NODES):
            shutil.rmtree(os.path.join(RUN_DIR, "node%d" % i, "files", file_id), ignore_errors=True)
        for i in range(NODES):
            subprocess.run(["bash", script, "restart", str(i)], env=env, capture_output=True)
        deadline = time.time() + 60
        while time.time() < deadline:
            time.sleep(1)
            try:
                members = aegis_client().members()
            except AegisError:
                continue
            if len(members) >= NODES and all(mm.state == "ALIVE" for mm in members):
                try:
                    for mm in members:
                        aegis_client().list_store(mm.address)
                    return
                except AegisError:
                    continue
        self.fail("cluster did not come back after the wipe")


class OutageTest(LiveCase):
    needs_aegis = True

    def docker(self, *args):
        try:
            return subprocess.run(["docker", *args], capture_output=True, text=True, timeout=180)
        except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
            self.skipTest("docker is not usable here: %s" % exc)

    def wait_for_cassandra(self, timeout=240):
        deadline = time.time() + timeout
        last = ""
        while time.time() < deadline:
            try:
                store = IndexStore.open(self.settings)
                store.close()
                return
            except (StoreUnavailable, Exception) as exc:  # noqa: BLE001
                last = str(exc)
                time.sleep(3)
        self.fail("Cassandra did not come back: %s" % last)

    def test_cassandra_down_then_back_nothing_lost_and_aegisfs_unaffected(self):
        if self.docker("inspect", CONTAINER).returncode != 0:
            self.skipTest("container %s not found" % CONTAINER)
        aegis = aegis_client()
        before = self.name("before.bin")
        aegis.create(self.file(1000, seed=11), before)
        indexer.reconcile(self.store, self.spool, aegis, self.settings)
        self.store.close()
        self.assertEqual(self.docker("stop", "-t", "20", CONTAINER).returncode, 0)
        try:
            # AegisFS keeps working while Cassandra is down.
            during = [self.name("during%d.bin" % i) for i in range(3)]
            for i, name in enumerate(during):
                src = self.file(1500 + i, seed=20 + i)
                aegis.create(src, name)
                dest = os.path.join(self.tmp, "back%d" % i)
                aegis.get(name, dest)
                self.assertEqual(slurp(dest), slurp(src))
            with self.assertRaises(StoreUnavailable):
                IndexStore.open(self.settings)
            # Indexing work queues instead of failing.
            code, data, err = self.run_cli("cassandra", "index", "--name", during[0])
            self.assertEqual(code, 0, (data, err))
            self.assertEqual(data["outcome"], "spooled")
            from aegisfsIntegrations.cassandra.hook import observe
            self.assertTrue(observe("create", during[1], spool_dir=self.settings.spool_dir))
            self.assertTrue(observe("create", during[2], spool_dir=self.settings.spool_dir))
            self.assertEqual(self.spool.depth(), 3)
            code, data, err = self.run_cli("cassandra", "index", "--drain")
            self.assertEqual(code, cli.EXIT_UNAVAILABLE, (data, err))
            self.assertEqual(self.spool.depth(), 3)  # nothing was lost by the failed drain
            code, data, err = self.run_cli("cassandra", "status")
            self.assertEqual(code, cli.EXIT_UNAVAILABLE)
            self.assertEqual(data["spool"]["pending_events"], 3)
        finally:
            self.assertEqual(self.docker("start", CONTAINER).returncode, 0)
        self.wait_for_cassandra()
        self.store = IndexStore.open(self.settings)
        code, data, err = self.run_cli("cassandra", "index", "--drain")
        self.assertEqual(code, 0, (data, err))
        self.assertEqual(data["drained"]["done"], 3)
        self.assertEqual(self.spool.depth(), 0)
        for name in during:
            row = self.store.get(name)
            self.assertIsNotNone(row, name)
            self.assertEqual(row["status"], "indexed")
        self.assertIsNotNone(self.store.get(before))  # data written before the outage survived the restart


class LargeInventoryTest(LiveCase):
    def test_thousands_of_entries(self):
        count = int(os.environ.get("AEGIS_CASS_LARGE", "3000"))
        aegis = s.FakeAegis()
        for i in range(count):
            aegis.add("%sbulk/dir%02d/file%05d.dat" % (self.names, i % 40, i), 1000 + i)
        started = time.time()
        report = indexer.reconcile(self.store, self.spool, aegis, self.settings)
        first = time.time() - started
        self.assertEqual(report["created"], count, report)
        started = time.time()
        again = indexer.reconcile(self.store, self.spool, aegis, self.settings)
        second = time.time() - started
        self.assertEqual((again["created"], again["updated"], again["unchanged"]), (0, 0, count))
        started = time.time()
        rows, truncated = self.store.namespace_listing(self.names + "bulk/dir07/", limit=1000)
        list_time = time.time() - started
        self.assertEqual(len(rows), count // 40)
        started = time.time()
        total = self.store.count_files()
        count_time = time.time() - started
        self.assertGreaterEqual(total, count)
        # an append to one file only touches that file
        aegis.entries[sorted(aegis.entries)[5]].version = 2
        started = time.time()
        third = indexer.reconcile(self.store, self.spool, aegis, self.settings)
        incremental = time.time() - started
        self.assertEqual((third["updated"], third["unchanged"]), (1, count - 1))
        print("\nlarge inventory: %d entries; first reconcile %.2fs (%.0f/s); no-change reconcile %.2fs; "
              "one change %.2fs; list one directory (%d rows) %.3fs; full count %.2fs"
              % (count, first, count / first, second, incremental, len(rows), list_time, count_time),
              file=sys.stderr)


if __name__ == "__main__":
    unittest.main()
