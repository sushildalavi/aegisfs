"""Live tests: a real HDFS cluster and a real AegisFS cluster.

These run inside the compose network (see runLive.sh), because WebHDFS redirects to DataNode host
names that only resolve there. Settings come from the same AEGISFS_INT_* environment variables the
tool uses. When HDFS is not reachable the tests skip with a message, unless AEGISFS_INT_HDFS_LIVE=1,
in which case that is a failure.
"""
import json
import os
import re
import signal
import subprocess
import sys
import tempfile
import time
import unittest
import uuid

import testSupport  # noqa: F401
from testSupport import sha

from aegisfsIntegrations.aegisfsClient import AegisClient, AegisError, sha256_file
from aegisfsIntegrations.config import Config
from aegisfsIntegrations.hdfs import settings as hdfsSettings
from aegisfsIntegrations.hdfs.transfer import TransferFailed, export_file, import_file
from aegisfsIntegrations.hdfs.webHdfs import WebHdfsClient, is_temp_name

HERE = os.path.dirname(os.path.abspath(__file__))
LAUNCHER = next(p for p in (os.path.join(HERE, "..", "..", "..", "extensions", "integrations",
                                         "aegisfs-integrations"),
                            "/opt/aegisfs-integrations/aegisfs-integrations")
                if os.path.exists(p))
REQUIRED = os.environ.get("AEGISFS_INT_HDFS_LIVE") == "1"
RUN = uuid.uuid4().hex[:8]
BIG_MIB = int(os.environ.get("AEGISFS_HDFS_LIVE_BIG_MIB", "100"))


def live_clients():
    cfg = Config.load()
    settings, options = hdfsSettings.load(cfg)
    hdfs = WebHdfsClient(settings)
    aegis = AegisClient.from_config(cfg)
    return cfg, hdfs, aegis, options


class LiveCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            cls.cfg, cls.hdfs, cls.aegis, cls.options = live_clients()
            cls.hdfs.namenode_reachable()
            if not cls.aegis.health():
                raise RuntimeError("the AegisFS entry node is not healthy")
        except Exception as exc:  # noqa: BLE001 - any setup failure means "no live cluster"
            message = "live HDFS/AegisFS not reachable (%s: %s)" % (type(exc).__name__, exc)
            if REQUIRED:
                raise AssertionError(message)
            raise unittest.SkipTest(message)
        root = cls.options.root or "/aegisfs-test"
        cls.dir = "%s/%s" % (root.rstrip("/"), RUN)
        cls.prefix = "hdfs-it/%s/" % RUN
        cls.hdfs.mkdirs(cls.dir)

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    # helpers
    def name(self, label):
        return self.prefix + label

    def hpath(self, label):
        return "%s/%s" % (self.dir, label)

    def local(self, data, label="in.bin"):
        path = os.path.join(self.tmp.name, label)
        with open(path, "wb") as handle:
            handle.write(data)
        return path

    def aegis_create(self, label, data):
        self.aegis.create(self.local(data, label.replace("/", "_")), self.name(label))
        return self.name(label)

    def hdfs_bytes(self, path):
        out = bytearray()
        self.hdfs.download(path, out.extend)
        return bytes(out)

    def aegis_bytes(self, name):
        dest = os.path.join(self.tmp.name, "dl-" + uuid.uuid4().hex[:6])
        self.aegis.get(name, dest)
        with open(dest, "rb") as handle:
            return handle.read()

    def export(self, aegis_name, label, **kw):
        return export_file(self.aegis, self.hdfs, aegis_name, self.hpath(label), **kw)

    def run_cli(self, *argv, env=None, timeout=900, rss=False):
        full = dict(os.environ)
        full.update(env or {})
        script = [os.path.join(HERE, "measureRss.py")] if rss else [LAUNCHER]
        return subprocess.run([sys.executable] + script + ["--json"] + list(argv), env=full,
                              capture_output=True, text=True, timeout=timeout)

    def temps_in(self, directory):
        return [e["pathSuffix"] for e in self.hdfs.list(directory) if is_temp_name(e["pathSuffix"])]


class RoundTripTests(LiveCase):
    def test_small_text_both_ways(self):
        text = "line one\ncafé ✓ two\n".encode()
        name = self.aegis_create("text", text)
        result = self.export(name, "text.txt")
        self.assertEqual(result["status"], "OK")
        self.assertEqual(self.hdfs_bytes(self.hpath("text.txt")), text)
        back = import_file(self.aegis, self.hdfs, self.hpath("text.txt"), self.name("text-back"))
        self.assertEqual(back["status"], "OK")
        self.assertEqual(self.aegis_bytes(self.name("text-back")), text)

    def test_random_binary_one_mib(self):
        data = os.urandom(1024 * 1024 + 17)
        name = self.aegis_create("bin", data)
        self.export(name, "bin.dat")
        self.assertEqual(sha(self.hdfs_bytes(self.hpath("bin.dat"))), sha(data))
        import_file(self.aegis, self.hdfs, self.hpath("bin.dat"), self.name("bin-back"))
        self.assertEqual(sha(self.aegis_bytes(self.name("bin-back"))), sha(data))

    def test_empty_file(self):
        try:
            name = self.aegis_create("empty", b"")
        except AegisError as exc:
            self.skipTest("this AegisFS build does not accept an empty create: %s" % exc)
        self.export(name, "empty.dat")
        self.assertEqual(self.hdfs.status(self.hpath("empty.dat"))["length"], 0)
        import_file(self.aegis, self.hdfs, self.hpath("empty.dat"), self.name("empty-back"))
        self.assertEqual(self.aegis_bytes(self.name("empty-back")), b"")

    def test_file_with_appended_content_exports_the_logical_bytes(self):
        first, second, third = os.urandom(70000), b"appended text\n", os.urandom(30000)
        name = self.aegis_create("appended", first)
        self.aegis.append(self.local(second, "a2"), name)
        self.aegis.append(self.local(third, "a3"), name)
        self.export(name, "appended.dat")
        self.assertEqual(self.hdfs_bytes(self.hpath("appended.dat")), first + second + third)

    def test_striped_file_is_exported_reassembled_and_imports_back(self):
        data = os.urandom(5 * 1000 * 1000)
        striped = AegisClient.from_config(self.cfg)
        striped.stripe_mib = 1
        striped.create(self.local(data, "striped"), self.name("striped"))
        # The file really is striped: its own entry is a small index, not the data.
        entries, _ = striped.inventory()
        index = entries[self.name("striped")]
        self.assertLess(index.size, 4096, "expected a stripe index, found %d bytes" % index.size)
        self.assertTrue(any(n.startswith(self.name("striped") + ".stripe.") for n in entries))
        result = export_file(striped, self.hdfs, self.name("striped"), self.hpath("striped.dat"))
        self.assertEqual(result["bytes"], len(data))
        self.assertEqual(self.hdfs.status(self.hpath("striped.dat"))["length"], len(data))
        self.assertEqual(sha(self.hdfs_bytes(self.hpath("striped.dat"))), sha(data))
        back = import_file(striped, self.hdfs, self.hpath("striped.dat"), self.name("striped-back"))
        self.assertEqual(back["status"], "OK")
        back_index = striped.inventory()[0][self.name("striped-back")]
        self.assertLess(back_index.size, 4096)
        self.assertEqual(sha(self.aegis_bytes(self.name("striped-back"))), sha(data))


class SafetyTests(LiveCase):
    def test_collision_is_refused_then_overwrite_replaces(self):
        name1 = self.aegis_create("c1", b"first")
        name2 = self.aegis_create("c2", b"second")
        self.export(name1, "collide.txt")
        with self.assertRaisesRegex(TransferFailed, "already exists"):
            self.export(name2, "collide.txt")
        self.assertEqual(self.hdfs_bytes(self.hpath("collide.txt")), b"first")
        self.assertEqual(self.temps_in(self.dir), [])
        self.export(name2, "collide.txt", overwrite=True)
        self.assertEqual(self.hdfs_bytes(self.hpath("collide.txt")), b"second")
        self.assertEqual(self.temps_in(self.dir), [])

    def test_import_into_existing_name_is_refused(self):
        name = self.aegis_create("existing", b"keep me")
        self.hdfs_put("other-content.txt", b"different")
        with self.assertRaisesRegex(TransferFailed, "already exists"):
            import_file(self.aegis, self.hdfs, self.hpath("other-content.txt"), name)
        self.assertEqual(self.aegis_bytes(name), b"keep me")

    def hdfs_put(self, label, data):
        self.hdfs.upload(self.local(data, "put"), self.hpath(label), len(data))

    def test_missing_sources(self):
        with self.assertRaises(TransferFailed):
            self.export(self.name("does-not-exist"), "never.txt")
        self.assertFalse(self.hdfs.exists(self.hpath("never.txt")))
        with self.assertRaisesRegex(TransferFailed, "does not exist"):
            import_file(self.aegis, self.hdfs, self.hpath("nothing-here"), self.name("x-none"))
        self.assertFalse(self.aegis.exists(self.name("x-none")))

    def test_invalid_names_and_paths_exit_nonzero_without_touching_anything(self):
        before = self.hdfs.list(self.dir)
        for argv in (["hdfs", "export", self.name("a"), self.dir + "/../escape"],
                     ["hdfs", "export", "../up", self.hpath("x")],
                     ["hdfs", "export", "/absolute", self.hpath("x")],
                     ["hdfs", "export", "a\x01b", self.hpath("x")],
                     ["hdfs", "export", self.name("a"), "relative/path"],
                     ["hdfs", "import", self.dir + "/x\x01y", self.name("n")],
                     ["hdfs", "import", self.hpath("x"), "a/../b"]):
            proc = self.run_cli(*argv)
            self.assertNotEqual(proc.returncode, 0, argv)
        self.assertEqual(self.hdfs.list(self.dir), before)

    def test_root_fence_blocks_paths_outside_the_configured_root(self):
        proc = self.run_cli("hdfs", "exists", "/tmp", env={"AEGISFS_INT_HDFS_ROOT": self.dir})
        self.assertEqual(proc.returncode, 1)
        self.assertIn("outside the configured root", proc.stderr)

    def test_hdfs_permissions_are_enforced(self):
        name = self.aegis_create("perm", b"data")
        with self.assertRaises(TransferFailed) as ctx:
            export_file(self.aegis, self.hdfs, name, "/not-allowed.dat")
        self.assertRegex(str(ctx.exception), "Permission denied|denied|AccessControl")
        proc = self.run_cli("hdfs", "export", name, "/not-allowed.dat",
                            env={"AEGISFS_INT_HDFS_ROOT": "/"})
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertRegex(proc.stderr, "denied|Permission")

    def test_live_redirect_to_unlisted_datanode_is_refused_and_nothing_is_sent(self):
        name = self.aegis_create("redir", b"x" * 1000)
        proc = self.run_cli("hdfs", "export", name, self.hpath("redir.dat"),
                            env={"AEGISFS_INT_HDFS_ALLOWED_REDIRECT_HOSTS": "somewhere-else"})
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn("allowed_redirect_hosts", proc.stderr)
        self.assertFalse(self.hdfs.exists(self.hpath("redir.dat")))
        self.assertEqual(self.temps_in(self.dir), [])

    def test_transport_errors(self):
        # TLS to a plain-HTTP port fails the handshake instead of falling back.
        https = re.sub(r"^http://", "https://", self.cfg.get("hdfs", "namenode_url"))
        proc = self.run_cli("hdfs", "status", env={"AEGISFS_INT_HDFS_NAMENODE_URL": https,
                                                     "AEGISFS_INT_HDFS_RETRIES": "0"})
        self.assertEqual(proc.returncode, 3, proc.stdout + proc.stderr)
        # plain http without the development switch is refused outright
        proc = self.run_cli("hdfs", "status", env={"AEGISFS_INT_HDFS_ALLOW_INSECURE_HTTP": "0"})
        self.assertEqual(proc.returncode, 2)
        self.assertIn("plain http://", proc.stderr)

    def test_hdfs_down_does_not_stop_aegisfs(self):
        dead = {"AEGISFS_INT_HDFS_NAMENODE_URL": "http://127.0.0.1:9", "AEGISFS_INT_HDFS_RETRIES": "0"}
        name = self.aegis_create("while-down", b"still works")
        proc = self.run_cli("hdfs", "export", name, self.hpath("down.dat"), env=dead)
        self.assertEqual(proc.returncode, 3, proc.stdout + proc.stderr)
        self.assertEqual(self.aegis_bytes(name), b"still works")
        self.aegis_create("while-down-2", b"created while HDFS was unreachable")
        self.assertEqual(self.aegis_bytes(self.name("while-down-2")),
                         b"created while HDFS was unreachable")


class InterruptionTests(LiveCase):
    def start_big_export(self, label, size_mib=BIG_MIB):
        path = os.path.join(self.tmp.name, label)
        block = os.urandom(1024 * 1024)
        with open(path, "wb") as handle:
            for _ in range(size_mib):
                handle.write(block)
        name = self.name(label)
        self.aegis.create(path, name)
        os.unlink(path)
        env = dict(os.environ)
        proc = subprocess.Popen([sys.executable, LAUNCHER, "--json", "hdfs", "export", name,
                                 self.hpath(label + ".dat")], env=env, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, text=True)
        return name, proc

    def wait_for_temp(self, proc, limit=240):
        end = time.time() + limit
        while time.time() < end:
            if proc.poll() is not None:
                self.fail("the export finished before it could be interrupted")
            if self.temps_in(self.dir):
                return
            time.sleep(0.2)
        self.fail("no temporary HDFS file appeared")

    def test_sigterm_mid_upload_removes_the_temp_and_leaves_no_destination(self):
        name, proc = self.start_big_export("term")
        self.wait_for_temp(proc)
        proc.send_signal(signal.SIGTERM)
        proc.communicate(timeout=120)
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(self.temps_in(self.dir), [])
        self.assertFalse(self.hdfs.exists(self.hpath("term.dat")))
        self.assertTrue(self.aegis.exists(name))

    def test_sigkill_mid_upload_leaves_no_destination_and_cleanup_removes_the_temp(self):
        name, proc = self.start_big_export("kill")
        self.wait_for_temp(proc)
        proc.kill()
        proc.communicate(timeout=60)
        self.assertFalse(self.hdfs.exists(self.hpath("kill.dat")))
        # The temp may or may not still be listed (HDFS drops an abandoned write after a lease
        # timeout), so the check is on the destination and on what `cleanup` can do.
        listed = self.temps_in(self.dir)
        proc2 = self.run_cli("hdfs", "cleanup", self.dir, "--older-than-hours", "0")
        self.assertEqual(proc2.returncode, 0, proc2.stdout + proc2.stderr)
        self.assertEqual(self.temps_in(self.dir), [])
        if listed:
            self.assertGreaterEqual(len(json.loads(proc2.stdout)["removed"]), 1)


class LargeFileTests(LiveCase):
    def test_hundred_mib_both_directions_with_peak_memory(self):
        mib = BIG_MIB
        path = os.path.join(self.tmp.name, "big")
        with open(path, "wb") as handle:
            block = os.urandom(1024 * 1024)
            for _ in range(mib):
                handle.write(block)
        want = sha256_file(path)
        self.aegis.create(path, self.name("big"))
        os.unlink(path)

        proc = self.run_cli("hdfs", "export", self.name("big"), self.hpath("big.dat"), rss=True)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        out = json.loads(proc.stdout)
        export_rss = int(re.search(r"PEAK_RSS_KB=(\d+)", proc.stderr).group(1))
        self.assertEqual(out["bytes"], mib * 1024 * 1024)
        self.assertEqual(out["sha256"], want)

        proc = self.run_cli("hdfs", "import", self.hpath("big.dat"), self.name("big-back"),
                            rss=True)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        import_rss = int(re.search(r"PEAK_RSS_KB=(\d+)", proc.stderr).group(1))
        self.assertEqual(json.loads(proc.stdout)["sha256"], want)
        got = os.path.join(self.tmp.name, "big-back")
        self.aegis.get(self.name("big-back"), got)
        self.assertEqual(sha256_file(got), want)
        print("\nPEAK RSS for %d MiB: export %.1f MiB, import %.1f MiB" %
              (mib, export_rss / 1024, import_rss / 1024), file=sys.stderr)
        # Holding the file in memory would need more than its own size on top of the
        # interpreter. 64 MiB of headroom over a bare interpreter is a loose bound.
        self.assertLess(export_rss, 64 * 1024)
        self.assertLess(import_rss, 64 * 1024)


class BatchTests(LiveCase):
    def test_export_all_import_all_and_resume(self):
        base = self.name("batch/")
        datas = {"a.bin": os.urandom(40000), "b.txt": b"bee\n" * 100,
                 "sub/c.bin": os.urandom(70000)}
        for rel, data in datas.items():
            self.aegis.create(self.local(data, "bt"), base + rel)
        state = os.path.join(self.tmp.name, "state")
        env = {"AEGISFS_INT_HDFS_STATE_DIR": state}
        target = self.hpath("batch-out")

        proc = self.run_cli("hdfs", "export-all", "--prefix", base, "--to", target, env=env)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(json.loads(proc.stdout)["counts"], {"complete": 3})
        for rel, data in datas.items():
            self.assertEqual(self.hdfs_bytes(target + "/" + rel), data)

        proc = self.run_cli("hdfs", "export-all", "--prefix", base, "--to", target, env=env)
        self.assertEqual(proc.returncode, 0)
        self.assertEqual(json.loads(proc.stdout)["counts"], {"skipped": 3})

        # One destination disappears: only that file is transferred again.
        self.hdfs._json_call("DELETE", target + "/b.txt", "DELETE", {"recursive": "false"})
        proc = self.run_cli("hdfs", "export-all", "--prefix", base, "--to", target, env=env)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(json.loads(proc.stdout)["counts"], {"complete": 1, "skipped": 2})

        # Import the exported directory back under a new prefix, recursively.
        back = self.name("batch-back/")
        proc = self.run_cli("hdfs", "import-all", target, "--prefix", back, "--recursive", env=env)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(json.loads(proc.stdout)["counts"], {"complete": 3})
        for rel, data in datas.items():
            self.assertEqual(self.aegis_bytes(back + rel), data)
        self.assertEqual(self.temps_in(target), [])

    def test_a_failing_item_gives_a_nonzero_exit_and_the_rest_still_complete(self):
        src = self.hpath("mixed-in")
        self.hdfs.mkdirs(src)
        for label, data in (("ok1", b"1" * 100), ("clash", b"2" * 100), ("ok2", b"3" * 100)):
            self.hdfs.upload(self.local(data, label), src + "/" + label, len(data))
        prefix = self.name("mixed/")
        self.aegis.create(self.local(b"already here", "pre"), prefix + "clash")
        env = {"AEGISFS_INT_HDFS_STATE_DIR": os.path.join(self.tmp.name, "st2")}
        proc = self.run_cli("hdfs", "import-all", src, "--prefix", prefix, env=env)
        self.assertEqual(proc.returncode, 1)
        rows = {r["item"].rsplit("/", 1)[-1]: r["state"] for r in json.loads(proc.stdout)["results"]}
        self.assertEqual(rows, {"ok1": "complete", "clash": "failed", "ok2": "complete"})
        self.assertEqual(self.aegis_bytes(prefix + "clash"), b"already here")


class StatusTests(LiveCase):
    def test_status_list_stat_exists(self):
        proc = self.run_cli("hdfs", "status")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        info = json.loads(proc.stdout)
        self.assertTrue(info["reachable"])
        self.assertEqual(info["safemode"], False)
        name = self.aegis_create("st", b"stat me")
        self.export(name, "st.txt")
        listing = json.loads(self.run_cli("hdfs", "list", self.dir).stdout)
        self.assertIn("st.txt", [e["name"] for e in listing["entries"]])
        stat = json.loads(self.run_cli("hdfs", "stat", self.hpath("st.txt"), "--checksum").stdout)
        self.assertEqual(stat["length"], 7)
        self.assertIn("algorithm", stat["hdfs_checksum"])
        self.assertEqual(self.run_cli("hdfs", "exists", self.hpath("st.txt")).returncode, 0)
        self.assertEqual(self.run_cli("hdfs", "exists", self.hpath("nope")).returncode, 1)


if __name__ == "__main__":
    unittest.main()
