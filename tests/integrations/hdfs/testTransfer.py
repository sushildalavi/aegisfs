"""Unit tests for export, import, batch resume and the CLI wiring, using fakes for both systems."""
import os
import tempfile
import unittest
from unittest import mock

import testSupport  # noqa: F401
from fakeWebHdfs import FakeHdfs
from testSupport import FakeAegis, make_client, sha

from aegisfsIntegrations import cli
from aegisfsIntegrations.hdfs import paths
from aegisfsIntegrations.hdfs.journal import Journal, JournalBusy
from aegisfsIntegrations.hdfs.transfer import (REDO, TransferFailed, export_file, export_items,
                                               import_file, run_batch, walk_hdfs)
from aegisfsIntegrations.hdfs.webHdfs import HdfsNotFound, is_temp_name


class Base(unittest.TestCase):
    def setUp(self):
        self.fake = FakeHdfs()
        self.addCleanup(self.fake.close)
        self.aegis = FakeAegis()
        self.hdfs = make_client(self.fake)
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.work = os.path.join(self.tmp.name, "work")
        os.mkdir(self.work)

    def assertNoTemps(self):
        self.assertEqual([p for p in self.fake.files if is_temp_name(p)], [])
        self.assertEqual(os.listdir(self.work), [], "local temp files were left behind")

    def export(self, name, path, **kw):
        return export_file(self.aegis, self.hdfs, name, path, tmp_dir=self.work, **kw)

    def do_import(self, path, name, **kw):
        return import_file(self.aegis, self.hdfs, path, name, tmp_dir=self.work, **kw)


class ExportTests(Base):
    def test_export_verifies_and_leaves_no_temp(self):
        data = os.urandom(200000)
        self.aegis.store["a/b"] = data
        result = self.export("a/b", "/out/b.bin")
        self.assertEqual(result["status"], "OK")
        self.assertEqual(result["bytes"], len(data))
        self.assertEqual(result["sha256"], sha(data))
        self.assertEqual(self.fake.files["/out/b.bin"], data)
        self.assertIn("a/b", self.aegis.store)  # the source is untouched
        self.assertNoTemps()

    def test_empty_and_text_files(self):
        self.aegis.store["e"] = b""
        self.aegis.store["t"] = "café ✓\n".encode()
        self.export("e", "/e")
        self.export("t", "/t")
        self.assertEqual(self.fake.files["/e"], b"")
        self.assertEqual(self.fake.files["/t"], "café ✓\n".encode())

    def test_existing_destination_is_refused_and_left_alone(self):
        self.aegis.store["a"] = b"new"
        self.fake.put_file("/d", b"old")
        with self.assertRaisesRegex(TransferFailed, "already exists"):
            self.export("a", "/d")
        self.assertEqual(self.fake.files["/d"], b"old")
        self.assertNoTemps()

    def test_overwrite_replaces_atomically(self):
        self.aegis.store["a"] = b"new"
        self.fake.put_file("/d", b"old")
        self.export("a", "/d", overwrite=True)
        self.assertEqual(self.fake.files["/d"], b"new")
        self.assertNoTemps()

    def test_destination_directory_is_refused(self):
        self.aegis.store["a"] = b"x"
        self.fake.dirs.add("/dir")
        with self.assertRaisesRegex(TransferFailed, "directory"):
            self.export("a", "/dir", overwrite=True)

    def test_missing_source_writes_nothing(self):
        with self.assertRaisesRegex(TransferFailed, "cannot read"):
            self.export("nope", "/x")
        self.assertEqual(self.fake.files, {})
        self.assertNoTemps()

    def test_datanode_rejection_cleans_up(self):
        self.aegis.store["a"] = b"x" * 1000
        self.fake.dn_error = (500, "IOException", "disk on fire")
        with self.assertRaises(TransferFailed):
            self.export("a", "/x")
        self.assertNoTemps()
        self.assertNotIn("/x", self.fake.files)

    def test_corrupted_upload_is_caught_by_the_readback_and_removed(self):
        self.aegis.store["a"] = os.urandom(5000)
        self.fake.corrupt_upload = True
        with self.assertRaisesRegex(TransferFailed, "read-back"):
            self.export("a", "/x")
        self.assertNoTemps()
        self.assertNotIn("/x", self.fake.files)

    def test_interrupt_during_verification_removes_the_temp(self):
        self.aegis.store["a"] = b"y" * 5000
        with mock.patch.object(type(self.hdfs), "sha256", side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                self.export("a", "/x")
        self.assertNoTemps()
        self.assertNotIn("/x", self.fake.files)

    def test_stalled_datanode_times_out_and_leaves_no_destination(self):
        self.aegis.store["a"] = b"z" * 1000
        self.fake.stall_upload = True
        self.hdfs = make_client(self.fake, timeout=1, retries=0)
        with self.assertRaises(TransferFailed):
            self.export("a", "/x")
        self.assertNotIn("/x", self.fake.files)
        self.assertNoTemps()

    def test_failed_cleanup_is_reported_not_hidden(self):
        self.aegis.store["a"] = b"x" * 100
        self.fake.corrupt_upload = True
        with mock.patch.object(type(self.hdfs), "delete_temp",
                               side_effect=webHdfsUnavailable()):
            with self.assertRaises(TransferFailed) as ctx:
                self.export("a", "/x")
        self.assertIn("temp_left_in_hdfs", ctx.exception.payload)
        self.assertTrue(is_temp_name(ctx.exception.payload["temp_left_in_hdfs"]))

    def test_identical_existing_destination_is_adopted_only_when_asked(self):
        self.aegis.store["a"] = b"same"
        self.fake.put_file("/d", b"same")
        result = self.export("a", "/d", adopt_identical=True)
        self.assertTrue(result["adopted_existing"])
        self.fake.put_file("/d2", b"different")
        with self.assertRaisesRegex(TransferFailed, "differs"):
            self.export("a", "/d2", adopt_identical=True)
        self.assertEqual(self.fake.files["/d2"], b"different")


def webHdfsUnavailable():
    from aegisfsIntegrations.hdfs.webHdfs import HdfsUnavailable
    return HdfsUnavailable("namenode went away")


class ImportTests(Base):
    def test_import_verifies_through_aegis_and_keeps_source(self):
        data = os.urandom(150000)
        self.fake.put_file("/in/f.bin", data)
        result = self.do_import("/in/f.bin", "imp/f")
        self.assertEqual(result["status"], "OK")
        self.assertEqual(self.aegis.store["imp/f"], data)
        self.assertEqual(self.fake.files["/in/f.bin"], data)
        self.assertNoTemps()

    def test_existing_aegis_name_is_refused(self):
        self.fake.put_file("/f", b"x")
        self.aegis.store["n"] = b"old"
        with self.assertRaisesRegex(TransferFailed, "already exists"):
            self.do_import("/f", "n")
        self.assertEqual(self.aegis.store["n"], b"old")

    def test_already_exists_from_a_race_is_reported(self):
        self.fake.put_file("/f", b"x")
        with mock.patch.object(self.aegis, "exists", return_value=False):
            self.aegis.store["n"] = b"someone else"
            with self.assertRaisesRegex(TransferFailed, "nothing was overwritten"):
                self.do_import("/f", "n")
        self.assertEqual(self.aegis.store["n"], b"someone else")
        self.assertNoTemps()

    def test_missing_and_directory_sources(self):
        with self.assertRaisesRegex(TransferFailed, "does not exist"):
            self.do_import("/nope", "n")
        self.fake.dirs.add("/d")
        with self.assertRaisesRegex(TransferFailed, "not a file"):
            self.do_import("/d", "n")
        self.assertEqual(self.aegis.store, {})

    def test_degraded_create_is_not_reported_as_ok(self):
        self.fake.put_file("/f", b"x" * 50)
        self.aegis.create_status = "DEGRADED"
        with self.assertRaises(TransferFailed) as ctx:
            self.do_import("/f", "n")
        self.assertEqual(ctx.exception.payload["status"], "DEGRADED")
        self.assertIn("fewer", str(ctx.exception))
        self.assertNoTemps()

    def test_readback_mismatch_is_reported_and_says_the_file_remains(self):
        self.fake.put_file("/f", b"x" * 50)
        self.aegis.corrupt_get = True
        with self.assertRaisesRegex(TransferFailed, "cannot remove it") as ctx:
            self.do_import("/f", "n")
        self.assertEqual(ctx.exception.payload["status"], "MISMATCH")
        self.assertNoTemps()

    def test_dropped_download_resumes_and_still_verifies(self):
        data = os.urandom(300000)
        self.fake.put_file("/f", data)
        self.fake.drop_open_after = 70000
        self.fake.open_drops_left = 2
        self.do_import("/f", "n")
        self.assertEqual(self.aegis.store["n"], data)

    def test_dead_namenode_leaves_nothing(self):
        self.fake.put_file("/f", b"x")
        self.fake.close()
        hdfs = make_client(self.fake, retries=0)
        with self.assertRaises(TransferFailed):
            import_file(self.aegis, hdfs, "/f", "n", tmp_dir=self.work)
        self.assertEqual(self.aegis.store, {})
        self.assertEqual(os.listdir(self.work), [])
        # Opening for AegisFS alone still works while HDFS is down.
        self.aegis.store["still"] = b"fine"
        self.assertTrue(self.aegis.exists("still"))
        self.fake = FakeHdfs()  # so cleanup has something to close


class NameTests(unittest.TestCase):
    def test_bad_aegis_names(self):
        for bad in ("", "/abs", "a/../b", "..", ".", "a/./b", "a//b", "a/", "x\x00y", "x\ny",
                    "chunk.stripe.1", "a" * 1025, "x\x7fy"):
            with self.assertRaises(paths.InvalidName, msg=repr(bad)):
                paths.check_aegis_name(bad)
        self.assertEqual(paths.check_aegis_name("dir/file-1.txt"), "dir/file-1.txt")

    def test_bad_hdfs_paths(self):
        for bad in ("", "relative/x", "/a/../b", "/..", "/a/./b", "/a//b", "/a/", "/a\x00b",
                    "/a\nb", "/a:b", "/a\\b"):
            with self.assertRaises(paths.InvalidName, msg=repr(bad)):
                paths.resolve_hdfs_path(bad)
        self.assertEqual(paths.resolve_hdfs_path("/"), "/")
        self.assertEqual(paths.resolve_hdfs_path("/a/b.txt"), "/a/b.txt")

    def test_root_fence(self):
        root = "/data/aegis"
        self.assertEqual(paths.resolve_hdfs_path("x/y", root), "/data/aegis/x/y")
        self.assertEqual(paths.resolve_hdfs_path("/data/aegis/x", root), "/data/aegis/x")
        for bad in ("/data/aegis2/x", "/etc/passwd", "../x", "/data/aegis/../x", "/data"):
            with self.assertRaises(paths.InvalidName, msg=bad):
                paths.resolve_hdfs_path(bad, root)

    def test_temp_name_pattern(self):
        self.assertTrue(is_temp_name("/a/.f.bin.aegis-tmp-0123456789abcdef"))
        self.assertFalse(is_temp_name("/a/f.bin"))
        self.assertFalse(is_temp_name("/a/.f.aegis-tmp-xyz"))


class JournalTests(Base):
    def test_last_state_wins_and_torn_line_is_ignored(self):
        state = os.path.join(self.tmp.name, "state")
        with Journal(state, "j1") as j:
            j.record("a", "started")
            j.record("a", "complete", bytes=3, sha256="x")
            path = j.path
        with open(path, "a") as handle:
            handle.write('{"item": "b", "sta')
        with Journal(state, "j1") as j:
            states = j.last_states()
        self.assertEqual(states["a"]["state"], "complete")
        self.assertNotIn("b", states)

    def test_only_one_run_at_a_time(self):
        state = os.path.join(self.tmp.name, "state")
        with Journal(state, "j2"):
            with self.assertRaises(JournalBusy):
                Journal(state, "j2").open()


class BatchTests(Base):
    def setUp(self):
        super().setUp()
        self.state = os.path.join(self.tmp.name, "state")
        self.datas = {"p/one": os.urandom(1000), "p/two": os.urandom(2000),
                      "p/sub/three": b"3" * 30}
        self.aegis.store.update(self.datas)
        self.items = export_items(self.aegis, sorted(self.datas), "/out", "p/")

    def run_export(self, overwrite=False):
        def do_one(src, dst, adopt):
            return export_file(self.aegis, self.hdfs, src, dst, overwrite, self.work,
                               adopt_identical=adopt)

        def verify(dst, rec):
            try:
                info = self.hdfs.status(dst)
            except HdfsNotFound:
                return REDO
            sha_now, _ = self.hdfs.sha256(dst, info["length"])
            return None if sha_now == rec["sha256"] and info["length"] == rec["bytes"] \
                else "no longer matches"
        with Journal(self.state, "export-test") as journal:
            return run_batch(journal, self.items, do_one, verify, overwrite)

    def test_item_mapping_keeps_subdirectories(self):
        self.assertEqual([i[2] for i in self.items],
                         ["/out/one", "/out/sub/three", "/out/two"])

    def test_resume_skips_verified_items_and_retries_failures(self):
        self.fake.dn_error = (500, "IOException", "boom")
        rows = self.run_export()
        self.assertEqual({r["state"] for r in rows}, {"failed"})
        self.fake.dn_error = None
        rows = self.run_export()
        self.assertEqual({r["state"] for r in rows}, {"complete"})
        rows = self.run_export()
        self.assertEqual({r["state"] for r in rows}, {"skipped"})
        self.assertNoTemps()

    def test_a_complete_item_whose_destination_changed_is_reported_not_skipped(self):
        self.run_export()
        self.fake.files["/out/one"] = b"tampered" + self.fake.files["/out/one"][8:]
        rows = {r["item"]: r for r in self.run_export()}
        self.assertEqual(rows["p/one"]["state"], "failed")
        self.assertIn("no longer matches", rows["p/one"]["detail"])
        self.assertEqual(rows["p/two"]["state"], "skipped")

    def test_a_complete_item_whose_destination_vanished_is_transferred_again(self):
        self.run_export()
        del self.fake.files["/out/two"]
        rows = {r["item"]: r for r in self.run_export()}
        self.assertEqual(rows["p/two"]["state"], "complete")
        self.assertEqual(rows["p/one"]["state"], "skipped")
        self.assertEqual(self.fake.files["/out/two"], self.datas["p/two"])

    def test_unjournaled_existing_destination_is_never_overwritten(self):
        self.fake.put_file("/out/one", b"someone elses file")
        rows = {r["item"]: r for r in self.run_export()}
        self.assertEqual(rows["p/one"]["state"], "failed")
        self.assertEqual(self.fake.files["/out/one"], b"someone elses file")
        self.assertEqual(rows["p/two"]["state"], "complete")

    def test_crash_after_rename_before_journal_is_adopted_when_identical(self):
        with Journal(self.state, "export-test") as j:
            j.record("p/one", "started")
        self.fake.put_file("/out/one", self.datas["p/one"])
        rows = {r["item"]: r for r in self.run_export()}
        self.assertEqual(rows["p/one"]["state"], "complete")
        self.assertIn("identical", rows["p/one"]["detail"])

    def test_invalid_destination_counts_as_failed_item(self):
        items = [("p/one", "p/one", "/out/../escape")]
        with Journal(self.state, "bad") as journal:
            rows = run_batch(journal, items,
                             lambda s, d, a: export_file(self.aegis, self.hdfs, s,
                                                         paths.resolve_hdfs_path(d)),
                             lambda d, r: None, False)
        self.assertEqual(rows[0]["state"], "failed")

    def test_walk_skips_our_temp_files_and_honours_recursion(self):
        self.fake.put_file("/in/a.txt", b"a")
        self.fake.put_file("/in/.b.aegis-tmp-0123456789abcdef", b"t")
        self.fake.put_file("/in/sub/c.txt", b"c")
        flat = [r for r, _ in walk_hdfs(self.hdfs, "/in", False)]
        deep = sorted(r for r, _ in walk_hdfs(self.hdfs, "/in", True))
        self.assertEqual(flat, ["a.txt"])
        self.assertEqual(deep, ["a.txt", "sub/c.txt"])


class CliTests(unittest.TestCase):
    def run_cli(self, argv, env):
        clean = {k: v for k, v in os.environ.items() if not k.startswith("AEGISFS_INT_")}
        clean.update(env)
        with mock.patch.dict(os.environ, clean, clear=True):
            return cli.main(argv)

    def test_missing_namenode_url_is_a_usage_error(self):
        self.assertEqual(self.run_cli(["hdfs", "status"], {}), cli.EXIT_USAGE)

    def test_http_without_switch_is_refused(self):
        env = {"AEGISFS_INT_HDFS_NAMENODE_URL": "http://127.0.0.1:1"}
        self.assertEqual(self.run_cli(["hdfs", "status"], env), cli.EXIT_USAGE)

    def test_kerberos_is_not_accepted(self):
        env = {"AEGISFS_INT_HDFS_NAMENODE_URL": "https://nn:1", "AEGISFS_INT_HDFS_AUTH": "kerberos"}
        self.assertEqual(self.run_cli(["hdfs", "status"], env), cli.EXIT_USAGE)

    def test_token_auth_needs_a_token_file(self):
        env = {"AEGISFS_INT_HDFS_NAMENODE_URL": "https://nn:1", "AEGISFS_INT_HDFS_AUTH": "token"}
        self.assertEqual(self.run_cli(["hdfs", "status"], env), cli.EXIT_USAGE)

    def test_unreachable_namenode_exits_unavailable(self):
        fake = FakeHdfs()
        url = fake.nn_url
        fake.close()
        env = {"AEGISFS_INT_HDFS_NAMENODE_URL": url, "AEGISFS_INT_HDFS_ALLOW_INSECURE_HTTP": "1",
               "AEGISFS_INT_HDFS_RETRIES": "0"}
        self.assertEqual(self.run_cli(["hdfs", "status"], env), cli.EXIT_UNAVAILABLE)

    def test_invalid_path_is_refused_before_any_request(self):
        fake = FakeHdfs()
        self.addCleanup(fake.close)
        env = {"AEGISFS_INT_HDFS_NAMENODE_URL": fake.nn_url,
               "AEGISFS_INT_HDFS_ALLOW_INSECURE_HTTP": "1"}
        self.assertEqual(self.run_cli(["hdfs", "list", "/a/../b"], env), cli.EXIT_FAILED)
        self.assertEqual(fake.requests, [])

    def test_list_stat_exists_against_the_fake(self):
        fake = FakeHdfs()
        self.addCleanup(fake.close)
        fake.put_file("/d/f.txt", b"hello")
        env = {"AEGISFS_INT_HDFS_NAMENODE_URL": fake.nn_url,
               "AEGISFS_INT_HDFS_ALLOW_INSECURE_HTTP": "1"}
        self.assertEqual(self.run_cli(["hdfs", "list", "/d"], env), 0)
        self.assertEqual(self.run_cli(["hdfs", "stat", "/d/f.txt"], env), 0)
        self.assertEqual(self.run_cli(["hdfs", "exists", "/d/f.txt"], env), 0)
        self.assertEqual(self.run_cli(["hdfs", "exists", "/d/none"], env), 1)

    def test_root_fence_applies_to_relative_and_absolute_paths(self):
        fake = FakeHdfs()
        self.addCleanup(fake.close)
        fake.put_file("/fence/f.txt", b"hello")
        env = {"AEGISFS_INT_HDFS_NAMENODE_URL": fake.nn_url,
               "AEGISFS_INT_HDFS_ALLOW_INSECURE_HTTP": "1", "AEGISFS_INT_HDFS_ROOT": "/fence"}
        self.assertEqual(self.run_cli(["hdfs", "exists", "f.txt"], env), 0)
        self.assertEqual(self.run_cli(["hdfs", "exists", "/etc/passwd"], env), cli.EXIT_FAILED)


if __name__ == "__main__":
    unittest.main()
