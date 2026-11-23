"""Reconcile, drain, checksum verification and confirm, using a fake AegisFS and the in-memory index."""
import hashlib
import os
import tempfile
import unittest

import cassSupport as s
from aegisfsIntegrations.aegisfsClient import AegisError
from aegisfsIntegrations.cassandra import indexer, model as m, queries
from aegisfsIntegrations.cassandra.errors import StoreUnavailable
from aegisfsIntegrations.cassandra.spool import Spool


class IndexerCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.spool = Spool(os.path.join(self.tmp.name, "spool"))
        self.store = s.memory_store()
        self.aegis = s.FakeAegis()
        self.settings = s.settings(workers=4)

    def tearDown(self):
        self.tmp.cleanup()

    def reconcile(self, **kw):
        return indexer.reconcile(self.store, self.spool, self.aegis, self.settings, **kw)


class ReconcileTest(IndexerCase):
    def test_discovers_new_files(self):
        self.aegis.add("docs/a.txt", 11)
        self.aegis.add("docs/b.txt", 22)
        report = self.reconcile()
        self.assertEqual((report["scanned_files"], report["created"]), (2, 2))
        row = self.store.get("docs/a.txt")
        self.assertEqual((row["version"], row["stored_size"], row["size"], row["size_basis"]),
                         (1, 11, 11, m.SIZE_LISTSTORE))
        self.assertEqual(row["replicas"], {"n1:1": True, "n2:1": True, "n3:1": True})
        self.assertIsNone(row["sha256"])

    def test_second_run_changes_nothing(self):
        self.aegis.add("a", 5)
        self.reconcile()
        report = self.reconcile()
        self.assertEqual((report["created"], report["updated"], report["unchanged"]), (0, 0, 1))

    def test_changed_version_updates_and_drops_checksum(self):
        self.aegis.add("a", 5, data=b"hello")
        self.reconcile(verify=True)
        self.assertIsNotNone(self.store.get("a")["sha256"])
        self.aegis.add("a", 9, version=2)
        report = self.reconcile()
        row = self.store.get("a")
        self.assertEqual((report["updated"], row["version"], row["stored_size"]), (1, 2, 9))
        self.assertIsNone(row["sha256"])

    def test_missing_file_is_marked_stale_not_dropped(self):
        self.aegis.add("a", 5)
        self.aegis.add("b", 5)
        self.reconcile()
        del self.aegis.entries["a"]
        report = self.reconcile()
        self.assertEqual(report["marked_stale"], 1)
        row = self.store.get("a")
        self.assertEqual((row["status"], row["version"]), ("stale", 1))
        self.assertIn("a", [r["name"] for r in self.store.by_attribute("status=stale")[0]])
        again = self.reconcile()
        self.assertEqual((again["marked_stale"], again["already_missing"]), (0, 1))

    def test_partial_scan_never_marks_stale(self):
        self.aegis.add("a", 5)
        self.reconcile()
        del self.aegis.entries["a"]
        self.aegis.unreachable = ["n3:1"]
        report = self.reconcile()
        self.assertEqual((report["marked_stale"], report["marked_incomplete_scan"]), (0, 1))
        self.assertEqual(self.store.get("a")["status"], "incomplete")

    def test_file_back_again_returns_to_indexed(self):
        self.aegis.add("a", 5)
        self.reconcile()
        saved = self.aegis.entries.pop("a")
        self.reconcile()
        self.aegis.entries["a"] = saved
        self.reconcile()
        self.assertEqual(self.store.get("a")["status"], "indexed")

    def test_under_replicated_is_incomplete(self):
        self.aegis.add("a", 5, seen_on=["n1:1"])
        report = self.reconcile()
        row = self.store.get("a")
        self.assertEqual((row["status"], report["incomplete_files"]), ("incomplete", 1))
        self.assertIn("1 of 3", row["status_reason"])

    def test_chunk_files_are_not_files_and_mark_the_parent_striped(self):
        self.aegis.add("big.bin", 300)
        for i in range(3):
            self.aegis.add("big.bin.stripe.c1.99.%d" % i, 1000)
        self.aegis.add("lost.bin.stripe.c1.99.0", 1000)
        report = self.reconcile()
        self.assertEqual((report["scanned_files"], report["chunk_files_seen"], report["orphan_chunk_files"]),
                         (1, 4, 1))
        row = self.store.get("big.bin")
        self.assertEqual((row["striped"], row["striped_basis"], row["chunk_files"]),
                         (True, m.STRIPED_CHUNKS, 3))
        self.assertIsNone(row["size"])          # the 300-byte object is the index, not the file
        self.assertEqual(row["stored_size"], 300)
        self.assertIsNone(self.store.get("big.bin.stripe.c1.99.0"))
        self.assertEqual(self.store.count_files(), 1)

    def test_unreachable_nodes_withhold_listing_size(self):
        self.aegis.unreachable = ["n9:1"]
        self.aegis.add("a", 5)
        self.reconcile()
        self.assertIsNone(self.store.get("a")["size"])

    def test_invalid_names_are_counted_not_fatal(self):
        self.aegis.add("good", 5)
        self.aegis.add("bad\nname", 5)
        report = self.reconcile()
        self.assertEqual((report["created"], report["invalid"]), (1, 1))

    def test_cassandra_failure_midway_spools_work(self):
        for i in range(5):
            self.aegis.add("f%d" % i, 5)
        real = self.store.apply
        state = {"n": 0}

        def flaky(o):
            state["n"] += 1
            if state["n"] > 2:
                raise StoreUnavailable("down")
            return real(o)

        self.store.apply = flaky
        report = self.reconcile()
        self.assertEqual(report["spooled"], 3)
        self.assertEqual(self.spool.depth(), 3)
        self.store.apply = real
        drained, _ = indexer.drain(self.store, self.spool, self.aegis, self.settings)
        self.assertEqual((drained.done, self.spool.depth()), (3, 0))
        self.assertEqual(self.store.count_files(), 5)

    def test_reconcile_replays_queue_first(self):
        self.aegis.add("a", 5)
        indexer.submit(None, self.spool, indexer.observation_from_entry(
            self.aegis.entries["a"], self.settings, m.utcnow_ms(), "test"))
        self.assertEqual(self.spool.depth(), 1)
        report = self.reconcile()
        self.assertEqual(report["drained"]["done"], 1)
        self.assertEqual(self.spool.depth(), 0)

    def test_aegis_down_fails_without_touching_the_index(self):
        self.aegis.add("a", 5)
        self.reconcile()
        self.aegis.down = True
        with self.assertRaises(AegisError):
            self.reconcile()
        self.assertEqual(self.store.get("a")["status"], "indexed")


class VerifyTest(IndexerCase):
    def test_checksum_recorded_only_when_read(self):
        self.aegis.add("a", 5, data=b"hello")
        self.reconcile()
        self.assertIsNone(self.store.get("a")["sha256"])
        report = self.reconcile(verify=True)
        row = self.store.get("a")
        self.assertEqual(report["verified"], 1)
        self.assertEqual(row["sha256"], hashlib.sha256(b"hello").hexdigest())
        self.assertEqual((row["verified_size"], row["size_basis"], row["verify_state"]), (5, "read", "verified"))
        self.assertIsNotNone(row["verified_at"])

    def test_verified_file_is_not_read_again(self):
        self.aegis.add("a", 5, data=b"hello")
        self.reconcile(verify=True)
        self.aegis.gets.clear()
        self.reconcile(verify=True)
        self.assertEqual(self.aegis.gets, [])

    def test_max_bytes_skips_large_files(self):
        self.aegis.add("small", 5, data=b"hello")
        self.aegis.add("large", 5000)
        report = self.reconcile(verify=True, max_bytes=100)
        self.assertEqual((report["verified"], report["verify_skipped"]), (1, 1))
        row = self.store.get("large")
        self.assertEqual((row["sha256"], row["verify_state"]), (None, "skipped_too_large"))
        self.assertNotIn("large", self.aegis.gets)

    def test_failed_read_is_recorded_without_a_checksum(self):
        self.aegis.add("a", 5)
        self.aegis.read_fails.add("a")
        report = self.reconcile(verify=True)
        row = self.store.get("a")
        self.assertEqual((report["verify_failed"], row["sha256"], row["verify_state"]), (1, None, "failed"))

    def test_checksum_discarded_if_the_file_changed_during_the_read(self):
        self.aegis.add("a", 5, data=b"hello")
        original_get = self.aegis.get

        def bump_then_get(name, dest, quorum=False):
            fields = original_get(name, dest)
            self.aegis.entries[name].version += 1
            return fields

        self.aegis.get = bump_then_get
        self.reconcile(verify=True)
        row = self.store.get("a")
        self.assertIsNone(row["sha256"])
        self.assertEqual(row["verify_state"], "failed")


class DrainTest(IndexerCase):
    def touch(self, name, op="create"):
        self.spool.append({"type": "touch", "op": op, "name": name, "attempts": 0})

    def test_touch_events_are_resolved_from_what_aegisfs_reports(self):
        self.aegis.add("x/y", 7)
        self.touch("x/y")
        self.touch("x/y", "append")
        report, details = indexer.drain(self.store, self.spool, self.aegis, self.settings)
        self.assertEqual(report.done, 2)
        self.assertEqual((details["created"], details["updated"]), (1, 1))  # a later look refreshes observed_at
        self.assertEqual(self.store.get("x/y")["stored_size"], 7)

    def test_touch_for_unknown_file_retries_then_gives_up(self):
        self.touch("ghost")
        for _ in range(4):
            report, _ = indexer.drain(self.store, self.spool, self.aegis, self.settings)
            self.assertEqual(report.retried, 1)
        report, _ = indexer.drain(self.store, self.spool, self.aegis, self.settings)
        self.assertEqual((report.rejected, report.remaining), (1, 0))
        self.assertEqual(self.spool.rejected_count(), 1)

    def test_chunk_touch_resolves_to_the_parent(self):
        self.aegis.add("big", 300)
        self.aegis.add("big.stripe.c.1.0", 100)
        self.touch("big.stripe.c.1.0")
        indexer.drain(self.store, self.spool, self.aegis, self.settings)
        self.assertEqual(self.store.get("big")["striped"], True)

    def test_aegis_down_keeps_events(self):
        self.touch("a")
        self.aegis.down = True
        report, _ = indexer.drain(self.store, self.spool, self.aegis, self.settings)
        self.assertEqual((report.retried, report.remaining), (1, 1))

    def test_invalid_queued_observation_is_rejected_not_retried_forever(self):
        bad = s.obs("ok", -1).to_json()
        self.spool.append({"type": "observation", "obs": bad})
        report, _ = indexer.drain(self.store, self.spool, self.aegis, self.settings)
        self.assertEqual((report.rejected, report.remaining), (1, 0))

    def test_outage_during_drain_loses_nothing(self):
        self.aegis.add("a", 1)
        self.aegis.add("b", 1)
        self.touch("a")
        self.touch("b")
        self.store.backend.fail = StoreUnavailable("down")
        report, _ = indexer.drain(self.store, self.spool, self.aegis, self.settings)
        self.assertTrue(report.stopped_early)
        self.assertEqual(self.spool.depth(), 2)
        self.store.backend.fail = None
        report, _ = indexer.drain(self.store, self.spool, self.aegis, self.settings)
        self.assertEqual((report.done, self.spool.depth()), (2, 0))

    def test_submit_spools_when_store_is_down_and_rejects_invalid(self):
        good = s.obs("a", 1, 0)
        self.assertEqual(indexer.submit(None, self.spool, good), "spooled")
        with self.assertRaises(m.ObservationError):
            indexer.submit(None, self.spool, s.obs("a", -1))
        self.assertEqual(self.spool.depth(), 1)


class ConfirmTest(IndexerCase):
    def indexed(self, name="a", data=b"hello", verify=True):
        self.aegis.add(name, len(data), data=data)
        self.reconcile(verify=verify)
        return self.store.get(name)

    def test_agrees_when_nothing_changed(self):
        row = self.indexed()
        result = queries.confirm(self.aegis, row, self.settings, read=True)
        self.assertTrue(result["agrees"], result)
        self.assertTrue(result["read_check"]["matches_index_checksum"])

    def test_reports_version_change(self):
        row = self.indexed()
        self.aegis.entries["a"].version = 2
        result = queries.confirm(self.aegis, row, self.settings)
        self.assertFalse(result["agrees"])
        self.assertIn("version", result["differences"][0])

    def test_content_changed_without_version_change_is_a_checksum_disagreement(self):
        row = self.indexed()
        self.aegis.contents["a"] = b"HELLO"
        result = queries.confirm(self.aegis, row, self.settings, read=True)
        self.assertFalse(result["agrees"])
        self.assertFalse(result["read_check"]["matches_index_checksum"])

    def test_aegisfs_read_failure_is_surfaced_not_overridden_by_the_index(self):
        row = self.indexed()
        self.aegis.read_fails.add("a")
        result = queries.confirm(self.aegis, row, self.settings, read=True)
        self.assertFalse(result["agrees"])
        self.assertIn("CHECKSUM_MISMATCH", result["aegisfs_error"])

    def test_file_gone_from_aegisfs(self):
        row = self.indexed()
        del self.aegis.entries["a"]
        result = queries.confirm(self.aegis, row, self.settings)
        self.assertFalse(result["agrees"])
        self.assertIsNone(result["live"])

    def test_every_payload_is_labelled(self):
        payload = queries.labelled({"x": 1}, s.at(0))
        self.assertFalse(payload["index"]["authoritative"])
        self.assertEqual(payload["index"]["observed_at"], "2026-01-01T12:00:00.000Z")


if __name__ == "__main__":
    unittest.main()
