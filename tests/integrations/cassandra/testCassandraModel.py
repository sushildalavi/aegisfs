"""Validation and merge rules (no database)."""
import unittest
from dataclasses import replace

import cassSupport as s
from aegisfsIntegrations.cassandra import model as m


class ValidationTest(unittest.TestCase):
    def test_valid_observation_passes(self):
        s.obs().validate(now=s.at(10))

    def test_rejects_bad_names(self):
        for bad in ["", None, "x" * 2000, "a\x00b", "line\nbreak"]:
            with self.assertRaises(m.ObservationError, msg=repr(bad)):
                s.obs(name=bad).validate(now=s.at(10))

    def test_chunk_files_are_not_files(self):
        with self.assertRaises(m.ObservationError):
            s.obs(name="big.bin.stripe.c1.123.0").validate(now=s.at(10))

    def test_rejects_bad_numbers(self):
        for field, value in [("version", -1), ("version", "3"), ("version", True), ("stored_size", -5),
                             ("chunk_files", 1.5)]:
            with self.assertRaises(m.ObservationError, msg=field):
                s.obs(**{field: value}).validate(now=s.at(10))
        with self.assertRaises(m.ObservationError):
            s.obs(version=None).validate(now=s.at(10))

    def test_rejects_future_timestamps(self):
        with self.assertRaises(m.ObservationError):
            s.obs(seconds=10_000).validate(now=s.at(0))

    def test_rejects_unknown_status_and_bad_replicas(self):
        with self.assertRaises(m.ObservationError):
            s.obs(status="great").validate(now=s.at(10))
        with self.assertRaises(m.ObservationError):
            s.obs(replicas={"n1": "yes"}).validate(now=s.at(10))
        with self.assertRaises(m.ObservationError):
            s.obs(replicas={"n%d" % i: True for i in range(100)}).validate(now=s.at(10))

    def test_checksum_rules(self):
        good = dict(sha256="a" * 64, verified_at=s.at(5), verified_size=10, size=10, size_basis=m.SIZE_READ)
        s.obs(**good).validate(now=s.at(10))
        for change in [dict(sha256="A" * 64), dict(sha256="abc"), dict(verified_at=None),
                       dict(verified_size=9), dict(size_basis=m.SIZE_LISTSTORE)]:
            with self.assertRaises(m.ObservationError, msg=str(change)):
                s.obs(**{**good, **change}).validate(now=s.at(10))
        with self.assertRaises(m.ObservationError):  # a time without a checksum
            s.obs(verified_at=s.at(1)).validate(now=s.at(10))

    def test_striped_size_must_be_read(self):
        with self.assertRaises(m.ObservationError):
            s.obs(striped=True, striped_basis=m.STRIPED_CHUNKS, size=10,
                  size_basis=m.SIZE_LISTSTORE).validate(now=s.at(10))
        s.obs(striped=True, striped_basis=m.STRIPED_CHUNKS).validate(now=s.at(10))

    def test_size_and_basis_travel_together(self):
        with self.assertRaises(m.ObservationError):
            s.obs(size=5).validate(now=s.at(10))

    def test_absent_cannot_claim_indexed(self):
        with self.assertRaises(m.ObservationError):
            m.Observation(name="a", kind="absent", observed_at=s.at(0), source="t").validate(now=s.at(1))

    def test_json_roundtrip(self):
        o = s.obs(replicas={"n1": True}, replicas_source="listStore", size=10, size_basis=m.SIZE_LISTSTORE)
        again = m.Observation.from_json(o.to_json())
        self.assertEqual(again, o)

    def test_from_json_rejects_junk(self):
        for bad in [[], {"name": "a"}, {"name": "a", "source": "x", "observed_at": "nope"},
                    {"name": "a", "source": "x", "observed_at": "2026-01-01T00:00:00Z", "mystery": 1}]:
            with self.assertRaises(m.ObservationError):
                m.Observation.from_json(bad)


class NamespaceTest(unittest.TestCase):
    def test_namespaces(self):
        self.assertEqual(m.namespace_of("a.txt"), "/")
        self.assertEqual(m.namespace_of("a/b/c.txt"), "a/b")
        self.assertEqual(m.namespace_of("/x/y"), "/x")
        self.assertEqual(m.ancestors_of("a/b"), [("/", "a"), ("a", "a/b")])
        self.assertEqual(m.ancestors_of("/"), [])

    def test_bucket_is_stable_and_bounded(self):
        self.assertEqual(m.bucket_of("hello", 16), m.bucket_of("hello", 16))
        self.assertTrue(all(0 <= m.bucket_of("f%d" % i, 7) < 7 for i in range(500)))
        self.assertEqual(m.bucket_of("hello", 16), 6)  # crc32 is fixed, so this must never change


class MergeTest(unittest.TestCase):
    def merge(self, existing, o):
        return m.merge(existing, o, 16, now=s.at(1000))

    def created(self, **kw):
        r = self.merge(None, s.obs(**kw))
        self.assertEqual(r.outcome, "created")
        return r.row

    def test_create(self):
        row = self.created()
        self.assertEqual(row["namespace"], "a")
        self.assertEqual(row["verify_state"], m.VERIFY_UNVERIFIED)
        self.assertIsNone(row["sha256"])

    def test_newer_version_replaces_and_drops_old_checksum(self):
        row = self.created(version=1, sha256="a" * 64, verified_at=s.at(0), verified_size=10, size=10,
                           size_basis=m.SIZE_READ, verify_state=m.VERIFY_VERIFIED)
        r = self.merge(row, s.obs(version=2, seconds=5, stored_size=20))
        self.assertEqual(r.outcome, "updated")
        self.assertEqual(r.row["version"], 2)
        self.assertIsNone(r.row["sha256"])
        self.assertIsNone(r.row["size"])

    def test_older_version_is_rejected_even_if_observed_later(self):
        row = self.created(version=5, seconds=0)
        r = self.merge(row, s.obs(version=4, seconds=100))
        self.assertEqual((r.outcome, r.reason), ("rejected", m.REJECT_OLDER_VERSION))

    def test_older_observation_same_version_rejected(self):
        row = self.created(version=3, seconds=50)
        r = self.merge(row, s.obs(version=3, seconds=10, stored_size=99))
        self.assertEqual((r.outcome, r.reason), ("rejected", m.REJECT_OLDER_OBSERVATION))

    def test_identical_repeat_is_unchanged(self):
        o = s.obs(version=2, seconds=3, replicas={"n1": True}, replicas_source="listStore")
        row = self.merge(None, o).row
        self.assertEqual(self.merge(row, o).outcome, "unchanged")

    def test_same_version_keeps_checksum_when_new_observation_has_none(self):
        row = self.created(sha256="b" * 64, verified_at=s.at(0), verified_size=10, size=10,
                           size_basis=m.SIZE_READ, verify_state=m.VERIFY_VERIFIED)
        r = self.merge(row, s.obs(seconds=10, replicas={"n1": True}, replicas_source="listStore"))
        self.assertEqual(r.row["sha256"], "b" * 64)
        self.assertEqual(r.row["size_basis"], m.SIZE_READ)

    def test_listing_guess_does_not_replace_verified_striped(self):
        row = self.created(striped=False, striped_basis=m.STRIPED_VERIFIED)
        r = self.merge(row, s.obs(seconds=10, striped=True, striped_basis=m.STRIPED_CHUNKS))
        self.assertFalse(r.row["striped"])

    def test_absent_marks_stale_without_changing_version(self):
        row = self.created(version=4)
        gone = m.Observation(name=row["name"], kind="absent", observed_at=s.at(60), source="t",
                             status=m.STATUS_STALE, status_reason="gone")
        r = self.merge(row, gone)
        self.assertEqual(r.outcome, "updated")
        self.assertEqual((r.row["version"], r.row["status"]), (4, m.STATUS_STALE))

    def test_absent_older_than_row_is_rejected(self):
        row = self.created(version=4, seconds=100)
        gone = m.Observation(name=row["name"], kind="absent", observed_at=s.at(10), source="t",
                             status=m.STATUS_STALE)
        self.assertEqual(self.merge(row, gone).outcome, "rejected")

    def test_absent_for_unknown_file_is_ignored(self):
        gone = m.Observation(name="never/seen", kind="absent", observed_at=s.at(1), source="t",
                             status=m.STATUS_STALE)
        self.assertEqual(self.merge(None, gone).outcome, "ignored")

    def test_attrs(self):
        self.assertEqual(m.attrs_of({"striped": True, "status": "indexed"}), ["striped=true"])
        self.assertEqual(m.attrs_of({"striped": None, "status": "stale"}), ["status=stale"])
        self.assertEqual(m.attrs_of({"striped": False, "status": "indexed"}), [])


if __name__ == "__main__":
    unittest.main()
