"""IndexStore rules against the in-memory back end: ordering, idempotency, listings, attributes."""
import threading
import unittest

import cassSupport as s
from aegisfsIntegrations.cassandra import model as m
from aegisfsIntegrations.cassandra.errors import StoreUnavailable


class ApplyTest(unittest.TestCase):
    def setUp(self):
        self.store = s.memory_store()

    def test_create_then_get(self):
        r = self.store.apply(s.obs("docs/a.txt", 1, 0))
        self.assertEqual(r.outcome, "created")
        row = self.store.get("docs/a.txt")
        self.assertEqual((row["version"], row["namespace"]), (1, "docs"))

    def test_repeated_event_is_idempotent(self):
        o = s.obs("a.txt", 2, 5)
        self.assertEqual(self.store.apply(o).outcome, "created")
        for _ in range(3):
            self.assertEqual(self.store.apply(o).outcome, "unchanged")
        self.assertEqual(len(self.store.backend.rows), 1)

    def test_out_of_order_older_version_does_not_overwrite(self):
        self.store.apply(s.obs("a.txt", 3, 10, stored_size=300))
        r = self.store.apply(s.obs("a.txt", 2, 99, stored_size=200))
        self.assertEqual((r.outcome, r.reason), ("rejected", m.REJECT_OLDER_VERSION))
        self.assertEqual(self.store.get("a.txt")["stored_size"], 300)

    def test_out_of_order_older_timestamp_same_version(self):
        self.store.apply(s.obs("a.txt", 1, 20, status="incomplete", status_reason="x"))
        r = self.store.apply(s.obs("a.txt", 1, 10, status="indexed"))
        self.assertEqual(r.outcome, "rejected")
        self.assertEqual(self.store.get("a.txt")["status"], "incomplete")

    def test_any_arrival_order_converges_to_the_same_row(self):
        import itertools
        events = [s.obs("f", 1, 1, stored_size=1), s.obs("f", 2, 2, stored_size=2),
                  s.obs("f", 2, 3, stored_size=2, status="incomplete", status_reason="r"),
                  s.obs("f", 3, 4, stored_size=3)]
        finals = set()
        for perm in itertools.permutations(events):
            store = s.memory_store()
            for e in perm:
                store.apply(e)
            row = store.get("f")
            finals.add((row["version"], row["stored_size"], row["status"]))
        self.assertEqual(finals, {(3, 3, "indexed")})

    def test_invalid_observation_is_rejected_and_nothing_written(self):
        with self.assertRaises(m.ObservationError):
            self.store.apply(s.obs("a.txt", -3))
        self.assertEqual(self.store.backend.rows, {})

    def test_unavailable_propagates(self):
        self.store.backend.fail = StoreUnavailable("down")
        with self.assertRaises(StoreUnavailable):
            self.store.apply(s.obs())
        self.store.backend.fail = None
        self.assertEqual(self.store.apply(s.obs()).outcome, "created")

    def test_lost_race_is_re_merged(self):
        backend = self.store.backend
        real_insert = backend.insert_row
        state = {"raced": False}

        def racing_insert(row):
            if not state["raced"]:
                state["raced"] = True
                real_insert(dict(row, version=7, observed_at=s.at(50)))  # someone else wins first
                return False
            return real_insert(row)

        backend.insert_row = racing_insert
        r = self.store.apply(s.obs("a.txt", 5, 10))
        self.assertEqual(r.outcome, "rejected")  # the racing writer had version 7
        self.assertEqual(self.store.get("a.txt")["version"], 7)

    def test_concurrent_writers_end_on_highest_version(self):
        errors = []

        def work(version):
            try:
                self.store.apply(s.obs("hot", version, version))
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        threads = [threading.Thread(target=work, args=(v,)) for v in range(1, 40)]
        [t.start() for t in threads]
        [t.join() for t in threads]
        self.assertEqual(errors, [])
        self.assertEqual(self.store.get("hot")["version"], 39)


class ListingTest(unittest.TestCase):
    def setUp(self):
        self.store = s.memory_store()
        for n in ["a.txt", "b.txt", "docs/x.md", "docs/y.md", "docs/sub/z.md", "docs2/q.md",
                  "logs/2026/01/one", "logs/2026/02/two"]:
            self.store.apply(s.obs(n))

    def names(self, prefix, **kw):
        rows, truncated = self.store.namespace_listing(prefix, **kw)
        return sorted(r["name"] for r in rows), truncated

    def test_everything(self):
        self.assertEqual(len(self.names("")[0]), 8)

    def test_directory_prefix_descends(self):
        self.assertEqual(self.names("docs/")[0], ["docs/sub/z.md", "docs/x.md", "docs/y.md"])

    def test_no_recursion(self):
        self.assertEqual(self.names("docs/", recursive=False)[0], ["docs/x.md", "docs/y.md"])

    def test_partial_leaf_prefix(self):
        self.assertEqual(self.names("docs/x")[0], ["docs/x.md"])
        self.assertEqual(self.names("a")[0], ["a.txt"])

    def test_prefix_matching_namespace_names(self):
        self.assertEqual(self.names("docs")[0], ["docs/sub/z.md", "docs/x.md", "docs/y.md", "docs2/q.md"])

    def test_deep(self):
        self.assertEqual(self.names("logs/2026/01")[0], ["logs/2026/01/one"])

    def test_limit_truncates(self):
        names, truncated = self.names("", limit=3)
        self.assertEqual((len(names), truncated), (3, True))

    def test_no_match(self):
        self.assertEqual(self.names("zzz"), ([], False))

    def test_counts(self):
        self.assertEqual(self.store.count_files(), 8)

    def test_known_summaries(self):
        self.assertEqual(len(self.store.known_summaries()), 8)


class AttributeTest(unittest.TestCase):
    def test_attribute_rows_follow_status_changes(self):
        store = s.memory_store()
        store.apply(s.obs("a", 1, 1, striped=True, striped_basis=m.STRIPED_CHUNKS))
        store.apply(s.obs("b", 1, 1, status="incomplete", status_reason="r"))
        self.assertEqual([r["name"] for r in store.by_attribute("striped=true")[0]], ["a"])
        self.assertEqual([r["name"] for r in store.by_attribute("status=incomplete")[0]], ["b"])
        store.apply(s.obs("b", 1, 2, status="indexed"))
        self.assertEqual(store.by_attribute("status=incomplete")[0], [])
        self.assertEqual(store.count_attribute("striped=true"), 1)

    def test_attribute_rows_that_no_longer_match_are_dropped(self):
        store = s.memory_store()
        store.apply(s.obs("a", 1, 1, status="incomplete", status_reason="r"))
        store.apply(s.obs("a", 1, 2, status="indexed"))
        # simulate a crash that left the old attribute row behind
        store.backend.attrs.setdefault(("status=incomplete", store.get("a")["bucket"]), {})["a"] = dict(
            name="a", file_id="x", version=1, status="incomplete", observed_at=s.at(1))
        rows, dropped, _ = store.by_attribute("status=incomplete")
        self.assertEqual((rows, dropped), ([], 1))

    def test_unknown_attribute(self):
        with self.assertRaises(ValueError):
            s.memory_store().by_attribute("color=red")

    def test_replay_heals_missing_derived_rows(self):
        store = s.memory_store()
        o = s.obs("a/b", 1, 1, striped=True, striped_basis=m.STRIPED_CHUNKS)
        store.apply(o)
        store.backend.summary.clear()
        store.backend.attrs.clear()
        self.assertEqual(store.apply(o).outcome, "unchanged")
        self.assertEqual(store.count_files(), 1)
        self.assertEqual(store.count_attribute("striped=true"), 1)


if __name__ == "__main__":
    unittest.main()
