"""The local retry queue."""
import json
import os
import tempfile
import threading
import unittest

import cassSupport as s  # noqa: F401  (sets the import path)
from aegisfsIntegrations.cassandra.errors import StoreUnavailable
from aegisfsIntegrations.cassandra.hook import observe
from aegisfsIntegrations.cassandra.spool import Spool


class SpoolTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.spool = Spool(os.path.join(self.tmp.name, "spool"))

    def tearDown(self):
        self.tmp.cleanup()

    def test_append_is_durable_and_counted(self):
        for i in range(5):
            self.spool.append({"type": "touch", "name": "f%d" % i})
        self.assertEqual(self.spool.depth(), 5)
        self.assertEqual(len(self.spool.peek()), 5)

    def test_drain_done_removes_events(self):
        for i in range(3):
            self.spool.append({"n": i})
        report = self.spool.drain(lambda e: "done")
        self.assertEqual((report.done, report.remaining), (3, 0))
        self.assertEqual(self.spool.depth(), 0)

    def test_outage_keeps_everything_not_yet_done(self):
        for i in range(6):
            self.spool.append({"n": i})
        seen = []

        def handler(event):
            if event["n"] == 3:
                raise StoreUnavailable("down")
            seen.append(event["n"])
            return "done"

        report = self.spool.drain(handler)
        self.assertTrue(report.stopped_early)
        self.assertEqual(seen, [0, 1, 2])
        self.assertEqual(self.spool.depth(), 3)
        again = self.spool.drain(lambda e: "done")
        self.assertEqual((again.done, again.remaining), (3, 0))

    def test_retry_and_reject(self):
        self.spool.append({"n": 1})
        self.spool.append({"n": 2})
        report = self.spool.drain(lambda e: "retry" if e["n"] == 1 else "bad event")
        self.assertEqual((report.retried, report.rejected, report.remaining), (1, 1, 1))
        with open(os.path.join(self.spool.dir, "rejected.jsonl")) as handle:
            self.assertEqual(json.loads(handle.readline())["reason"], "bad event")
        self.assertEqual(self.spool.rejected_count(), 1)

    def test_torn_line_is_set_aside(self):
        self.spool.append({"n": 1})
        with open(os.path.join(self.spool.dir, "pending.jsonl"), "a") as handle:
            handle.write('{"n": 2, "oops')  # crash mid-write, no newline
        report = self.spool.drain(lambda e: "done")
        self.assertEqual((report.done, report.rejected), (1, 1))

    def test_batch_left_by_a_dead_drain_is_picked_up(self):
        os.makedirs(self.spool.dir, exist_ok=True)
        path = os.path.join(self.spool.dir, "draining-999999999-1.jsonl")  # no such process
        with open(path, "w") as handle:
            handle.write(json.dumps({"n": 9}) + "\n")
        self.assertEqual(self.spool.depth(), 1)
        report = self.spool.drain(lambda e: "done")
        self.assertEqual(report.done, 1)
        self.assertFalse(os.path.exists(path))

    def test_concurrent_appends_lose_nothing(self):
        def work(k):
            for i in range(50):
                self.spool.append({"k": k, "i": i})
        threads = [threading.Thread(target=work, args=(k,)) for k in range(8)]
        [t.start() for t in threads]
        [t.join() for t in threads]
        self.assertEqual(self.spool.depth(), 400)

    def test_empty_spool(self):
        self.assertEqual(self.spool.depth(), 0)
        self.assertEqual(self.spool.drain(lambda e: "done").done, 0)


class HookTest(unittest.TestCase):
    def test_hook_queues_without_network(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertTrue(observe("create", "a/b.txt", spool_dir=tmp))
            event = json.loads(Spool(tmp).peek()[0])
            self.assertEqual((event["type"], event["name"], event["op"]), ("touch", "a/b.txt", "create"))

    def test_hook_never_raises(self):
        self.assertFalse(observe("create", ""))
        self.assertFalse(observe("create", None))
        self.assertFalse(observe("create", "x", spool_dir="/proc/definitely/not/writable"))


if __name__ == "__main__":
    unittest.main()
