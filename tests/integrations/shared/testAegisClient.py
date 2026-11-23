"""AegisClient against a fake `aegisfs` binary, so the parsing and error handling are covered
without a running cluster. The live paths are covered by the Cassandra and HDFS live tests."""
import os
import stat
import sys
import tempfile
import unittest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "extensions", "integrations"))
sys.path.insert(0, ROOT)

from aegisfsIntegrations.aegisfsClient import (  # noqa: E402
    AegisClient, AegisError, AegisUnavailable, is_stripe_chunk, parse_fields, sha256_file)

FAKE = r'''#!/usr/bin/env bash
# args: [--insecure] [--stripe-mib N] --entry ADDR <command> ...
while [ "$1" != "--entry" ]; do shift; done
shift; entry="$1"; shift; cmd="$1"; shift
case "$cmd" in
  health) echo status=OK ;;
  members)
    echo status=OK; echo count=3
    echo m0_node=a; echo m0_addr=h:1; echo m0_state=ALIVE; echo m0_inc=1
    echo m1_node=b; echo m1_addr=h:2; echo m1_state=FAILED; echo m1_inc=2
    echo m2_node=c; echo m2_addr=h:3; echo m2_state=ALIVE; echo m2_inc=1 ;;
  listStore)
    echo status=OK
    case "$entry" in
      h:1) echo count=2; echo f0_name=x/one; echo f0_id=11; echo f0_size=10; echo f0_version=1; echo f0_segs=1
           echo f1_name=x/two; echo f1_id=22; echo f1_size=5; echo f1_version=1; echo f1_segs=1 ;;
      h:3) echo count=1; echo f0_name=x/one; echo f0_id=11; echo f0_size=14; echo f0_version=2; echo f0_segs=2 ;;
      *) echo status=NETWORK_ERROR ;;
    esac ;;
  ls)
    echo status=OK; echo replicas=2
    echo r0_node=a; echo r0_addr=h:1; echo r0_present=yes
    echo r1_node=c; echo r1_addr=h:3; echo r1_present=no ;;
  get) if [ "$1" = missing ]; then echo status=NOT_FOUND; exit 1; fi; printf 'data' > "$2"; echo status=OK ;;
  create) echo status=DEGRADED; echo committed=2 ;;
  *) echo status=INVALID_ARGUMENT; exit 2 ;;
esac
'''


class AegisClientTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dir = tempfile.mkdtemp()
        cls.binary = os.path.join(cls.dir, "aegisfs")
        with open(cls.binary, "w") as handle:
            handle.write(FAKE)
        os.chmod(cls.binary, os.stat(cls.binary).st_mode | stat.S_IEXEC)

    def client(self, **kw):
        return AegisClient(binary=self.binary, entry="h:1", **kw)

    def test_parse_fields_ignores_lines_without_equals(self):
        self.assertEqual(parse_fields("status=OK\nnoise\na=b=c\n"), {"status": "OK", "a": "b=c"})

    def test_health(self):
        self.assertTrue(self.client().health())

    def test_members_are_parsed_with_addresses_and_states(self):
        members = self.client().members()
        self.assertEqual([(m.node, m.address, m.state) for m in members],
                         [("a", "h:1", "ALIVE"), ("b", "h:2", "FAILED"), ("c", "h:3", "ALIVE")])

    def test_inventory_skips_failed_nodes_and_prefers_the_highest_version(self):
        found, unreachable = self.client().inventory()
        self.assertEqual(sorted(found), ["x/one", "x/two"])
        self.assertEqual((found["x/one"].version, found["x/one"].size), (2, 14))
        self.assertEqual(sorted(found["x/one"].seen_on), ["h:1", "h:3"])
        self.assertEqual(unreachable, [])

    def test_placement_reports_what_each_replica_holds(self):
        reps = self.client().placement("x/one")
        self.assertEqual([(r.address, r.present) for r in reps], [("h:1", True), ("h:3", False)])

    def test_get_writes_the_file_and_maps_failures(self):
        dest = os.path.join(self.dir, "out")
        self.client().get("x/one", dest)
        self.assertEqual(open(dest).read(), "data")
        with self.assertRaises(AegisError) as ctx:
            self.client().get("missing", dest)
        self.assertEqual(ctx.exception.status, "NOT_FOUND")

    def test_create_treats_degraded_as_a_failure_unless_asked(self):
        with self.assertRaises(AegisError) as ctx:
            self.client().create("/dev/null", "x/new")
        self.assertEqual(ctx.exception.status, "DEGRADED")
        self.assertEqual(self.client().create_degraded_ok("/dev/null", "x/new")["status"], "DEGRADED")

    def test_missing_binary_and_unreachable_cluster_are_reported_as_unavailable(self):
        with self.assertRaises(AegisUnavailable):
            AegisClient(binary="/nonexistent/aegisfs").members()
        with self.assertRaises(AegisUnavailable):
            self.client().list_store("h:9")

    def test_stripe_chunk_names_are_recognised(self):
        self.assertTrue(is_stripe_chunk("a/b.stripe.cli-1.99.0"))
        self.assertFalse(is_stripe_chunk("a/b"))

    def test_sha256_file_streams(self):
        path = os.path.join(self.dir, "h")
        with open(path, "wb") as handle:
            handle.write(b"abc")
        self.assertEqual(sha256_file(path),
                         "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")


if __name__ == "__main__":
    unittest.main()
