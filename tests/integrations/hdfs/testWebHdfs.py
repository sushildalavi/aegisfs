"""Unit tests for the WebHDFS client against the in-process fake server."""
import os
import tempfile
import unittest

import testSupport  # noqa: F401  (sets up sys.path)
from fakeWebHdfs import FakeHdfs
from testSupport import make_client, sha

from aegisfsIntegrations.hdfs import webHdfs
from aegisfsIntegrations.hdfs.webHdfs import (HdfsAccessDenied, HdfsBadRequest, HdfsExists,
                                              HdfsNotFound, HdfsProtocolError, HdfsUnavailable,
                                              parse_namenode_url, validate_redirect)


class ClientTestCase(unittest.TestCase):
    def setUp(self):
        self.fake = FakeHdfs()
        self.addCleanup(self.fake.close)
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def local(self, data, name="in.bin"):
        path = os.path.join(self.tmp.name, name)
        with open(path, "wb") as handle:
            handle.write(data)
        return path


class RedirectTests(ClientTestCase):
    def test_upload_and_download_follow_validated_redirect(self):
        client = make_client(self.fake)
        data = os.urandom(300000)
        client.upload(self.local(data), "/d/file.bin", len(data))
        self.assertEqual(self.fake.files["/d/file.bin"], data)
        chunks = []
        total = client.download("/d/file.bin", chunks.append, len(data))
        self.assertEqual(total, len(data))
        self.assertEqual(b"".join(chunks), data)
        roles = [(r[0], r[1]) for r in self.fake.requests if r[3].get("op") in ("CREATE", "OPEN")]
        self.assertEqual(roles, [("nn", "PUT"), ("dn", "PUT"), ("nn", "GET"), ("dn", "GET")])

    def test_streaming_uses_bounded_chunks(self):
        client = make_client(self.fake, chunk_size=16384)
        data = os.urandom(200000)
        client.upload(self.local(data), "/s.bin", len(data))
        self.assertLessEqual(self.fake.max_upload_read, 65536 * 16)
        sizes = []
        client.download("/s.bin", lambda b: sizes.append(len(b)))
        self.assertLessEqual(max(sizes), 16384)
        self.assertGreater(len(sizes), 5)

    def test_empty_file_round_trip(self):
        client = make_client(self.fake)
        client.upload(self.local(b""), "/empty", 0)
        self.assertEqual(self.fake.files["/empty"], b"")
        chunks = []
        self.assertEqual(client.download("/empty", chunks.append, 0), 0)

    def test_unusual_characters_in_path(self):
        client = make_client(self.fake)
        data = b"hello"
        client.upload(self.local(data), "/a b/ü #1.txt", 5)
        self.assertEqual(self.fake.files["/a b/ü #1.txt"], data)

    def test_redirect_to_host_outside_allowlist_is_refused_before_any_data_moves(self):
        self.fake.location_override = lambda loc: loc.replace("127.0.0.1", "localhost")
        client = make_client(self.fake)
        with self.assertRaises(HdfsProtocolError) as ctx:
            client.upload(self.local(b"secret bytes"), "/x", 12)
        self.assertIn("not the NameNode", str(ctx.exception))
        self.assertEqual([r for r in self.fake.requests if r[0] == "dn"], [])

    def test_allowlisted_host_is_accepted(self):
        self.fake.location_override = lambda loc: loc.replace("127.0.0.1", "localhost")
        client = make_client(self.fake, allowed_redirect_hosts=["localhost"])
        client.upload(self.local(b"ok"), "/x", 2)
        self.assertEqual(self.fake.files["/x"], b"ok")

    def test_credentials_are_not_added_to_the_datanode_request(self):
        client = make_client(self.fake, user="alice")
        client.upload(self.local(b"ok"), "/x", 2)
        dn = [r for r in self.fake.requests if r[0] == "dn"][0]
        # Only what the NameNode wrote into the Location reached the DataNode.
        self.assertEqual(set(dn[3]) - {"op", "user.name", "overwrite"}, set())

    def test_token_auth_sends_delegation_and_not_user_name_to_the_namenode(self):
        client = make_client(self.fake, user=None, token="tok-123")
        client.status("/")
        nn = self.fake.requests[0]
        self.assertEqual(nn[3].get("delegation"), "tok-123")
        self.assertNotIn("user.name", nn[3])

    def test_token_never_appears_in_error_text(self):
        client = make_client(self.fake, user=None, token="tok-SECRET")
        self.fake.nn_errors = [(403, "AccessControlException", "denied")]
        with self.assertRaises(HdfsAccessDenied) as ctx:
            client.status("/x")
        self.assertNotIn("tok-SECRET", str(ctx.exception))
        self.fake.location_override = lambda loc: loc.replace("127.0.0.1", "localhost")
        with self.assertRaises(HdfsProtocolError) as ctx:
            client.upload(self.local(b"x"), "/y", 1)
        self.assertNotIn("tok-SECRET", str(ctx.exception))

    def test_redirect_loop_is_detected(self):
        nn_port = self.fake.nn.server_address[1]
        self.fake.location_override = lambda loc: loc.replace(str(self.fake.dn_port), str(nn_port))
        client = make_client(self.fake)
        with self.assertRaises(HdfsProtocolError) as ctx:
            client.upload(self.local(b"x"), "/loop", 1)
        self.assertRegex(str(ctx.exception), "loop|redirects")

    def test_redirect_count_is_bounded(self):
        counter = {"n": 0}
        nn_port = self.fake.nn.server_address[1]

        def override(loc):
            counter["n"] += 1
            return loc.replace(str(self.fake.dn_port), str(nn_port)) + "&hop=%d" % counter["n"]
        self.fake.location_override = override
        client = make_client(self.fake, max_redirects=2)
        with self.assertRaises(HdfsProtocolError) as ctx:
            client.upload(self.local(b"x"), "/hops", 1)
        self.assertIn("more than 2 redirects", str(ctx.exception))


class ValidateRedirectTests(unittest.TestCase):
    base = "http://nn:9870/webhdfs/v1/x?op=OPEN"

    def check(self, location, scheme="http", allowed=()):
        return validate_redirect(location, self.base, scheme, "nn", allowed)

    def test_https_to_http_downgrade_refused(self):
        with self.assertRaisesRegex(HdfsProtocolError, "changes the scheme"):
            validate_redirect("http://nn:9864/x", "https://nn:9871/x", "https", "nn", [])

    def test_http_to_https_also_refused(self):
        with self.assertRaisesRegex(HdfsProtocolError, "changes the scheme"):
            self.check("https://nn:9864/x")

    def test_userinfo_refused(self):
        with self.assertRaisesRegex(HdfsProtocolError, "user information"):
            self.check("http://bob:pw@nn:9864/x")
        with self.assertRaisesRegex(HdfsProtocolError, "user information"):
            self.check("http://nn@evil:9864/x")

    def test_unknown_host_refused_and_listed_host_accepted(self):
        with self.assertRaises(HdfsProtocolError):
            self.check("http://dn1:9864/x")
        self.assertTrue(self.check("http://DN1:9864/x", allowed=["dn1"]).startswith("http://DN1"))

    def test_namenode_host_is_always_allowed(self):
        self.assertEqual(self.check("http://nn:1234/y"), "http://nn:1234/y")

    def test_relative_location_resolves_against_current_url(self):
        self.assertEqual(self.check("/webhdfs/v1/y?op=OPEN"), "http://nn:9870/webhdfs/v1/y?op=OPEN")

    def test_other_schemes_and_control_characters_refused(self):
        for bad in ("file:///etc/passwd", "ftp://nn/x", "gopher://nn/x", "http://nn/x\r\nSet: y"):
            with self.assertRaises(HdfsProtocolError):
                self.check(bad)

    def test_bad_port_refused(self):
        with self.assertRaises(HdfsProtocolError):
            self.check("http://nn:99999/x")


class ConfigurationRefusalTests(unittest.TestCase):
    def test_plain_http_refused_without_the_switch(self):
        with self.assertRaisesRegex(HdfsProtocolError, "plain http://"):
            parse_namenode_url("http://nn:9870")
        self.assertEqual(parse_namenode_url("http://nn:9870", True), ("http", "nn", 9870))

    def test_https_default_port_and_no_switch_needed(self):
        self.assertEqual(parse_namenode_url("https://NN.example"), ("https", "nn.example", 443))

    def test_bad_namenode_urls(self):
        for bad in ("", "ftp://nn", "https://user:pw@nn", "https://nn/webhdfs", "https://nn?x=1",
                    "nn:9870", "https://", "https://nn/ x"):
            with self.assertRaises(HdfsProtocolError, msg=bad):
                parse_namenode_url(bad, True)

    def test_https_client_verifies_certificates(self):
        client = webHdfs.WebHdfsClient(webHdfs.HdfsSettings("https://nn.invalid:9871", user="u"))
        import ssl
        self.assertEqual(client._ssl.verify_mode, ssl.CERT_REQUIRED)
        self.assertTrue(client._ssl.check_hostname)


class ErrorMappingTests(ClientTestCase):
    def test_remote_exceptions_map_to_specific_errors(self):
        client = make_client(self.fake)
        with self.assertRaises(HdfsNotFound):
            client.status("/missing")
        self.fake.nn_errors = [(403, "AccessControlException", "Permission denied")]
        with self.assertRaises(HdfsAccessDenied):
            client.status("/")
        self.fake.nn_errors = [(403, "FileAlreadyExistsException", "exists")]
        with self.assertRaises(HdfsExists):
            client.status("/")
        self.fake.nn_errors = [(400, "IllegalArgumentException", "bad")]
        with self.assertRaises(HdfsBadRequest):
            client.status("/")

    def test_plain_401_and_403_are_access_errors(self):
        client = make_client(self.fake)
        for status in (401, 403):
            self.fake.nn_errors = [status]
            with self.assertRaises(HdfsAccessDenied):
                client.status("/")

    def test_server_errors_become_unavailable_after_bounded_retries(self):
        client = make_client(self.fake, retries=2)
        self.fake.nn_errors = [500, 500, 500, 500]
        with self.assertRaises(HdfsUnavailable):
            client.status("/")
        self.assertEqual(len(self.fake.requests), 3)  # one try plus two retries

    def test_transient_status_is_retried_then_succeeds(self):
        client = make_client(self.fake, retries=3)
        self.fake.nn_errors = [503, 502]
        self.assertEqual(client.status("/")["type"], "DIRECTORY")
        self.assertEqual(len(self.fake.requests), 3)

    def test_rename_is_not_retried(self):
        client = make_client(self.fake, retries=3)
        self.fake.put_file("/a", b"1")
        self.fake.nn_errors = [503]
        with self.assertRaises(HdfsUnavailable):
            client.rename("/a", "/b")
        self.assertEqual(len(self.fake.requests), 1)

    def test_datanode_error_on_upload_is_mapped(self):
        client = make_client(self.fake)
        self.fake.dn_error = (403, "AccessControlException", "no write")
        with self.assertRaises(HdfsAccessDenied):
            client.upload(self.local(b"x"), "/f", 1)

    def test_unreachable_namenode(self):
        fake = FakeHdfs()
        url = fake.nn_url
        fake.close()
        client = make_client(fake, namenode_url=url, retries=1)
        with self.assertRaises(HdfsUnavailable):
            client.status("/")

    def test_timeout_is_reported_as_unavailable(self):
        self.fake.stall_upload = True
        client = make_client(self.fake, timeout=1, retries=0)
        with self.assertRaises(HdfsUnavailable):
            client.upload(self.local(b"x" * 10), "/stall", 10)

    def test_create_fails_if_file_exists(self):
        client = make_client(self.fake)
        self.fake.put_file("/there", b"old")
        with self.assertRaises(HdfsExists):
            client.upload(self.local(b"new"), "/there", 3)
        self.assertEqual(self.fake.files["/there"], b"old")


class DownloadResumeTests(ClientTestCase):
    def test_dropped_connection_resumes_at_the_right_offset(self):
        data = os.urandom(500000)
        self.fake.put_file("/big", data)
        self.fake.drop_open_after = 123456
        self.fake.open_drops_left = 2
        client = make_client(self.fake, retries=3)
        got = bytearray()
        client.download("/big", got.extend, len(data))
        self.assertEqual(sha(bytes(got)), sha(data))
        offsets = [r[3].get("offset") for r in self.fake.requests if r[3].get("op") == "OPEN"
                   and r[0] == "nn"]
        self.assertEqual(offsets[0], None)
        self.assertEqual(len(offsets), 3)

    def test_gives_up_after_the_retry_limit(self):
        self.fake.put_file("/big", os.urandom(100000))
        self.fake.drop_open_after = 1000
        self.fake.open_drops_left = 99
        client = make_client(self.fake, retries=1)
        with self.assertRaises(HdfsUnavailable):
            client.download("/big", lambda b: None, 100000)

    def test_a_failing_sink_is_not_retried_as_a_network_error(self):
        self.fake.put_file("/f", b"abc")
        client = make_client(self.fake, retries=3)

        def sink(block):
            raise OSError("disk full")
        with self.assertRaisesRegex(OSError, "disk full"):
            client.download("/f", sink)
        self.assertEqual(len([r for r in self.fake.requests if r[3].get("op") == "OPEN"
                              and r[0] == "nn"]), 1)


class OtherOperationTests(ClientTestCase):
    def test_delete_refuses_anything_that_is_not_our_temp_name(self):
        client = make_client(self.fake)
        self.fake.put_file("/precious.txt", b"x")
        for path in ("/precious.txt", "/.x.aegis-tmp-zz", "/x.aegis-tmp-0123456789abcdef"):
            with self.assertRaises(HdfsBadRequest):
                client.delete_temp(path)
        self.assertIn("/precious.txt", self.fake.files)
        self.fake.put_file("/.n.aegis-tmp-0123456789abcdef", b"t")
        self.assertTrue(client.delete_temp("/.n.aegis-tmp-0123456789abcdef"))

    def test_rename_overwrite_uses_the_option(self):
        client = make_client(self.fake)
        self.fake.put_file("/a", b"new")
        self.fake.put_file("/b", b"old")
        self.assertFalse(client.rename("/a", "/b"))
        self.assertEqual(self.fake.files["/b"], b"old")
        self.assertTrue(client.rename("/a", "/b", overwrite=True))
        self.assertEqual(self.fake.files["/b"], b"new")

    def test_checksum_follows_redirect(self):
        self.fake.put_file("/c", b"abc")
        self.assertIn("algorithm", make_client(self.fake).checksum("/c"))


if __name__ == "__main__":
    unittest.main()
