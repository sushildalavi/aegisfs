"""https behaviour of the client, against the fake server with a throwaway certificate."""
import os
import shutil
import subprocess
import tempfile
import unittest

import testSupport  # noqa: F401
from fakeWebHdfs import FakeHdfs
from testSupport import make_client

from aegisfsIntegrations.hdfs.webHdfs import HdfsProtocolError, HdfsUnavailable


@unittest.skipUnless(shutil.which("openssl"), "openssl is not installed")
class TlsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dir = tempfile.mkdtemp()
        d = cls.dir

        def run(*cmd):
            subprocess.run(cmd, cwd=d, check=True, capture_output=True)
        run("openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
            "-subj", "/CN=test-ca", "-keyout", "ca.key", "-out", "ca.pem")
        run("openssl", "req", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN=localhost",
            "-keyout", "node.key", "-out", "node.csr")
        with open(os.path.join(d, "ext"), "w") as handle:
            handle.write("subjectAltName=IP:127.0.0.1,DNS:localhost\n")
        run("openssl", "x509", "-req", "-in", "node.csr", "-CA", "ca.pem", "-CAkey", "ca.key",
            "-CAcreateserial", "-days", "2", "-extfile", "ext", "-out", "node.pem")
        cls.ca = os.path.join(d, "ca.pem")
        cls.tls = (os.path.join(d, "node.pem"), os.path.join(d, "node.key"))

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.dir, ignore_errors=True)

    def setUp(self):
        self.fake = FakeHdfs(tls=self.tls)
        self.addCleanup(self.fake.close)
        self.payload = os.path.join(self.dir, "p.bin")
        with open(self.payload, "wb") as handle:
            handle.write(b"over tls")

    def test_round_trip_with_the_right_ca(self):
        client = make_client(self.fake, ca_file=self.ca, allow_insecure_http=False)
        client.upload(self.payload, "/t.bin", 8)
        out = bytearray()
        client.download("/t.bin", out.extend)
        self.assertEqual(bytes(out), b"over tls")

    def test_unknown_ca_is_rejected_not_ignored(self):
        client = make_client(self.fake, allow_insecure_http=False, retries=0)
        with self.assertRaisesRegex(HdfsUnavailable, "certificate verification failed"):
            client.status("/")
        self.assertEqual(self.fake.requests, [])

    def test_https_namenode_redirecting_to_http_is_refused(self):
        self.fake.location_override = lambda loc: loc.replace("https://", "http://")
        client = make_client(self.fake, ca_file=self.ca, allow_insecure_http=False)
        with self.assertRaisesRegex(HdfsProtocolError, "changes the scheme"):
            client.upload(self.payload, "/t.bin", 8)
        self.assertEqual([r for r in self.fake.requests if r[0] == "dn"], [])


if __name__ == "__main__":
    unittest.main()
