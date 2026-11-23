"""Shared helpers for the HDFS tests: import path setup and a fake AegisFS client."""
import hashlib
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
for candidate in (os.path.join(REPO, "extensions", "integrations"),
                  "/opt/aegisfs-integrations", HERE):
    if os.path.isdir(candidate) and candidate not in sys.path:
        sys.path.insert(0, candidate)

from aegisfsIntegrations.aegisfsClient import AegisError  # noqa: E402
from aegisfsIntegrations.hdfs.webHdfs import HdfsSettings, WebHdfsClient  # noqa: E402


class FakeAegis:
    """Stands in for AegisClient: whole files kept in memory, no network."""

    def __init__(self):
        self.store = {}
        self.create_status = "OK"
        self.corrupt_get = False
        self.stripe_mib = None
        self.created = []

    def exists(self, name):
        return name in self.store

    def get(self, name, dest):
        if name not in self.store:
            raise AegisError("NOT_FOUND", "no such file")
        data = self.store[name]
        if self.corrupt_get and data:
            data = data[:-1] + bytes([data[-1] ^ 1])
        with open(dest, "wb") as handle:
            handle.write(data)
        return {"status": "OK"}

    def create_degraded_ok(self, src, name):
        if name in self.store:
            raise AegisError("ALREADY_EXISTS", "")
        with open(src, "rb") as handle:
            self.store[name] = handle.read()
        self.created.append(name)
        return {"status": self.create_status, "committed": "3"}

    def health(self):
        return True

    def inventory(self):
        class Entry:
            pass
        out = {}
        for name in self.store:
            e = Entry()
            e.name = name
            out[name] = e
        return out, []


def make_client(fake, **overrides):
    opts = dict(namenode_url=fake.nn_url, user="tester", allow_insecure_http=True,
                allowed_redirect_hosts=[], timeout=5, retries=2, backoff=0.01,
                chunk_size=64 * 1024)
    opts.update(overrides)
    return WebHdfsClient(HdfsSettings(**opts))


def sha(data):
    return hashlib.sha256(data).hexdigest()
