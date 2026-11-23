"""Helpers shared by the Cassandra tests: import path, a fake AegisFS, observation builders."""
import os
import sys
from datetime import datetime, timedelta

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
INTEGRATIONS = os.path.join(ROOT, "extensions", "integrations")
if INTEGRATIONS not in sys.path:
    sys.path.insert(0, INTEGRATIONS)

from aegisfsIntegrations.aegisfsClient import AegisError, FileEntry, Replica  # noqa: E402
from aegisfsIntegrations.cassandra.backend import MemoryBackend  # noqa: E402
from aegisfsIntegrations.cassandra.model import Observation, utcnow_ms  # noqa: E402
from aegisfsIntegrations.cassandra.settings import Settings  # noqa: E402
from aegisfsIntegrations.cassandra.store import IndexStore  # noqa: E402

T0 = datetime(2026, 1, 1, 12, 0, 0)


def at(seconds):
    return T0 + timedelta(seconds=seconds)


def obs(name="a/b.txt", version=1, seconds=0, **kw):
    kw.setdefault("source", "test")
    kw.setdefault("file_id", "f%s" % abs(hash(name) % 10**8))
    kw.setdefault("stored_size", 10)
    return Observation(name=name, version=version, observed_at=at(seconds), **kw)


def memory_store():
    return IndexStore(MemoryBackend())


def settings(**kw):
    s = Settings(**kw)
    s.validate()
    return s


class FakeAegis:
    """Enough of AegisClient for the indexer: an inventory, placements and file contents."""

    def __init__(self, entries=None, unreachable=None, nodes=("n1:1", "n2:1", "n3:1")):
        self.entries = dict(entries or {})
        self.unreachable = list(unreachable or [])
        self.nodes = list(nodes)
        self.contents = {}
        self.read_fails = set()
        self.down = False
        self.gets = []

    def add(self, name, size=10, version=1, seen_on=None, file_id=None, data=None):
        seen_on = list(self.nodes if seen_on is None else seen_on)
        self.entries[name] = FileEntry(name, file_id or ("id" + str(abs(hash(name)) % 10**6)), size,
                                       version, 1, seen_on)
        if data is not None:
            self.contents[name] = data

    def inventory(self):
        if self.down:
            raise AegisError("NETWORK_ERROR", "down")
        copies = {n: FileEntry(e.name, e.file_id, e.size, e.version, e.segments, list(e.seen_on))
                  for n, e in self.entries.items()}
        return copies, list(self.unreachable)

    def placement(self, name):
        entry = self.entries.get(name)
        return [Replica("node%d" % i, addr, entry is not None and addr in entry.seen_on)
                for i, addr in enumerate(self.nodes)]

    def list_store(self, address):
        return [FileEntry(e.name, e.file_id, e.size, e.version, e.segments, [address])
                for e in self.entries.values() if address in e.seen_on]

    def get(self, name, dest, quorum=False):
        self.gets.append(name)
        if name in self.read_fails:
            raise AegisError("CHECKSUM_MISMATCH", "bad")
        data = self.contents.get(name, b"x" * self.entries[name].size)
        with open(dest, "wb") as handle:
            handle.write(data)
        return {"status": "OK"}
