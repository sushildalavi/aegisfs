"""Thin wrapper over the `aegisfs` command-line client.

Both integrations reach the cluster only through this module, which runs the same `aegisfs`
binary a person would. That keeps TLS, the cluster token, retries and striping exactly as the
cluster defines them: a striped file is read back whole by `get`, and a large `create` is striped
by the client's own `--stripe-mib` setting. Nothing here talks to the storage format directly.

Everything returned by `inventory()` and `placement()` is what the cluster reported at that moment.
It is an observation, not a guarantee.
"""
import hashlib
import os
import subprocess
from dataclasses import dataclass, field

STRIPE_MARKER = ".stripe."  # chunk files are named <name>.stripe.<client>.<time>.<n>


class AegisError(Exception):
    """An AegisFS operation did not succeed. `status` is the cluster's status name."""

    def __init__(self, status, message=""):
        super().__init__("%s%s" % (status, (": " + message) if message else ""))
        self.status = status
        self.message = message


class AegisUnavailable(AegisError):
    """No AegisFS node could be reached at all."""


@dataclass
class FileEntry:
    name: str
    file_id: str
    size: int
    version: int
    segments: int
    seen_on: list = field(default_factory=list)  # node addresses that reported this file


@dataclass
class Replica:
    node: str
    address: str
    present: bool


@dataclass
class Member:
    node: str
    address: str
    state: str
    incarnation: int


def parse_fields(text):
    out = {}
    for line in text.splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            out[key] = value
    return out


def is_stripe_chunk(name):
    return STRIPE_MARKER in name


class AegisClient:
    def __init__(self, binary="aegisfs", entry="127.0.0.1:9100", stripe_mib=None, insecure=False,
                 timeout=300, environ=None):
        self.binary = binary
        self.entry = entry
        self.stripe_mib = stripe_mib
        self.insecure = insecure
        self.timeout = timeout
        self.environ = dict(os.environ if environ is None else environ)

    @classmethod
    def from_config(cls, cfg):
        stripe = cfg.get("aegisfs", "stripe_mib")
        return cls(
            binary=cfg.get("aegisfs", "bin", "aegisfs"),
            entry=cfg.get("aegisfs", "entry", "127.0.0.1:9100"),
            stripe_mib=int(stripe) if stripe not in (None, "") else None,
            insecure=cfg.get_bool("aegisfs", "insecure", False),
            timeout=cfg.get_int("aegisfs", "timeout_seconds", 300),
        )

    def _run(self, *args, entry=None):
        cmd = [self.binary]
        if self.insecure:
            cmd.append("--insecure")
        if self.stripe_mib is not None:
            cmd += ["--stripe-mib", str(self.stripe_mib)]
        cmd += ["--entry", entry or self.entry, *args]
        try:
            proc = subprocess.run(cmd, text=True, capture_output=True, timeout=self.timeout,
                                  env=self.environ)
        except FileNotFoundError:
            raise AegisUnavailable("NOT_INSTALLED", "aegisfs binary not found: %s" % self.binary)
        except subprocess.TimeoutExpired:
            raise AegisUnavailable("TIMEOUT", "aegisfs %s timed out" % (args[0] if args else ""))
        fields = parse_fields(proc.stdout)
        status = fields.get("status")
        if status is None:
            raise AegisUnavailable("NETWORK_ERROR", (proc.stderr or proc.stdout).strip())
        return proc.returncode, fields, proc.stdout, proc.stderr.strip()

    def _checked(self, *args, entry=None, ok=("OK",)):
        _, fields, _, err = self._run(*args, entry=entry)
        status = fields.get("status")
        if status not in ok:
            if status == "NETWORK_ERROR":
                raise AegisUnavailable(status, err)
            raise AegisError(status, err or fields.get("note", ""))
        return fields

    # ---- cluster state ----
    def health(self):
        try:
            return self._run("health")[1].get("status") == "OK"
        except AegisError:
            return False

    def members(self, entry=None):
        fields = self._checked("members", entry=entry)
        out = []
        for i in range(int(fields.get("count", "0"))):
            out.append(Member(fields["m%d_node" % i], fields["m%d_addr" % i],
                              fields["m%d_state" % i], int(fields.get("m%d_inc" % i, "0"))))
        return out

    def list_store(self, entry):
        """Files held by one specific node."""
        fields = self._checked("listStore", entry=entry)
        out = []
        for i in range(int(fields.get("count", "0"))):
            p = "f%d_" % i
            out.append(FileEntry(fields[p + "name"], fields[p + "id"], int(fields[p + "size"]),
                                 int(fields[p + "version"]), int(fields[p + "segs"]), [entry]))
        return out

    def inventory(self):
        """Every file any alive node reports. Returns (entries_by_name, unreachable_addresses).

        A node only lists what it stores, so a file is found as long as one alive replica holds it.
        If nodes disagree about a file the highest version wins, which is what a read would use.
        """
        found = {}
        unreachable = []
        for member in self.members():
            if member.state != "ALIVE":
                continue
            try:
                entries = self.list_store(member.address)
            except AegisError:
                unreachable.append(member.address)
                continue
            for entry in entries:
                current = found.get(entry.name)
                if current is None:
                    found[entry.name] = entry
                else:
                    current.seen_on.append(member.address)
                    if (entry.version, entry.size) > (current.version, current.size):
                        entry.seen_on = current.seen_on
                        found[entry.name] = entry
        return found, unreachable

    def placement(self, name):
        fields = self._checked("ls", name)
        out = []
        for i in range(int(fields.get("replicas", "0"))):
            p = "r%d_" % i
            out.append(Replica(fields.get(p + "node", ""), fields.get(p + "addr", ""),
                               fields.get(p + "present") == "yes"))
        return out

    # ---- data ----
    def get(self, name, dest_path, quorum=False):
        """Read the whole logical file (striped files are reassembled) into dest_path."""
        fields = self._checked("getQuorum" if quorum else "get", name, dest_path,
                               ok=("OK",))
        return fields

    def create(self, src_path, name):
        return self._checked("create", src_path, name, ok=("OK",))

    def create_degraded_ok(self, src_path, name):
        """Like create, but reports DEGRADED (fewer than rf replicas) instead of raising."""
        return self._checked("create", src_path, name, ok=("OK", "DEGRADED"))

    def append(self, src_path, name):
        return self._checked("append", src_path, name, ok=("OK",))

    def exists(self, name):
        try:
            self.placement(name)
        except AegisError:
            return False
        # ls answers for any name, so check presence on at least one designated replica.
        return any(r.present for r in self.placement(name))


def sha256_file(path, chunk=1024 * 1024):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        while True:
            block = handle.read(chunk)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()
