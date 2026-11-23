"""Cassandra settings, read through the shared Config (flag > env > file > default).

Environment names are AEGISFS_INT_CASSANDRA_<KEY>, for example AEGISFS_INT_CASSANDRA_KEYSPACE.
The password is only ever read from the file named by `password_file`.
"""
import re
from dataclasses import dataclass, field
from typing import List, Optional

from ..config import ConfigError

AREA = "cassandra"
_IDENT = re.compile(r"^[a-z][a-z0-9_]{0,47}$")
CONSISTENCY_LEVELS = ("ANY", "ONE", "TWO", "THREE", "QUORUM", "ALL", "LOCAL_QUORUM",
                      "EACH_QUORUM", "LOCAL_ONE")
SERIAL_LEVELS = ("SERIAL", "LOCAL_SERIAL")
STRATEGIES = ("SimpleStrategy", "NetworkTopologyStrategy")


@dataclass
class Settings:
    contact_points: List[str] = field(default_factory=lambda: ["127.0.0.1"])
    port: int = 9042
    local_dc: Optional[str] = None
    keyspace: str = "aegisfs_index"
    username: Optional[str] = None
    password: Optional[str] = field(default=None, repr=False)
    tls: bool = True
    tls_ca_file: Optional[str] = None
    tls_verify: bool = True
    tls_server_name: Optional[str] = None
    allow_insecure: bool = False
    read_consistency: str = "LOCAL_QUORUM"
    write_consistency: str = "LOCAL_QUORUM"
    serial_consistency: str = "LOCAL_SERIAL"
    create_keyspace: bool = True
    replication_strategy: str = "SimpleStrategy"
    replication_factor: int = 1
    replication_dc_factors: dict = field(default_factory=dict)
    connect_timeout: float = 5.0
    request_timeout: float = 10.0
    bucket_count: int = 16
    expected_replicas: int = 3
    spool_dir: str = ".aegisfs-index-spool"
    workers: int = 8
    migrations_dir: Optional[str] = None

    @classmethod
    def from_config(cls, cfg, overrides=None):
        o = overrides or {}
        get = lambda key, default=None: cfg.get(AREA, key, default, o.get(key))  # noqa: E731
        s = cls()
        s.contact_points = cfg.get_list(AREA, "contact_points", ["127.0.0.1"], o.get("contact_points"))
        s.port = cfg.get_int(AREA, "port", 9042, o.get("port"))
        s.local_dc = get("local_dc") or None
        s.keyspace = str(get("keyspace", "aegisfs_index"))
        s.username = get("username") or None
        s.password = cfg.read_secret(AREA, "password")
        s.tls = cfg.get_bool(AREA, "tls", True, o.get("tls"))
        s.tls_ca_file = get("tls_ca_file") or None
        s.tls_verify = cfg.get_bool(AREA, "tls_verify", True, o.get("tls_verify"))
        s.tls_server_name = get("tls_server_name") or None
        s.allow_insecure = cfg.get_bool(AREA, "allow_insecure", False, o.get("allow_insecure"))
        s.read_consistency = str(get("read_consistency", "LOCAL_QUORUM")).upper()
        s.write_consistency = str(get("write_consistency", "LOCAL_QUORUM")).upper()
        s.serial_consistency = str(get("serial_consistency", "LOCAL_SERIAL")).upper()
        s.create_keyspace = cfg.get_bool(AREA, "create_keyspace", True, o.get("create_keyspace"))
        s.replication_strategy = str(get("replication_strategy", "SimpleStrategy"))
        s.replication_factor = cfg.get_int(AREA, "replication_factor", 1, o.get("replication_factor"))
        s.replication_dc_factors = _parse_dc_factors(get("replication_dc_factors"))
        s.connect_timeout = float(get("connect_timeout", 5.0))
        s.request_timeout = float(get("request_timeout", 10.0))
        s.bucket_count = cfg.get_int(AREA, "bucket_count", 16, o.get("bucket_count"))
        s.expected_replicas = cfg.get_int(AREA, "expected_replicas", 3, o.get("expected_replicas"))
        s.spool_dir = str(get("spool_dir", ".aegisfs-index-spool"))
        s.workers = cfg.get_int(AREA, "workers", 8, o.get("workers"))
        s.migrations_dir = get("migrations_dir") or None
        s.validate()
        return s

    def validate(self):
        if not _IDENT.match(self.keyspace):
            raise ConfigError("cassandra.keyspace must be lowercase letters, digits and underscores")
        if not self.contact_points:
            raise ConfigError("cassandra.contact_points is empty")
        for level, label in ((self.read_consistency, "read_consistency"),
                             (self.write_consistency, "write_consistency")):
            if level not in CONSISTENCY_LEVELS:
                raise ConfigError("cassandra.%s must be one of %s" % (label, ", ".join(CONSISTENCY_LEVELS)))
        if self.serial_consistency not in SERIAL_LEVELS:
            raise ConfigError("cassandra.serial_consistency must be SERIAL or LOCAL_SERIAL")
        if self.replication_strategy not in STRATEGIES:
            raise ConfigError("cassandra.replication_strategy must be one of %s" % ", ".join(STRATEGIES))
        if self.replication_factor < 1:
            raise ConfigError("cassandra.replication_factor must be at least 1")
        if self.replication_strategy == "NetworkTopologyStrategy" and not self.replication_dc_factors:
            raise ConfigError("NetworkTopologyStrategy needs cassandra.replication_dc_factors, "
                              "for example dc1:3,dc2:3")
        if not 1 <= self.bucket_count <= 256:
            raise ConfigError("cassandra.bucket_count must be between 1 and 256")
        if self.expected_replicas < 1:
            raise ConfigError("cassandra.expected_replicas must be at least 1")
        if self.workers < 1 or self.workers > 64:
            raise ConfigError("cassandra.workers must be between 1 and 64")
        if (self.username is None) != (self.password is None):
            raise ConfigError("cassandra.username and cassandra.password_file go together")
        if not self.tls and not self.allow_insecure:
            raise ConfigError("cassandra.tls is off. Set cassandra.allow_insecure=true to confirm "
                              "this is a development setup with no TLS")
        if self.tls and not self.tls_verify and not self.allow_insecure:
            raise ConfigError("cassandra.tls_verify is off. Set cassandra.allow_insecure=true to "
                              "confirm this is a development setup")

    def replication_cql(self):
        if self.replication_strategy == "SimpleStrategy":
            return "{'class': 'SimpleStrategy', 'replication_factor': %d}" % self.replication_factor
        parts = ["'class': 'NetworkTopologyStrategy'"]
        for dc, factor in sorted(self.replication_dc_factors.items()):
            parts.append("'%s': %d" % (dc.replace("'", ""), factor))
        return "{" + ", ".join(parts) + "}"

    def public_summary(self):
        """Settings safe to print: no password."""
        return {
            "contact_points": self.contact_points, "port": self.port, "local_dc": self.local_dc,
            "keyspace": self.keyspace, "username": self.username, "tls": self.tls,
            "tls_verify": self.tls_verify, "tls_server_name": self.tls_server_name,
            "allow_insecure": self.allow_insecure,
            "read_consistency": self.read_consistency, "write_consistency": self.write_consistency,
            "replication": self.replication_cql(), "bucket_count": self.bucket_count,
            "spool_dir": self.spool_dir,
        }


def _parse_dc_factors(value):
    if not value:
        return {}
    if isinstance(value, dict):
        items = list(value.items())
    else:
        items = []
        for part in str(value).split(","):
            if not part.strip():
                continue
            if ":" not in part:
                raise ConfigError("replication_dc_factors entries look like dc1:3")
            items.append(tuple(x.strip() for x in part.split(":", 1)))
    out = {}
    for dc, factor in items:
        try:
            out[str(dc)] = int(factor)
        except (TypeError, ValueError):
            raise ConfigError("replication factor for %s must be a whole number" % dc)
        if out[str(dc)] < 1:
            raise ConfigError("replication factor for %s must be at least 1" % dc)
    return out
