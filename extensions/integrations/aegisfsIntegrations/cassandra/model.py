"""What gets stored in the index, how an observation is checked, and how it is merged.

Everything here is plain Python with no database access, so the ordering rules can be tested
without Cassandra.

An *observation* is one statement of the form "at time T, AegisFS reported file F at version V".
The index keeps one row per file. A new observation replaces the row only if it is not older than
what the row already holds, where "older" is decided by the pair (AegisFS file version,
observed_at). The file version always wins; observed_at only breaks ties inside one version.
"""
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional

STRIPE_MARKER = ".stripe."  # chunk files are named <name>.stripe.<client>.<time>.<n>
ROOT_NAMESPACE = "/"       # Cassandra does not allow an empty partition key, so root is "/"

STATUS_INDEXED = "indexed"        # seen on at least the expected number of replicas
STATUS_INCOMPLETE = "incomplete"  # seen on fewer replicas than expected, or a scan was partial
STATUS_STALE = "stale"            # not reported by AegisFS in the most recent scan
STATUSES = (STATUS_INDEXED, STATUS_INCOMPLETE, STATUS_STALE)

VERIFY_VERIFIED = "verified"
VERIFY_UNVERIFIED = "unverified"
VERIFY_SKIPPED = "skipped_too_large"
VERIFY_FAILED = "failed"
VERIFY_STATES = (VERIFY_VERIFIED, VERIFY_UNVERIFIED, VERIFY_SKIPPED, VERIFY_FAILED)

SIZE_READ = "read"                     # measured by reading the whole file
SIZE_LISTSTORE = "listStore_no_chunks"  # listStore size, and no stripe chunks were seen for it
SIZE_BASES = (SIZE_READ, SIZE_LISTSTORE)

STRIPED_VERIFIED = "verified_get"       # a read of the file reported a striped reassembly
STRIPED_CHUNKS = "inferred_from_chunks"  # chunk files named after this file exist
STRIPED_BASES = (STRIPED_VERIFIED, STRIPED_CHUNKS)

MAX_NAME_CHARS = 1024
MAX_REPLICAS = 64
MAX_FUTURE_SKEW = timedelta(minutes=5)

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_FILE_ID = re.compile(r"^[0-9A-Za-z_.:-]{1,128}$")
_CONTROL = re.compile(r"[\x00-\x1f\x7f]")


class ObservationError(ValueError):
    """The observation is not acceptable and will never be acceptable (do not retry it)."""


def utcnow_ms():
    """Current UTC time, naive, truncated to milliseconds (the precision Cassandra keeps)."""
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    return now.replace(microsecond=(now.microsecond // 1000) * 1000)


def to_ms(value):
    return value.replace(microsecond=(value.microsecond // 1000) * 1000)


def iso(value):
    return None if value is None else value.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def parse_iso(text):
    if text is None:
        return None
    if isinstance(text, datetime):
        return to_ms(text.replace(tzinfo=None) if text.tzinfo is None else
                     text.astimezone(timezone.utc).replace(tzinfo=None))
    try:
        parsed = datetime.fromisoformat(str(text).replace("Z", "+00:00"))
    except ValueError:
        raise ObservationError("not a timestamp: %r" % (text,))
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return to_ms(parsed)


def is_chunk_name(name):
    return STRIPE_MARKER in name


def parent_of_chunk(name):
    return name.split(STRIPE_MARKER, 1)[0]


def namespace_of(name):
    ns = name.rpartition("/")[0]
    return ns if ns else ROOT_NAMESPACE


def parent_namespace(ns):
    if ns == ROOT_NAMESPACE:
        return None
    return ns.rpartition("/")[0] or ROOT_NAMESPACE


def ancestors_of(ns):
    """[(parent, child), ...] edges from the root down to `ns`."""
    edges = []
    while ns != ROOT_NAMESPACE:
        parent = parent_namespace(ns)
        edges.append((parent, ns))
        ns = parent
    edges.reverse()
    return edges


def bucket_of(name, bucket_count):
    """Stable bucket for a name. Not Python's hash(), which changes between runs."""
    import zlib
    return zlib.crc32(name.encode("utf-8")) % bucket_count


@dataclass
class Observation:
    name: str
    observed_at: datetime
    source: str
    kind: str = "present"            # "present" or "absent"
    version: Optional[int] = None    # required for "present"
    file_id: Optional[str] = None
    stored_size: Optional[int] = None  # what listStore reports (the index size for a striped file)
    size: Optional[int] = None         # logical size, only when known
    size_basis: Optional[str] = None
    striped: Optional[bool] = None
    striped_basis: Optional[str] = None
    chunk_files: Optional[int] = None
    replicas: Optional[Dict[str, bool]] = None
    replicas_source: Optional[str] = None
    status: str = STATUS_INDEXED
    status_reason: Optional[str] = None
    sha256: Optional[str] = None
    verified_at: Optional[datetime] = None
    verified_size: Optional[int] = None
    verify_state: Optional[str] = None

    def validate(self, now=None):
        """Raise ObservationError if this must not be stored. Returns self."""
        now = now or utcnow_ms()
        if not isinstance(self.name, str) or not self.name:
            raise ObservationError("name must be a non-empty string")
        if len(self.name) > MAX_NAME_CHARS:
            raise ObservationError("name is longer than %d characters" % MAX_NAME_CHARS)
        if _CONTROL.search(self.name):
            raise ObservationError("name contains a control character")
        if is_chunk_name(self.name):
            raise ObservationError("stripe chunk files are not indexed as files")
        if self.kind not in ("present", "absent"):
            raise ObservationError("kind must be present or absent")
        if not isinstance(self.observed_at, datetime):
            raise ObservationError("observed_at must be a timestamp")
        if self.observed_at > now + MAX_FUTURE_SKEW:
            raise ObservationError("observed_at is in the future")
        if not self.source or not isinstance(self.source, str) or len(self.source) > 128:
            raise ObservationError("source must be a short non-empty string")
        if self.status not in STATUSES:
            raise ObservationError("status must be one of %s" % ", ".join(STATUSES))
        if self.kind == "absent":
            if self.status == STATUS_INDEXED:
                raise ObservationError("an absent observation cannot be marked indexed")
            return self
        _nonneg_int(self.version, "version", required=True)
        _nonneg_int(self.stored_size, "stored_size")
        _nonneg_int(self.size, "size")
        _nonneg_int(self.chunk_files, "chunk_files")
        _nonneg_int(self.verified_size, "verified_size")
        if self.file_id is not None and not (isinstance(self.file_id, str) and _FILE_ID.match(self.file_id)):
            raise ObservationError("file_id has an unexpected format")
        if self.striped is not None and not isinstance(self.striped, bool):
            raise ObservationError("striped must be true or false")
        if self.striped_basis is not None and self.striped_basis not in STRIPED_BASES:
            raise ObservationError("unknown striped_basis")
        if self.striped_basis is not None and self.striped is None:
            raise ObservationError("striped_basis given without striped")
        if self.size_basis is not None and self.size_basis not in SIZE_BASES:
            raise ObservationError("unknown size_basis")
        if (self.size is None) != (self.size_basis is None):
            raise ObservationError("size and size_basis must be given together")
        if self.size is not None and self.striped is True and self.size_basis != SIZE_READ:
            raise ObservationError("the logical size of a striped file is only known by reading it")
        if self.verify_state is not None and self.verify_state not in VERIFY_STATES:
            raise ObservationError("unknown verify_state")
        if self.replicas is not None:
            if not isinstance(self.replicas, dict) or len(self.replicas) > MAX_REPLICAS:
                raise ObservationError("replicas must be a small map of address to true/false")
            for key, value in self.replicas.items():
                if not isinstance(key, str) or not key or not isinstance(value, bool):
                    raise ObservationError("replicas must map address strings to booleans")
        if self.sha256 is not None:
            if not (isinstance(self.sha256, str) and _SHA256.match(self.sha256)):
                raise ObservationError("sha256 must be 64 lowercase hex characters")
            if self.verified_at is None or self.verified_size is None:
                raise ObservationError("a checksum needs verified_at and verified_size")
            if self.verified_at > now + MAX_FUTURE_SKEW:
                raise ObservationError("verified_at is in the future")
            if self.size_basis != SIZE_READ or self.size != self.verified_size:
                raise ObservationError("a checksum must come with the size that was read")
            if self.verify_state not in (None, VERIFY_VERIFIED):
                raise ObservationError("a checksum implies verify_state=verified")
        elif self.verified_at is not None or self.verified_size is not None:
            raise ObservationError("verified_at/verified_size given without a checksum")
        return self

    def to_json(self):
        out = {k: getattr(self, k) for k in self.__dataclass_fields__}
        out["observed_at"] = iso(self.observed_at)
        out["verified_at"] = iso(self.verified_at)
        return out

    @classmethod
    def from_json(cls, data):
        if not isinstance(data, dict):
            raise ObservationError("an observation must be a JSON object")
        unknown = set(data) - set(cls.__dataclass_fields__)
        if unknown:
            raise ObservationError("unknown fields: %s" % ", ".join(sorted(unknown)))
        data = dict(data)
        data["observed_at"] = parse_iso(data.get("observed_at"))
        data["verified_at"] = parse_iso(data.get("verified_at"))
        if data["observed_at"] is None:
            raise ObservationError("observed_at is required")
        for key in ("name", "source"):
            if key not in data:
                raise ObservationError("%s is required" % key)
        return cls(**data)


def _nonneg_int(value, label, required=False):
    if value is None:
        if required:
            raise ObservationError("%s is required" % label)
        return
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ObservationError("%s must be a whole number >= 0" % label)


# Columns of the main table that a merge produces. `first_indexed_at` and `updated_at` are
# bookkeeping and are not part of ordering.
ROW_FIELDS = (
    "name", "namespace", "bucket", "file_id", "version", "observed_at", "stored_size", "size",
    "size_basis", "striped", "striped_basis", "chunk_files", "sha256", "verified_at",
    "verified_size", "verify_state", "replicas", "replicas_source", "replicas_observed_at",
    "status", "status_reason", "source", "first_indexed_at", "updated_at",
)

REJECT_OLDER_VERSION = "older_version"
REJECT_OLDER_OBSERVATION = "older_observation"


@dataclass
class MergeResult:
    row: Optional[dict]
    outcome: str  # created | updated | unchanged | rejected | ignored
    reason: Optional[str] = None


def merge(existing, obs, bucket_count, now=None):
    """Combine the stored row (or None) with a validated observation. Pure function."""
    now = now or utcnow_ms()
    if existing is None:
        if obs.kind == "absent":
            # A file that was never indexed is not invented just to mark it missing.
            return MergeResult(None, "ignored", "absent observation for a file that is not indexed")
        row = _row_from(obs, bucket_count)
        row["first_indexed_at"] = now
        row["updated_at"] = now
        return MergeResult(row, "created")

    old_key = (existing["version"], existing["observed_at"])
    if obs.kind == "absent":
        if (existing["version"], obs.observed_at) < old_key:
            return MergeResult(None, "rejected", REJECT_OLDER_OBSERVATION)
        row = dict(existing)
        row.update(status=obs.status, status_reason=obs.status_reason, observed_at=obs.observed_at,
                   source=obs.source)
    else:
        if obs.version < existing["version"]:
            return MergeResult(None, "rejected", REJECT_OLDER_VERSION)
        if (obs.version, obs.observed_at) < old_key:
            return MergeResult(None, "rejected", REJECT_OLDER_OBSERVATION)
        if obs.version > existing["version"]:
            row = _row_from(obs, bucket_count)
            row["first_indexed_at"] = existing.get("first_indexed_at") or now
        else:
            row = _merge_same_version(existing, obs)
    row["updated_at"] = now
    comparable = [k for k in ROW_FIELDS if k != "updated_at"]
    if all(row.get(k) == existing.get(k) for k in comparable):
        return MergeResult(existing, "unchanged")
    return MergeResult(row, "updated")


def _row_from(obs, bucket_count):
    row = {k: None for k in ROW_FIELDS}
    row.update(
        name=obs.name, namespace=namespace_of(obs.name), bucket=bucket_of(obs.name, bucket_count),
        file_id=obs.file_id, version=obs.version, observed_at=obs.observed_at,
        stored_size=obs.stored_size, size=obs.size, size_basis=obs.size_basis,
        striped=obs.striped, striped_basis=obs.striped_basis, chunk_files=obs.chunk_files,
        sha256=obs.sha256, verified_at=obs.verified_at, verified_size=obs.verified_size,
        verify_state=obs.verify_state or VERIFY_UNVERIFIED,
        replicas=dict(obs.replicas) if obs.replicas is not None else None,
        replicas_source=obs.replicas_source,
        replicas_observed_at=obs.observed_at if obs.replicas is not None else None,
        status=obs.status, status_reason=obs.status_reason, source=obs.source)
    return row


def _merge_same_version(existing, obs):
    """Same file version: fields the observation does not know keep their stored value."""
    row = dict(existing)
    row.update(observed_at=obs.observed_at, source=obs.source, status=obs.status,
               status_reason=obs.status_reason)
    for key in ("file_id", "stored_size", "chunk_files"):
        if getattr(obs, key) is not None:
            row[key] = getattr(obs, key)
    # A value learned by reading the file is not replaced by a guess made from a listing.
    keep_striped = existing["striped_basis"] == STRIPED_VERIFIED and obs.striped_basis != STRIPED_VERIFIED
    if obs.striped is not None and not keep_striped:
        row["striped"], row["striped_basis"] = obs.striped, obs.striped_basis
    keep_size = existing["size_basis"] == SIZE_READ and obs.size_basis != SIZE_READ
    if obs.size is not None and not keep_size:
        row["size"], row["size_basis"] = obs.size, obs.size_basis
    if obs.replicas is not None:
        row["replicas"] = dict(obs.replicas)
        row["replicas_source"] = obs.replicas_source
        row["replicas_observed_at"] = obs.observed_at
    if obs.sha256 is not None:
        row.update(sha256=obs.sha256, verified_at=obs.verified_at, verified_size=obs.verified_size,
                   verify_state=VERIFY_VERIFIED)
    elif obs.verify_state in (VERIFY_SKIPPED, VERIFY_FAILED) and existing["sha256"] is None:
        row["verify_state"] = obs.verify_state
    return row


def attrs_of(row):
    """Attribute keys under which a row is listed in the attribute table."""
    attrs = []
    if row.get("striped") is True:
        attrs.append("striped=true")
    if row["status"] in (STATUS_STALE, STATUS_INCOMPLETE):
        attrs.append("status=" + row["status"])
    return attrs
