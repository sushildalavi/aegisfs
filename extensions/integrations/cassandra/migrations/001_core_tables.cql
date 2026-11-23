-- Core tables of the AegisFS metadata index.
-- This index is an observation of what AegisFS reported. AegisFS stays the source of truth.
-- Every statement here must be safe to run twice (the runner records applied versions, but a
-- crash between the statement and the record means a statement can be seen again).

-- One row per AegisFS file. This is the only table written with lightweight transactions.
-- (version, observed_at) is the ordering guard: an older observation never replaces a newer one.
CREATE TABLE IF NOT EXISTS files_by_name (
  name                 text PRIMARY KEY,
  namespace            text,
  bucket               int,
  file_id              text,
  version              bigint,
  observed_at          timestamp,
  stored_size          bigint,   -- size listStore reports (for a striped file: the index object)
  size                 bigint,   -- logical size, null unless known
  size_basis           text,     -- how size was learned: read | listStore_no_chunks
  striped              boolean,  -- null when not known
  striped_basis        text,     -- verified_get | inferred_from_chunks
  chunk_files          int,      -- stripe chunk files seen for this name (informational)
  sha256               text,     -- null unless a full read was hashed
  verified_at          timestamp,
  verified_size        bigint,
  verify_state         text,     -- verified | unverified | skipped_too_large | failed
  replicas             frozen<map<text, boolean>>,  -- node address -> file present
  replicas_source      text,     -- listStore (only nodes that listed it) | ls (designated replicas)
  replicas_observed_at timestamp,
  status               text,     -- indexed | incomplete | stale
  status_reason        text,
  source               text,
  first_indexed_at     timestamp,
  updated_at           timestamp
);

-- Listing of one namespace (the part of the name before the last '/'; '/' is the root).
-- Split into buckets so that no partition grows without limit. Name is the clustering key, so a
-- prefix is a range read inside each bucket.
CREATE TABLE IF NOT EXISTS files_by_namespace (
  namespace      text,
  bucket         int,
  name           text,
  file_id        text,
  version        bigint,
  stored_size    bigint,
  size           bigint,
  striped        boolean,
  status         text,
  replica_count  int,
  sha256         text,
  verified_at    timestamp,
  observed_at    timestamp,
  PRIMARY KEY ((namespace, bucket), name)
);

-- Directory structure, so a prefix listing can find sub-namespaces without scanning.
CREATE TABLE IF NOT EXISTS namespace_children (
  parent text,
  child  text,
  PRIMARY KEY ((parent), child)
);
