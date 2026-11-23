-- Lookups by attribute, and settings that must not change once data exists.

-- Files that have an attribute worth asking about on its own. attr is one of:
--   striped=true, status=stale, status=incomplete
-- The 'indexed' state is not listed here (use the namespace listing).
-- Rows here are kept in step with files_by_name but are written afterwards, so a reader
-- re-checks them against files_by_name (the query code does this).
CREATE TABLE IF NOT EXISTS files_by_attr (
  attr        text,
  bucket      int,
  name        text,
  file_id     text,
  version     bigint,
  status      text,
  observed_at timestamp,
  PRIMARY KEY ((attr, bucket), name)
);

-- bucket_count is fixed when the index is first used, because a name's bucket is derived from it.
CREATE TABLE IF NOT EXISTS index_meta (
  key   text PRIMARY KEY,
  value text
);
