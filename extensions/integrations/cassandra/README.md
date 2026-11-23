# Cassandra schema migrations

`migrations/NNN_name.cql` are applied in order by `aegisfs-integrations cassandra migrate`.
Never edit a migration that has been applied; add the next number. Every statement has to be
safe to run twice. See `docs/cassandraIntegration.md` for what the tables are for.
