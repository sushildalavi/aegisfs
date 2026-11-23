"""Errors raised by the Cassandra integration's library code."""


class DriverMissing(Exception):
    """The cassandra-driver Python package is not installed."""


class StoreUnavailable(Exception):
    """Cassandra could not be reached or did not answer in time. Safe to retry later."""


class SchemaError(Exception):
    """The schema is missing, out of date, or its recorded migrations do not match the files."""
