"""Errors raised by the Cassandra integration's library code."""


class DriverMissing(Exception):
    """The cassandra-driver Python package is not installed."""


class StoreUnavailable(Exception):
    """Cassandra could not be reached or did not answer in time. Safe to retry later."""


class SchemaError(Exception):
    """The schema is missing, out of date, or its recorded migrations do not match the files."""


class SecurityError(Exception):
    """The server refused the login, or its TLS certificate could not be verified.

    This is not retried and nothing is queued for it: the setting is wrong, and waiting will not
    fix it. The message never contains the password.
    """
