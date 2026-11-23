#!/usr/bin/env python3
"""Create the index login on the secure Cassandra test container and retire the default superuser.

Run by scripts/integrations/cassandraUp.sh --secure after the container is healthy. Cassandra
creates a superuser `cassandra` with a well-known default password on first start, so this changes
that password to a random one and adds a normal login `aegis_index`. Safe to run again.

    secureSetup.py <creds dir> [host] [port]
"""
import os
import ssl
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..",
                                "extensions", "integrations"))
from cassandra import AuthenticationFailed  # noqa: E402
from cassandra.auth import PlainTextAuthProvider  # noqa: E402
from cassandra.cluster import Cluster, NoHostAvailable  # noqa: E402


def read(path):
    with open(path) as handle:
        return handle.read().strip()


def connect(directory, host, port, password):
    ctx = ssl.create_default_context(cafile=os.path.join(directory, "ca.pem"))
    cluster = Cluster([host], port=port, ssl_context=ctx, connect_timeout=5,
                      auth_provider=PlainTextAuthProvider("cassandra", password))
    return cluster, cluster.connect()


def main():
    directory = sys.argv[1]
    host = sys.argv[2] if len(sys.argv) > 2 else "127.0.0.1"
    port = int(sys.argv[3]) if len(sys.argv) > 3 else 9042
    admin, index = read(os.path.join(directory, "admin.pass")), read(os.path.join(directory, "index.pass"))
    deadline = time.time() + 180
    last = None
    while time.time() < deadline:
        for password in ("cassandra", admin):  # fresh server first, then an already configured one
            try:
                cluster, session = connect(directory, host, port, password)
            except (NoHostAvailable, AuthenticationFailed) as exc:
                last = exc
                continue
            session.execute("CREATE ROLE IF NOT EXISTS aegis_index WITH PASSWORD = %s AND LOGIN = true", (index,))
            session.execute("ALTER ROLE aegis_index WITH PASSWORD = %s", (index,))
            if password != admin:
                session.execute("ALTER ROLE cassandra WITH PASSWORD = %s", (admin,))
            cluster.shutdown()
            print("secure Cassandra ready: login aegis_index, TLS on, default superuser password replaced")
            return 0
        time.sleep(4)
    print("secure Cassandra did not become ready: %s" % type(last).__name__, file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
