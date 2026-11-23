"""Opening a Cassandra session from Settings."""
import ssl

from .errors import DriverMissing, StoreUnavailable


def import_driver():
    try:
        import cassandra  # noqa: F401
        import cassandra.cluster  # noqa: F401
    except ImportError as exc:
        raise DriverMissing("the cassandra-driver package is not installed (%s). Install it with "
                            "'pip install -r extensions/integrations/requirements.txt'" % exc)


def build_ssl_context(settings):
    if not settings.tls:
        return None
    ctx = ssl.create_default_context(cafile=settings.tls_ca_file)
    if not settings.tls_verify:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    return ctx


def consistency(name):
    from cassandra import ConsistencyLevel
    return getattr(ConsistencyLevel, name)


def connect(settings, use_keyspace=True):
    """Return a connected (cluster, session). Raises StoreUnavailable if no node answers."""
    import_driver()
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT, NoHostAvailable
    from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy

    policy = DCAwareRoundRobinPolicy(local_dc=settings.local_dc) if settings.local_dc \
        else DCAwareRoundRobinPolicy()
    policy = TokenAwarePolicy(policy)

    def profile(level):
        return ExecutionProfile(load_balancing_policy=policy, consistency_level=consistency(level),
                                serial_consistency_level=consistency(settings.serial_consistency),
                                request_timeout=settings.request_timeout)

    profiles = {EXEC_PROFILE_DEFAULT: profile(settings.write_consistency),
                "read": profile(settings.read_consistency)}
    auth = None
    if settings.username:
        auth = PlainTextAuthProvider(settings.username, settings.password)
    cluster = Cluster(settings.contact_points, port=settings.port, auth_provider=auth,
                      ssl_context=build_ssl_context(settings), execution_profiles=profiles,
                      connect_timeout=settings.connect_timeout)
    try:
        session = cluster.connect(settings.keyspace) if use_keyspace else cluster.connect()
    except NoHostAvailable as exc:
        cluster.shutdown()
        if use_keyspace and "using keyspace" in str(exc):
            # The nodes answered, but the keyspace is missing: that needs `cassandra migrate`.
            from .errors import SchemaError
            raise SchemaError("keyspace %s does not exist; run 'cassandra migrate' first" % settings.keyspace)
        raise StoreUnavailable("no Cassandra node answered at %s:%s (%s)" % (
            ",".join(settings.contact_points), settings.port, _scrub(exc, settings)))
    except Exception as exc:
        cluster.shutdown()
        name = type(exc).__name__
        if name == "InvalidRequest" and "keyspace" in str(exc).lower():
            from .errors import SchemaError
            raise SchemaError("keyspace %s does not exist; run 'cassandra migrate' first" % settings.keyspace)
        raise StoreUnavailable("cannot open a Cassandra session: %s: %s" % (name, _scrub(exc, settings)))
    return cluster, session


def _scrub(exc, settings):
    from ..config import redact
    return redact(str(exc), [settings.password])
