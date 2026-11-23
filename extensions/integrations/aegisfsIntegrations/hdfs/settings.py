"""Build WebHdfsClient settings from the shared Config (flag > environment > file > default)."""
import getpass
import os

from ..config import ConfigError
from .webHdfs import HdfsSettings, parse_namenode_url, HdfsProtocolError

AREA = "hdfs"


class HdfsOptions:
    """The non-network options: where state lives, the root fence, temp dir."""

    def __init__(self, root, state_dir, tmp_dir):
        self.root = root
        self.state_dir = state_dir
        self.tmp_dir = tmp_dir


def load(cfg, namenode_url=None, user=None):
    """Return (HdfsSettings, HdfsOptions). Raises ConfigError for anything unusable."""
    url = cfg.get(AREA, "namenode_url", None, namenode_url)
    if not url:
        raise ConfigError("hdfs.namenode_url is not set (use --namenode-url, "
                          "AEGISFS_INT_HDFS_NAMENODE_URL or the configuration file)")
    allow_http = cfg.get_bool(AREA, "allow_insecure_http", False)
    try:
        parse_namenode_url(url, allow_http)
    except HdfsProtocolError as exc:
        raise ConfigError(str(exc))
    auth = str(cfg.get(AREA, "auth", "simple")).lower()
    if auth not in ("simple", "token"):
        raise ConfigError("hdfs.auth must be 'simple' or 'token' (Kerberos is not supported)")
    token = None
    name = None
    if auth == "token":
        token = cfg.read_secret(AREA, "token")
        if not token:
            raise ConfigError("hdfs.auth=token needs hdfs.token_file (a file holding the "
                              "delegation token)")
    else:
        name = cfg.get(AREA, "user", None, user) or _login()
    ca_file = cfg.get(AREA, "ca_file")
    if ca_file and not os.path.isfile(ca_file):
        raise ConfigError("hdfs.ca_file %s does not exist" % ca_file)
    if url.startswith("http://") and ca_file:
        raise ConfigError("hdfs.ca_file is set but the NameNode URL is http://")
    allowed = cfg.get_list(AREA, "allowed_redirect_hosts", [])
    settings = HdfsSettings(
        namenode_url=url, user=name, token=token, allow_insecure_http=allow_http,
        ca_file=ca_file, allowed_redirect_hosts=allowed,
        timeout=float(cfg.get_int(AREA, "timeout_seconds", 60)),
        retries=cfg.get_int(AREA, "retries", 3),
        backoff=float(cfg.get(AREA, "retry_backoff_seconds", 0.5)),
        max_redirects=cfg.get_int(AREA, "max_redirects", 3),
        chunk_size=cfg.get_int(AREA, "chunk_bytes", 1024 * 1024))
    if settings.chunk_size < 4096 or settings.timeout <= 0 or settings.retries < 0:
        raise ConfigError("hdfs.chunk_bytes, timeout_seconds or retries is out of range")
    state_dir = cfg.get(AREA, "state_dir") or os.path.join(
        os.path.expanduser("~"), ".aegisfs-integrations", "hdfs")
    options = HdfsOptions(root=cfg.get(AREA, "root"), state_dir=state_dir,
                          tmp_dir=cfg.get(AREA, "tmp_dir"))
    return settings, options


def _login():
    try:
        return getpass.getuser()
    except Exception:
        raise ConfigError("hdfs.user is not set and the login name cannot be determined")
