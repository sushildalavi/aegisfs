"""Name and path checks done before anything is sent anywhere.

HDFS paths are checked here rather than left to the NameNode, so a bad value is refused with a
clear message and never reaches a request. AegisFS names follow the rules the storage nodes apply
(no leading slash, no control characters, no "." or ".." components, at most 1024 bytes), plus two
of our own: no empty components, and no ".stripe." text, which the striping code reserves for chunk
files.
"""
import posixpath

MAX_HDFS_PATH = 4000
MAX_AEGIS_NAME = 1024
STRIPE_MARKER = ".stripe."


class InvalidName(ValueError):
    """A name or path was refused before use."""


def _check_text(value, what):
    if not isinstance(value, str) or not value:
        raise InvalidName("%s must not be empty" % what)
    for ch in value:
        if ord(ch) < 0x20 or ord(ch) == 0x7f:
            raise InvalidName("%s contains a control character" % what)
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        raise InvalidName("%s is not valid text" % what)


def check_aegis_name(name):
    _check_text(name, "the AegisFS name")
    if len(name.encode("utf-8")) > MAX_AEGIS_NAME:
        raise InvalidName("the AegisFS name is longer than %d bytes" % MAX_AEGIS_NAME)
    if name.startswith("/"):
        raise InvalidName("the AegisFS name must not start with '/'")
    if name.endswith("/"):
        raise InvalidName("the AegisFS name must not end with '/'")
    for part in name.split("/"):
        if part in ("", ".", ".."):
            raise InvalidName("the AegisFS name has an empty, '.' or '..' component")
    if STRIPE_MARKER in name:
        raise InvalidName("the AegisFS name must not contain %r (reserved for striped chunks)"
                          % STRIPE_MARKER)
    return name


def _check_components(path, what):
    if path == "/":
        return
    for part in path.split("/")[1:]:
        if part in (".", ".."):
            raise InvalidName("%s must not contain '.' or '..' components" % what)
        if part == "":
            raise InvalidName("%s must not contain empty components ('//' or a trailing '/')"
                              % what)
        if ":" in part or "\\" in part:
            raise InvalidName("%s must not contain ':' or backslash" % what)


def resolve_hdfs_path(path, root=None):
    """Return a clean absolute HDFS path.

    With `root` set, a relative path is taken to be below it and an absolute path must already be
    inside it. Without `root` the path must be absolute. Either way the result has no "." or ".."
    parts, so nothing can step outside `root`.
    """
    _check_text(path, "the HDFS path")
    if root is not None:
        root = _clean_root(root)
    if not path.startswith("/"):
        if root is None:
            raise InvalidName("the HDFS path must be absolute (start with '/'), or set hdfs.root "
                              "to use relative paths")
        path = root.rstrip("/") + "/" + path if root != "/" else "/" + path
    if len(path.encode("utf-8")) > MAX_HDFS_PATH:
        raise InvalidName("the HDFS path is too long")
    _check_components(path, "the HDFS path")
    if path != "/" and path.endswith("/"):
        raise InvalidName("the HDFS path must not end with '/'")
    if root is not None and root != "/":
        if path != root and not path.startswith(root + "/"):
            raise InvalidName("the HDFS path %s is outside the configured root %s" % (path, root))
    return path


def _clean_root(root):
    _check_text(root, "hdfs.root")
    if not root.startswith("/"):
        raise InvalidName("hdfs.root must be an absolute path")
    root = root.rstrip("/") or "/"
    if root != "/":
        _check_components(root, "hdfs.root")
    return root


def parent_and_base(path):
    parent, base = posixpath.split(path)
    return parent or "/", base
