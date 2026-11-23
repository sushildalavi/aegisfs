"""A small WebHDFS client built on the standard library.

WebHDFS reads and writes in two steps: the request goes to the NameNode, which answers with a
307 redirect naming a DataNode, and the data moves to or from that DataNode. This client never
lets http.client or urllib follow a redirect on its own. Each hop is checked here first:

  * the scheme must match the scheme of the configured NameNode URL (no https -> http downgrade),
  * the URL must not carry user information,
  * the host must be the NameNode host or appear in `allowed_redirect_hosts`,
  * the number of hops is bounded and a URL that repeats is treated as a loop.

Credentials (the simple user name, or a delegation token) are added only to the request sent to
the NameNode. A redirect target is requested exactly as the NameNode wrote it; nothing is added.
Error messages show a URL without its query string, so a token cannot end up in a message.

Request and response bodies move in fixed-size chunks and are never held whole in memory.
Kerberos (SPNEGO) is not implemented.
"""
import hashlib
import http.client
import json
import re
import ssl
import time
import urllib.parse

DEFAULT_CHUNK = 1024 * 1024
RETRY_STATUS = {502, 503, 504}
CONTROL = re.compile(r"[\x00-\x20\x7f]")
TEMP_PATTERN = re.compile(r"^\..+\.aegis-tmp-[0-9a-f]{16}$")


class HdfsError(Exception):
    """Base class. `kind` is a short stable word callers can test."""
    kind = "error"

    def __init__(self, message, http_status=None, remote=None):
        super().__init__(message)
        self.http_status = http_status
        self.remote = remote


class HdfsNotFound(HdfsError):
    kind = "not_found"


class HdfsExists(HdfsError):
    kind = "exists"


class HdfsAccessDenied(HdfsError):
    kind = "access_denied"


class HdfsUnavailable(HdfsError):
    """The NameNode or a DataNode could not be reached, timed out, or reported a server error."""
    kind = "unavailable"


class HdfsProtocolError(HdfsError):
    """A response or redirect was not acceptable (including every redirect-validation refusal)."""
    kind = "protocol"


class HdfsBadRequest(HdfsError):
    kind = "bad_request"


class _SinkError(Exception):
    """The caller's write callback failed; this is not a network problem and is not retried."""

    def __init__(self, original):
        super().__init__(str(original))
        self.original = original


class HdfsSettings:
    """Everything the client needs. Built from Config by `fromConfig` or directly in tests."""

    def __init__(self, namenode_url, user=None, token=None, allow_insecure_http=False,
                 ca_file=None, allowed_redirect_hosts=None, timeout=60.0, retries=3,
                 backoff=0.5, max_redirects=3, chunk_size=DEFAULT_CHUNK):
        self.namenode_url = namenode_url
        self.user = user
        self.token = token
        self.allow_insecure_http = allow_insecure_http
        self.ca_file = ca_file
        self.allowed_redirect_hosts = list(allowed_redirect_hosts or [])
        self.timeout = float(timeout)
        self.retries = int(retries)
        self.backoff = float(backoff)
        self.max_redirects = int(max_redirects)
        self.chunk_size = int(chunk_size)


def describe_url(url):
    """The URL without its query string or user information, for messages."""
    parts = urllib.parse.urlsplit(url)
    host = parts.hostname or "?"
    port = (":%d" % parts.port) if parts.port else ""
    return "%s://%s%s%s" % (parts.scheme, host, port, parts.path)


def parse_namenode_url(url, allow_insecure_http=False):
    """Check the configured NameNode URL and return (scheme, host, port)."""
    if not url:
        raise HdfsProtocolError("no NameNode URL is configured (set hdfs.namenode_url)")
    if CONTROL.search(url):
        raise HdfsProtocolError("the NameNode URL contains spaces or control characters")
    parts = urllib.parse.urlsplit(url)
    if parts.scheme not in ("http", "https"):
        raise HdfsProtocolError("the NameNode URL must start with https:// (or http:// with the "
                                "insecure development switch)")
    if parts.username is not None or parts.password is not None or "@" in parts.netloc:
        raise HdfsProtocolError("the NameNode URL must not contain a user name or password")
    if not parts.hostname:
        raise HdfsProtocolError("the NameNode URL has no host")
    if parts.path not in ("", "/") or parts.query or parts.fragment:
        raise HdfsProtocolError("the NameNode URL must be just scheme, host and port")
    if parts.scheme == "http" and not allow_insecure_http:
        raise HdfsProtocolError(
            "plain http:// NameNode URLs are refused because user names and data would cross the "
            "network unprotected; use https://, or set hdfs.allow_insecure_http=true "
            "(AEGISFS_INT_HDFS_ALLOW_INSECURE_HTTP=1) for a development cluster only")
    try:
        port = parts.port or (443 if parts.scheme == "https" else 80)
    except ValueError:
        raise HdfsProtocolError("the NameNode URL has an invalid port")
    return parts.scheme, parts.hostname.lower(), port


def validate_redirect(location, current_url, scheme, namenode_host, allowed_hosts):
    """Return the absolute, checked URL for a Location header, or raise HdfsProtocolError."""
    if CONTROL.search(location):
        raise HdfsProtocolError("redirect refused: the Location header has control characters")
    absolute = urllib.parse.urljoin(current_url, location)
    parts = urllib.parse.urlsplit(absolute)
    if parts.scheme not in ("http", "https"):
        raise HdfsProtocolError("redirect refused: unsupported scheme %r" % parts.scheme)
    if parts.scheme != scheme:
        raise HdfsProtocolError(
            "redirect refused: it changes the scheme from %s to %s" % (scheme, parts.scheme))
    if parts.username is not None or parts.password is not None or "@" in parts.netloc:
        raise HdfsProtocolError("redirect refused: the URL contains user information")
    host = (parts.hostname or "").lower()
    if not host:
        raise HdfsProtocolError("redirect refused: the URL has no host")
    try:
        parts.port
    except ValueError:
        raise HdfsProtocolError("redirect refused: the URL has an invalid port")
    allowed = {namenode_host} | {h.lower() for h in allowed_hosts}
    if host not in allowed:
        raise HdfsProtocolError(
            "redirect refused: host %r is not the NameNode and is not in allowed_redirect_hosts"
            % host)
    return absolute


def is_temp_name(path):
    return bool(TEMP_PATTERN.match(path.rsplit("/", 1)[-1]))


class WebHdfsClient:
    def __init__(self, settings):
        self.settings = settings
        self.scheme, self.host, self.port = parse_namenode_url(
            settings.namenode_url, settings.allow_insecure_http)
        self._ssl = None
        if self.scheme == "https":
            # Certificate and host name verification stay on. There is deliberately no option
            # to turn them off; point ca_file at a private CA instead.
            self._ssl = ssl.create_default_context(cafile=settings.ca_file)

    # ---- plumbing ----
    def _connection(self, scheme, host, port):
        if scheme == "https":
            return http.client.HTTPSConnection(host, port, timeout=self.settings.timeout,
                                               context=self._ssl)
        return http.client.HTTPConnection(host, port, timeout=self.settings.timeout)

    def _namenode_url(self, path, op, params=None):
        query = [("op", op)]
        if self.settings.token:
            query.append(("delegation", self.settings.token))
        elif self.settings.user:
            query.append(("user.name", self.settings.user))
        for key, value in (params or {}).items():
            if value is not None:
                query.append((key, str(value)))
        return "%s://%s:%d/webhdfs/v1%s?%s" % (
            self.scheme, self.host, self.port, urllib.parse.quote(path, safe="/"),
            urllib.parse.urlencode(query))

    def _sleep(self, attempt):
        time.sleep(self.settings.backoff * (2 ** attempt))

    def _send(self, method, url, body_path=None, body_size=None):
        """One request, no redirect handling. Returns (connection, response)."""
        parts = urllib.parse.urlsplit(url)
        target = parts.path + ("?" + parts.query if parts.query else "")
        conn = self._connection(parts.scheme, parts.hostname, parts.port)
        try:
            if body_path is None:
                conn.request(method, target, headers={"Content-Length": "0"} if method == "PUT"
                             else {})
            else:
                conn.putrequest(method, target)
                conn.putheader("Content-Type", "application/octet-stream")
                conn.putheader("Content-Length", str(body_size))
                conn.endheaders()
                sent = 0
                with open(body_path, "rb") as handle:
                    while sent < body_size:
                        block = handle.read(min(self.settings.chunk_size, body_size - sent))
                        if not block:
                            raise HdfsUnavailable("the local file was shorter than expected "
                                                  "while uploading")
                        conn.send(block)
                        sent += len(block)
            return conn, conn.getresponse()
        except HdfsError:
            conn.close()
            raise
        except ssl.SSLCertVerificationError as exc:
            conn.close()
            raise HdfsUnavailable("TLS certificate verification failed for %s: %s"
                                  % (describe_url(url), exc.verify_message))
        except (OSError, http.client.HTTPException) as exc:
            conn.close()
            raise HdfsUnavailable("cannot talk to %s: %s" % (describe_url(url), exc))

    @staticmethod
    def _raise_for_response(status, body, url):
        remote = None
        message = ""
        try:
            parsed = json.loads(body.decode("utf-8", "replace"))
            remote = parsed.get("RemoteException") if isinstance(parsed, dict) else None
        except ValueError:
            pass
        if remote:
            name = remote.get("exception", "")
            message = remote.get("message", "") or name
            if name == "FileNotFoundException":
                raise HdfsNotFound(message, status, remote)
            if name == "FileAlreadyExistsException":
                raise HdfsExists(message, status, remote)
            if name in ("AccessControlException", "SecurityException",
                        "AuthorizationException", "AuthenticationException"):
                raise HdfsAccessDenied(message, status, remote)
            if name in ("SafeModeException", "StandbyException", "RetriableException"):
                raise HdfsUnavailable("%s: %s" % (name, message), status, remote)
            if name in ("IllegalArgumentException", "InvalidPathException",
                        "PathIsNotEmptyDirectoryException", "ParentNotDirectoryException"):
                raise HdfsBadRequest("%s: %s" % (name, message), status, remote)
            detail = "%s: %s" % (name, message)
        else:
            detail = body.decode("utf-8", "replace").strip()[:200]
        if status in (401, 403):
            raise HdfsAccessDenied("HTTP %d from %s%s" % (status, describe_url(url),
                                                           (": " + detail) if detail else ""),
                                   status, remote)
        if status == 404:
            raise HdfsNotFound("HTTP 404 from %s%s" % (describe_url(url),
                                                       (": " + detail) if detail else ""),
                               status, remote)
        if status >= 500:
            raise HdfsUnavailable("HTTP %d from %s%s" % (status, describe_url(url),
                                                         (": " + detail) if detail else ""),
                                  status, remote)
        raise HdfsProtocolError("HTTP %d from %s%s" % (status, describe_url(url),
                                                       (": " + detail) if detail else ""),
                                status, remote)

    def _resolve(self, method, url, expect_redirect, body_path=None, body_size=None):
        """Send to the NameNode URL and walk validated redirects.

        Returns (connection, response, final_url) with the response unread. The caller closes the
        connection. With `body_path` the data is sent only to the final, non-redirect hop: the
        first request never carries it.
        """
        seen = {url}
        current = url
        hops = 0
        while True:
            last = hops > 0
            conn, resp = self._send(method, current,
                                    body_path if (last and body_path is not None) else None,
                                    body_size)
            if resp.status in (301, 302, 303, 307, 308):
                location = resp.getheader("Location")
                resp.read()
                conn.close()
                if not location:
                    raise HdfsProtocolError("redirect from %s without a Location header"
                                            % describe_url(current))
                hops += 1
                if hops > self.settings.max_redirects:
                    raise HdfsProtocolError("redirect refused: more than %d redirects"
                                            % self.settings.max_redirects)
                nxt = validate_redirect(location, current, self.scheme, self.host,
                                        self.settings.allowed_redirect_hosts)
                if nxt in seen:
                    raise HdfsProtocolError("redirect refused: loop at %s" % describe_url(nxt))
                seen.add(nxt)
                current = nxt
                continue
            return conn, resp, current

    def _json_call(self, method, path, op, params=None, idempotent=True):
        """A call whose answer is a small JSON document (or empty)."""
        url = self._namenode_url(path, op, params)
        attempts = self.settings.retries + 1 if idempotent else 1
        for attempt in range(attempts):
            try:
                conn, resp, final = self._resolve(method, url, False)
                try:
                    body = resp.read()
                    status = resp.status
                finally:
                    conn.close()
                if status in RETRY_STATUS and attempt + 1 < attempts:
                    self._sleep(attempt)
                    continue
                if status >= 400:
                    self._raise_for_response(status, body, final)
                if not body:
                    return {}
                try:
                    return json.loads(body.decode("utf-8"))
                except ValueError:
                    raise HdfsProtocolError("%s returned a body that is not JSON"
                                            % describe_url(final))
            except HdfsUnavailable:
                if attempt + 1 >= attempts:
                    raise
                self._sleep(attempt)

    # ---- operations ----
    def status(self, path):
        """GETFILESTATUS. Returns the FileStatus dict; raises HdfsNotFound if absent."""
        return self._json_call("GET", path, "GETFILESTATUS")["FileStatus"]

    def exists(self, path):
        try:
            self.status(path)
            return True
        except HdfsNotFound:
            return False

    def list(self, path):
        data = self._json_call("GET", path, "LISTSTATUS")
        return data["FileStatuses"]["FileStatus"]

    def mkdirs(self, path):
        return bool(self._json_call("PUT", path, "MKDIRS").get("boolean"))

    def rename(self, source, destination, overwrite=False):
        """RENAME. Without overwrite a destination that already exists makes it fail.

        Not retried: after a lost answer the caller checks the paths instead of repeating it.
        """
        params = {"destination": destination}
        if overwrite:
            params["renameoptions"] = "OVERWRITE"
        data = self._json_call("PUT", source, "RENAME", params, idempotent=False)
        if overwrite and not data:
            return True  # the options form answers 200 with no body on success
        return bool(data.get("boolean"))

    def delete_temp(self, path):
        """Delete a file this tool created. Refuses any path that is not one of our temp names."""
        if not is_temp_name(path):
            raise HdfsBadRequest("refusing to delete %s: not one of this tool's temporary files"
                                 % path)
        data = self._json_call("DELETE", path, "DELETE", {"recursive": "false"})
        return bool(data.get("boolean"))

    def checksum(self, path):
        """GETFILECHECKSUM: HDFS's own composite checksum (not comparable to SHA-256)."""
        url = self._namenode_url(path, "GETFILECHECKSUM")
        conn, resp, final = self._resolve("GET", url, True)
        try:
            body = resp.read()
            if resp.status >= 400:
                self._raise_for_response(resp.status, body, final)
            return json.loads(body.decode("utf-8"))["FileChecksum"]
        finally:
            conn.close()

    def namenode_reachable(self):
        """Return the root FileStatus, proving the NameNode answers WebHDFS requests."""
        return self.status("/")

    def safemode(self):
        """True/False from the NameNode's JMX page, or None when it cannot be determined."""
        url = "%s://%s:%d/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo" % (
            self.scheme, self.host, self.port)
        try:
            conn, resp, _ = self._resolve("GET", url, False)
            try:
                body = resp.read()
                if resp.status != 200:
                    return None
            finally:
                conn.close()
            text = json.loads(body.decode("utf-8"))["beans"][0].get("Safemode")
            return bool(text)
        except (HdfsError, ValueError, KeyError, IndexError):
            return None

    def upload(self, local_path, hdfs_path, size, progress=None):
        """Create a new file (never overwrites) from a local file of known size.

        Step one asks the NameNode, step two sends the bytes to the DataNode it names. Not
        retried here: a half-written create must be cleaned up by the caller, not repeated.
        """
        url = self._namenode_url(hdfs_path, "CREATE", {"overwrite": "false"})
        conn, resp, final = self._resolve("PUT", url, True, body_path=local_path,
                                          body_size=size)
        try:
            body = resp.read()
            if resp.status != 201:
                if resp.status in (200, 307):
                    raise HdfsProtocolError("unexpected HTTP %d answering the create at %s"
                                            % (resp.status, describe_url(final)))
                self._raise_for_response(resp.status, body, final)
        finally:
            conn.close()
        if final == url:
            raise HdfsProtocolError("the NameNode accepted the create without naming a DataNode")

    def download(self, hdfs_path, write, expected_size=None):
        """Stream a file into `write(bytes)` in chunks, resuming after a dropped connection.

        Returns the number of bytes delivered. Resumes use the OPEN offset parameter, up to the
        configured number of retries, and never deliver a byte twice.
        """
        delivered = 0
        failures = 0
        while True:
            params = {"offset": delivered} if delivered else None
            url = self._namenode_url(hdfs_path, "OPEN", params)
            conn = None
            try:
                conn, resp, final = self._resolve("GET", url, True)
                if resp.status >= 400:
                    body = resp.read()
                    if resp.status in RETRY_STATUS and failures < self.settings.retries:
                        failures += 1
                        self._sleep(failures - 1)
                        continue
                    self._raise_for_response(resp.status, body, final)
                while True:
                    block = resp.read(self.settings.chunk_size)
                    if not block:
                        break
                    try:
                        write(block)
                    except Exception as sink_exc:
                        raise _SinkError(sink_exc)
                    delivered += len(block)
                if expected_size is not None and delivered < expected_size:
                    raise HdfsUnavailable("the connection ended after %d of %d bytes"
                                          % (delivered, expected_size))
                return delivered
            except _SinkError as exc:
                raise exc.original
            except (HdfsUnavailable, http.client.HTTPException, OSError) as exc:
                if failures >= self.settings.retries:
                    if isinstance(exc, HdfsError):
                        raise
                    raise HdfsUnavailable("reading %s failed after %d bytes: %s"
                                          % (hdfs_path, delivered, exc))
                failures += 1
                self._sleep(failures - 1)
            finally:
                if conn is not None:
                    conn.close()

    def sha256(self, hdfs_path, expected_size=None):
        digest = hashlib.sha256()
        total = self.download(hdfs_path, digest.update, expected_size)
        return digest.hexdigest(), total
