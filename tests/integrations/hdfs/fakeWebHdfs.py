"""An in-memory stand-in for a WebHDFS NameNode and DataNode, for unit tests.

It speaks just enough of the protocol to exercise the client: the NameNode answers metadata calls
and 307-redirects CREATE, OPEN and GETFILECHECKSUM to the DataNode; the DataNode takes and serves
the bytes. Test hooks let a test change the Location header, inject errors, or cut a download
short. It records every request so a test can check what was sent where.
"""
import hashlib
import json
import ssl
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def remote_error(handler, status, exception, message):
    body = json.dumps({"RemoteException": {"exception": exception, "message": message,
                                           "javaClassName": "java.io." + exception}}).encode()
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


class FakeHdfs:
    def __init__(self, tls=None):
        self.tls = tls  # (certfile, keyfile) to serve https
        self.files = {}          # path -> bytes
        self.dirs = {"/"}
        self.mtime = {}
        self.requests = []       # (role, method, path, {query})
        self.max_upload_read = 0  # biggest single read the DataNode did from the socket
        self.location_override = None  # callable(default_location) -> str
        self.nn_errors = []      # statuses (or (status, exception)) to answer the next NN calls
        self.drop_open_after = None    # cut the first OPEN response after this many bytes
        self.open_drops_left = 0
        self.dn_error = None     # (status, exception, message) for every DataNode PUT
        self.corrupt_upload = False  # flip the last byte of every stored upload
        self.stall_upload = False  # DataNode accepts bytes then never answers
        self.lock = threading.Lock()
        self.nn = self._server("nn")
        self.dn = self._server("dn")
        self.threads = []
        for srv in (self.nn, self.dn):
            t = threading.Thread(target=srv.serve_forever, kwargs={"poll_interval": 0.02},
                                 daemon=True)
            t.start()
            self.threads.append(t)

    def _server(self, role):
        owner = self

        class Handler(_Handler):
            fake = owner
            server_role = role

        srv = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        srv.daemon_threads = True
        if self.tls:
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.load_cert_chain(*self.tls)
            srv.socket = context.wrap_socket(srv.socket, server_side=True)
        return srv

    @property
    def nn_url(self):
        return "%s://127.0.0.1:%d" % ("https" if self.tls else "http", self.nn.server_address[1])

    @property
    def dn_port(self):
        return self.dn.server_address[1]

    def close(self):
        for srv in (self.nn, self.dn):
            srv.shutdown()
            srv.server_close()

    def put_file(self, path, data):
        self.files[path] = data
        self.mtime[path] = int(time.time() * 1000)
        parent = path.rsplit("/", 1)[0] or "/"
        self.dirs.add(parent)

    def status_json(self, path):
        if path in self.files:
            return {"type": "FILE", "length": len(self.files[path]), "pathSuffix": "",
                    "owner": "tester", "group": "g", "permission": "644", "replication": 2,
                    "blockSize": 1 << 24, "modificationTime": self.mtime.get(path, 0),
                    "accessTime": 0}
        if path in self.dirs:
            return {"type": "DIRECTORY", "length": 0, "pathSuffix": "", "owner": "tester",
                    "group": "g", "permission": "755", "replication": 0, "blockSize": 0,
                    "modificationTime": 0, "accessTime": 0}
        return None


class _Handler(BaseHTTPRequestHandler):
    fake = None
    server_role = ""
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def _parse(self):
        parts = urllib.parse.urlsplit(self.path)
        query = dict(urllib.parse.parse_qsl(parts.query, keep_blank_values=True))
        path = urllib.parse.unquote(parts.path)
        if path.startswith("/webhdfs/v1"):
            path = path[len("/webhdfs/v1"):] or "/"
        self.fake.requests.append((self.server_role, self.command, path, query))
        return path, query

    def _json(self, obj, status=200):
        body = json.dumps(obj).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _redirect(self, path, query):
        default = "%s://127.0.0.1:%d/webhdfs/v1%s?%s" % (
            "https" if self.fake.tls else "http", self.fake.dn_port, urllib.parse.quote(path), urllib.parse.urlencode(query))
        location = self.fake.location_override(default) if self.fake.location_override \
            else default
        self.send_response(307)
        self.send_header("Location", location)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        self._dispatch()

    def do_PUT(self):
        self._dispatch()

    def do_DELETE(self):
        self._dispatch()

    def _dispatch(self):
        path, query = self._parse()
        op = query.get("op", "")
        if self.server_role == "nn":
            if self.fake.nn_errors:
                err = self.fake.nn_errors.pop(0)
                if isinstance(err, tuple):
                    return remote_error(self, err[0], err[1], err[2])
                self.send_response(err)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            return self._namenode(path, query, op)
        return self._datanode(path, query, op)

    def _drain(self):
        n = int(self.headers.get("Content-Length") or 0)
        while n > 0:
            n -= len(self.rfile.read(min(n, 65536)))

    def _namenode(self, path, query, op):
        f = self.fake
        self._drain()
        if op == "GETFILESTATUS":
            st = f.status_json(path)
            if st is None:
                return remote_error(self, 404, "FileNotFoundException",
                                    "File does not exist: " + path)
            return self._json({"FileStatus": st})
        if op == "LISTSTATUS":
            if path not in f.dirs:
                return remote_error(self, 404, "FileNotFoundException",
                                    "File " + path + " does not exist.")
            prefix = path.rstrip("/") + "/"
            out = []
            for p in sorted(f.files):
                if p.startswith(prefix) and "/" not in p[len(prefix):]:
                    st = f.status_json(p)
                    st["pathSuffix"] = p[len(prefix):]
                    out.append(st)
            for d in sorted(f.dirs):
                if d != path and d.startswith(prefix) and "/" not in d[len(prefix):]:
                    st = f.status_json(d)
                    st["pathSuffix"] = d[len(prefix):]
                    out.append(st)
            return self._json({"FileStatuses": {"FileStatus": out}})
        if op == "MKDIRS":
            f.dirs.add(path)
            return self._json({"boolean": True})
        if op == "RENAME":
            dest = query["destination"]
            if path not in f.files:
                return self._json({"boolean": False})
            if dest in f.files:
                if query.get("renameoptions") == "OVERWRITE":
                    f.files[dest] = f.files.pop(path)
                    f.mtime[dest] = int(time.time() * 1000)
                    self.send_response(200)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                return self._json({"boolean": False})
            f.files[dest] = f.files.pop(path)
            f.mtime[dest] = int(time.time() * 1000)
            return self._json({"boolean": True})
        if op == "DELETE":
            existed = path in f.files
            f.files.pop(path, None)
            return self._json({"boolean": existed})
        if op in ("CREATE", "OPEN", "GETFILECHECKSUM"):
            if op == "CREATE" and path in f.files and query.get("overwrite") != "true":
                return remote_error(self, 403, "FileAlreadyExistsException",
                                    path + " already exists")
            if op != "CREATE" and path not in f.files:
                return remote_error(self, 404, "FileNotFoundException",
                                    "File does not exist: " + path)
            return self._redirect(path, query)
        return remote_error(self, 400, "IllegalArgumentException", "unknown op " + op)

    def _datanode(self, path, query, op):
        f = self.fake
        if op == "CREATE":
            if f.dn_error:
                self._drain()
                return remote_error(self, *f.dn_error)
            remaining = int(self.headers.get("Content-Length") or 0)
            chunks = []
            while remaining > 0:
                block = self.rfile.read(min(remaining, 1 << 20))
                if not block:
                    break
                f.max_upload_read = max(f.max_upload_read, len(block))
                chunks.append(block)
                remaining -= len(block)
            if f.stall_upload:
                time.sleep(30)
                return
            data = b"".join(chunks)
            if f.corrupt_upload and data:
                data = data[:-1] + bytes([data[-1] ^ 1])
            f.put_file(path, data)
            self.send_response(201)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if op == "OPEN":
            data = f.files.get(path)
            if data is None:
                return remote_error(self, 404, "FileNotFoundException", "gone")
            offset = int(query.get("offset", 0))
            body = data[offset:]
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if f.drop_open_after is not None and f.open_drops_left > 0:
                f.open_drops_left -= 1
                self.wfile.write(body[:f.drop_open_after])
                self.wfile.flush()
                self.close_connection = True
                self.connection.close()
                return
            self.wfile.write(body)
            return
        if op == "GETFILECHECKSUM":
            data = f.files.get(path, b"")
            return self._json({"FileChecksum": {"algorithm": "MD5-of-0MD5-of-512CRC32C",
                                                "bytes": hashlib.md5(data).hexdigest(),
                                                "length": 28}})
        return remote_error(self, 400, "IllegalArgumentException", "unknown op " + op)
