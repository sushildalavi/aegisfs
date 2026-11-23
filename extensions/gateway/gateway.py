#!/usr/bin/env python3
"""Dependency-free REST gateway for an AegisFS entry node."""
import json
import os
import subprocess
import sys
import tempfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote, urlparse

BIN = os.environ.get("AEGISFS_BIN", "aegisfs")
ENTRY = os.environ.get("AEGISFS_ENTRY", "127.0.0.1:9100")
TOKEN = os.environ.get("AEGISFS_AUTH_TOKEN", "")

def observe(op, name):
    """Optionally tell the Cassandra index that a file changed.

    Off unless AEGISFS_INT_CASSANDRA_SPOOL_DIR is set. The hook only appends a line to a local file,
    and anything that goes wrong here is ignored, so it can never change the reply to the client.
    """
    if not os.environ.get("AEGISFS_INT_CASSANDRA_SPOOL_DIR"):
        return
    try:
        here = os.path.dirname(os.path.abspath(__file__))
        extra = os.environ.get("AEGISFS_INTEGRATIONS_PATH") or os.path.join(here, "..", "integrations")
        if extra not in sys.path:
            sys.path.insert(0, extra)
        from aegisfsIntegrations.cassandra.hook import observe as queue_index
        queue_index(op, name)
    except Exception:
        pass

def cli(*args):
    p = subprocess.run([BIN, "--entry", ENTRY, *args], text=True, capture_output=True)
    fields = {}
    for line in p.stdout.splitlines():
        if "=" in line:
            k, v = line.split("=", 1); fields[k] = v
    return p.returncode, fields, p.stderr.strip()

class Handler(BaseHTTPRequestHandler):
    server_version = "AegisFS-Gateway/0.1"
    def _authorized(self):
        return not TOKEN or self.headers.get("Authorization", "") == "Bearer " + TOKEN
    def _reply(self, status, obj, content_type="application/json"):
        body = obj if isinstance(obj, bytes) else json.dumps(obj).encode()
        self.send_response(status); self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)
    def _name(self):
        parts = [unquote(x) for x in urlparse(self.path).path.split("/") if x]
        return "/".join(parts[1:]) if len(parts) > 1 and parts[0] == "files" else ""
    def _operation(self, op, name, body):
        with tempfile.NamedTemporaryFile() as f:
            f.write(body); f.flush(); code, fields, err = cli(op, f.name, name)
        status = 200 if fields.get("status") in ("OK", "DEGRADED") else 409
        if status == 200:
            observe(op, name)
        self._reply(status, {"status": fields.get("status", "NETWORK_ERROR"), "fields": fields, "error": err})
    def do_GET(self):
        if not self._authorized(): return self._reply(401, {"error": "unauthorized"})
        path = urlparse(self.path).path
        if path == "/health":
            code, fields, err = cli("health"); return self._reply(200 if code == 0 else 503, {"status": fields.get("status"), "error": err})
        if path == "/status":
            code, fields, err = cli("status"); return self._reply(200 if code == 0 else 503, fields | ({"error": err} if err else {}))
        if path == "/metrics":
            code, fields, err = cli("metrics"); return self._reply(200 if code == 0 else 503, fields | ({"error": err} if err else {}))
        name = self._name()
        if not name: return self._reply(404, {"error": "use /files/{name}"})
        with tempfile.NamedTemporaryFile() as f:
            code, fields, err = cli("get", name, f.name)
            if code != 0: return self._reply(404 if fields.get("status") == "NOT_FOUND" else 503, {"status": fields.get("status"), "error": err})
            f.seek(0); return self._reply(200, f.read(), "application/octet-stream")
    def do_PUT(self):
        if not self._authorized(): return self._reply(401, {"error": "unauthorized"})
        name = self._name()
        if not name: return self._reply(400, {"error": "use /files/{name}"})
        return self._operation("create", name, self.rfile.read(int(self.headers.get("Content-Length", "0"))))
    def do_POST(self):
        if not self._authorized(): return self._reply(401, {"error": "unauthorized"})
        name = self._name()
        if not name: return self._reply(400, {"error": "use /files/{name}"})
        return self._operation("append", name, self.rfile.read(int(self.headers.get("Content-Length", "0"))))
    def log_message(self, fmt, *args):
        if os.environ.get("AEGISFS_GATEWAY_QUIET") != "1": super().log_message(fmt, *args)

if __name__ == "__main__":
    ThreadingHTTPServer((os.environ.get("GATEWAY_HOST", "0.0.0.0"), int(os.environ.get("GATEWAY_PORT", "8080"))), Handler).serve_forever()
