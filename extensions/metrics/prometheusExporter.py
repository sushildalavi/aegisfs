#!/usr/bin/env python3
"""Prometheus exporter for the AegisFS CLI metrics endpoint."""
import os, subprocess
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
BIN = os.environ.get("AEGISFS_BIN", "aegisfs"); ENTRY = os.environ.get("AEGISFS_ENTRY", "127.0.0.1:9100")
def scrape():
    p = subprocess.run([BIN, "--entry", ENTRY, "metrics"], text=True, capture_output=True)
    out = ["# TYPE aegisfs_info gauge"]
    for line in p.stdout.splitlines():
        if "=" not in line or not line.startswith(("counter.", "lat.")): continue
        key, value = line.split("=", 1)
        try: float(value)
        except ValueError: continue
        out.append("aegisfs_" + key.replace(".", "_") + " " + value)
    return ("\n".join(out) + "\n").encode()
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/metrics": self.send_response(404); self.end_headers(); return
        body = scrape(); self.send_response(200); self.send_header("Content-Type", "text/plain; version=0.0.4"); self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)
    def log_message(self, *_): pass
if __name__ == "__main__":
    ThreadingHTTPServer((os.environ.get("EXPORTER_HOST", "0.0.0.0"), int(os.environ.get("EXPORTER_PORT", "9108"))), Handler).serve_forever()
