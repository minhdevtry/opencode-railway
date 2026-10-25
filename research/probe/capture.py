#!/usr/bin/env python3
"""Server giả để bắt nguyên văn request của opencode CLI thật (header + body), ghi ra capture.jsonl."""
import json, sys
from http.server import BaseHTTPRequestHandler, HTTPServer
OUT = sys.argv[2] if len(sys.argv) > 2 else "capture.jsonl"
class H(BaseHTTPRequestHandler):
    def do_POST(self):
        n = int(self.headers.get("content-length", 0)); body = self.rfile.read(n).decode("utf-8", "replace")
        with open(OUT, "a") as f:
            f.write(json.dumps({"path": self.path, "headers": dict(self.headers), "body": body}) + "\n")
        self.send_response(500); self.send_header("content-type", "application/json"); self.end_headers()
        self.wfile.write(b'{"error":{"message":"captured"}}')
    do_GET = do_POST
    def log_message(self, *a): pass
HTTPServer(("127.0.0.1", int(sys.argv[1])), H).serve_forever()
