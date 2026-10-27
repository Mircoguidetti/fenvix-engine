#!/usr/bin/env python3
"""A minimal fake S3 (path style: PUT/GET/HEAD /bucket/key) for checking s3probe's counters locally, WITHOUT AWS.
Every FAIL_EVERY-th request is answered 500 first, so object_store retries and the HTTP-level count exceeds the
CountingStore count. Usage: fake_s3.py <port> [FAIL_EVERY=0]"""
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading

store, n, lock = {}, [0], threading.Lock()
FAIL_EVERY = int(sys.argv[2]) if len(sys.argv) > 2 else 0


class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def fail(self):
        with lock:
            n[0] += 1
            bad = FAIL_EVERY and n[0] % FAIL_EVERY == 0
        if bad:
            self.send_response(500); self.send_header("Content-Length", "0"); self.end_headers()
        return bad

    def do_PUT(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        if self.fail():
            return
        store[self.path] = body
        self.send_response(200); self.send_header("ETag", '"x"'); self.send_header("Content-Length", "0"); self.end_headers()

    def do_GET(self):
        if self.fail():
            return
        b = store.get(self.path)
        if b is None:
            self.send_response(404); self.send_header("Content-Length", "0"); self.end_headers(); return
        self.send_response(200); self.send_header("Content-Length", str(len(b))); self.send_header("ETag", '"x"'); self.end_headers(); self.wfile.write(b)


ThreadingHTTPServer(("127.0.0.1", int(sys.argv[1])), H).serve_forever()
