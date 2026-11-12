"""An HTTP forwarder in front of the object store that can hold back some requests: while a rule is set, every request
that matches it (method and a substring of the path) waits, without an answer, until the rule is cleared; every other
request goes through. A store that stops answering for one database only, so that a control operation on that database
can still run its other steps. Used by ctl-tests/test_31_lease_lost_store_down.py (erase with the store frozen).

    p = PathProxy(("127.0.0.1", 9000)); p.endpoint -> "http://127.0.0.1:<port>"
    p.hold("PUT", "/tenants/db1/"); ...; p.release(); p.held -> requests held so far

A rule can also refuse instead of hold (fix-store-errors): every matching request is answered at once with an error, as
a full disk (MinIO 507 XMinioStorageFull) or S3 throttling (503 SlowDown) does, until the rule is cleared:
    p.refuse(("PUT", "POST"), "/tenants/", 507); ...; p.release(); p.refused -> requests refused so far
"""
import http.client
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HOP = {"expect", "connection", "keep-alive", "proxy-connection", "transfer-encoding", "te", "trailer", "upgrade"}


class PathProxy:
    def __init__(self, dst, port=0):
        self.dst = dst
        self.rule = None
        self.held = 0
        self.refusal = None   # (methods, path_part, status)
        self.refused = 0
        self.journal = None   # a list to record (unix time, method, path, status) of every request, or None
        self.lock = threading.Lock()
        proxy = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *a):
                pass

            def _any(self):
                n = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(n) if n else b""
                if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
                    body = self._chunked()
                f = proxy.refusal
                if f and self.command in f[0] and f[1] in self.path.split("?")[0]:
                    with proxy.lock:
                        proxy.refused += 1
                        if proxy.journal is not None:
                            proxy.journal.append((time.time(), self.command, self.path.split("?")[0], f[2]))
                    code, msg = ("XMinioStorageFull", "Storage backend has reached its minimum free drive threshold.") if f[2] == 507 else \
                        ("SlowDown", "Please reduce your request rate.") if f[2] == 503 else ("InternalError", "We encountered an internal error.")
                    data = f'<?xml version="1.0" encoding="UTF-8"?>\n<Error><Code>{code}</Code><Message>{msg}</Message></Error>'.encode()
                    try:
                        self.send_response_only(f[2], {507: "Insufficient Storage", 503: "Slow Down"}.get(f[2], "Internal Server Error"))
                        self.send_header("Content-Type", "application/xml")
                        self.send_header("Content-Length", str(len(data)))
                        self.end_headers()
                        self.wfile.write(data)
                    except OSError:
                        self.close_connection = True
                    return
                counted = False
                while True:
                    r = proxy.rule
                    if not (r and self.command == r[0] and r[1] in self.path.split("?")[0]):
                        break
                    if not counted:
                        counted = True
                        with proxy.lock:
                            proxy.held += 1
                    time.sleep(0.05)
                headers = {k: v for k, v in self.headers.items() if k.lower() not in HOP}
                headers["Content-Length"] = str(len(body))
                c = http.client.HTTPConnection(*proxy.dst, timeout=120)
                try:
                    c.request(self.command, self.path, body=body, headers=headers)
                    resp = c.getresponse()
                    data = resp.read()
                except OSError:
                    self.close_connection = True
                    return
                finally:
                    c.close()
                if proxy.journal is not None:
                    with proxy.lock:
                        proxy.journal.append((time.time(), self.command, self.path.split("?")[0], resp.status))
                        if self.command == "POST" and "delete" in self.path.split("?", 1)[-1]:
                            # a multi-object delete: one entry per key of its body
                            import re as _re
                            for k in _re.findall(rb"<Key>([^<]+)</Key>", body):
                                proxy.journal.append((time.time(), "DELETE*", "/" + self.path.split("?")[0].strip("/") + "/" + k.decode(), resp.status))
                try:
                    self.send_response_only(resp.status, resp.reason)
                    for k, v in resp.getheaders():
                        if k.lower() not in HOP and k.lower() != "content-length":
                            self.send_header(k, v)
                    # A HEAD answer carries the object's own length (fix-store-errors: this forwarder sent 0, so every
                    # object looked empty to a HEAD; SlateDB took the WAL objects of a writer it replaced for empty fences
                    # and skipped them on replay: acknowledged writes looked lost, through this forwarder only. The same
                    # defect was fixed in ctl-tests/proxy.py in integration-3.)
                    if self.command == "HEAD" and resp.getheader("Content-Length") is not None:
                        self.send_header("Content-Length", resp.getheader("Content-Length"))
                    else:
                        self.send_header("Content-Length", str(len(data)))
                    self.end_headers()
                    if self.command != "HEAD":
                        self.wfile.write(data)
                except OSError:
                    self.close_connection = True

            def _chunked(self):
                out = b""
                while True:
                    size = int(self.rfile.readline().split(b";")[0].strip(), 16)
                    if size == 0:
                        while self.rfile.readline() not in (b"\r\n", b"\n", b""):
                            pass
                        return out
                    out += self.rfile.read(size)
                    self.rfile.readline()

            do_GET = do_PUT = do_POST = do_DELETE = do_HEAD = _any

        self.srv = ThreadingHTTPServer(("127.0.0.1", port), Handler)
        self.srv.daemon_threads = True
        self.port = self.srv.server_address[1]
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()

    @property
    def endpoint(self):
        return f"http://127.0.0.1:{self.port}"

    def hold(self, method, path_part):
        self.rule = (method, path_part)

    def refuse(self, methods, path_part, status=507):
        self.refusal = (tuple(methods), path_part, status)

    def release(self):
        self.rule = None
        self.refusal = None

    def close(self):
        self.srv.shutdown()
