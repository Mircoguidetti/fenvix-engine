"""An HTTP forwarder in front of the object store that can hold back some requests: while a rule is set, every request
that matches it (method and a substring of the path) waits, without an answer, until the rule is cleared; every other
request goes through. A store that stops answering for one database only, so that a control operation on that database
can still run its other steps. Used by ctl-tests/test_31_lease_lost_store_down.py (erase with the store frozen).

    p = PathProxy(("127.0.0.1", 9000)); p.endpoint -> "http://127.0.0.1:<port>"
    p.hold("PUT", "/tenants/db1/"); ...; p.release(); p.held -> requests held so far
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
                try:
                    self.send_response_only(resp.status, resp.reason)
                    for k, v in resp.getheaders():
                        if k.lower() not in HOP and k.lower() != "content-length":
                            self.send_header(k, v)
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

    def release(self):
        self.rule = None

    def close(self):
        self.srv.shutdown()
