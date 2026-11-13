"""A forwarder in front of the object store that fails and slows requests at random (P-fix-slow-store item 4): a store
seen at 1 TB rates, where S3's throttling (503 SlowDown) and its latency tails are normal, not an outage.

    python3 chaosproxy.py --dst 127.0.0.1:9000 --port 9100 --fail-pct 1 --tail-pct 5 --tail-ms 200 --journal faults.log

- `--fail-pct`: that share of requests is answered 503 SlowDown at once, without reaching the store (S3's answer when a
  prefix is over its request rate). object_store retries it (its retry layer), as it would on S3.
- `--tail-pct`, `--tail-ms`: that share of the requests that are forwarded waits `--tail-ms` first (a tail of S3's
  latency).
- `--journal`: one line per injected fault: `<unix time> <503|tail> <method> <path>`, so a stall seen by a client can be
  matched with the faults injected during it.
- `--seed`: the random choice is seeded, so a run can be repeated with the same faults in the same order of requests.
- `--exclude`: requests whose path contains this text get no fault (for example the lease objects).

Importable too: `ChaosProxy(("127.0.0.1", 9000), fail_pct=1, tail_pct=5, tail_ms=200)`; `.endpoint`, `.counts`, `.close()`.
Upstream connections are kept alive per handler thread (one per client connection), so the forwarder adds about one
loopback hop per request, not a TCP handshake.
"""
import argparse
import http.client
import random
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HOP = {"expect", "connection", "keep-alive", "proxy-connection", "transfer-encoding", "te", "trailer", "upgrade"}


class ChaosProxy:
    def __init__(self, dst, port=0, fail_pct=0.0, tail_pct=0.0, tail_ms=0, journal=None, seed=None, exclude=None):
        self.dst = dst
        self.fail_pct, self.tail_pct, self.tail_ms = fail_pct, tail_pct, tail_ms
        self.exclude = exclude
        self.rng = random.Random(seed)
        self.lock = threading.Lock()
        self.counts = {"requests": 0, "failed": 0, "tails": 0}
        self.journal = open(journal, "a", buffering=1) if journal else None
        proxy = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *a):
                pass

            def _up(self):
                c = getattr(self, "_conn", None)
                if c is None:
                    c = http.client.HTTPConnection(*proxy.dst, timeout=300)
                    self._conn = c
                return c

            def _any(self):
                n = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(n) if n else b""
                if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
                    body = self._chunked()
                path = self.path.split("?")[0]
                fail = tail = False
                if not (proxy.exclude and proxy.exclude in path):
                    with proxy.lock:
                        r = proxy.rng.random() * 100
                        fail = r < proxy.fail_pct
                        tail = (not fail) and proxy.rng.random() * 100 < proxy.tail_pct
                with proxy.lock:
                    proxy.counts["requests"] += 1
                    if fail:
                        proxy.counts["failed"] += 1
                    if tail:
                        proxy.counts["tails"] += 1
                if (fail or tail) and proxy.journal:
                    proxy.journal.write(f"{time.time():.3f} {'503' if fail else 'tail'} {self.command} {path}\n")
                if fail:
                    data = b'<?xml version="1.0" encoding="UTF-8"?>\n<Error><Code>SlowDown</Code><Message>Please reduce your request rate.</Message></Error>'
                    try:
                        self.send_response_only(503, "Slow Down")
                        self.send_header("Content-Type", "application/xml")
                        self.send_header("Content-Length", str(len(data)))
                        self.end_headers()
                        if self.command != "HEAD":
                            self.wfile.write(data)
                    except OSError:
                        self.close_connection = True
                    return
                if tail:
                    time.sleep(proxy.tail_ms / 1000.0)
                headers = {k: v for k, v in self.headers.items() if k.lower() not in HOP}
                headers["Content-Length"] = str(len(body))
                for attempt in (0, 1):
                    c = self._up()
                    try:
                        c.request(self.command, self.path, body=body, headers=headers)
                        resp = c.getresponse()
                        data = resp.read()
                        break
                    except (OSError, http.client.HTTPException):
                        # a kept-alive upstream connection closed by the store: one new connection, then give up
                        c.close()
                        self._conn = None
                        if attempt == 1:
                            self.close_connection = True
                            return
                try:
                    self.send_response_only(resp.status, resp.reason)
                    for k, v in resp.getheaders():
                        if k.lower() not in HOP and k.lower() != "content-length":
                            self.send_header(k, v)
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

        class Srv(ThreadingHTTPServer):
            request_queue_size = 512

        self.srv = Srv(("127.0.0.1", port), Handler)
        self.srv.daemon_threads = True
        self.port = self.srv.server_address[1]
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()

    @property
    def endpoint(self):
        return f"http://127.0.0.1:{self.port}"

    def close(self):
        self.srv.shutdown()
        if self.journal:
            self.journal.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dst", default="127.0.0.1:9000")
    ap.add_argument("--port", type=int, default=9100)
    ap.add_argument("--fail-pct", type=float, default=1.0)
    ap.add_argument("--tail-pct", type=float, default=5.0)
    ap.add_argument("--tail-ms", type=int, default=200)
    ap.add_argument("--journal")
    ap.add_argument("--seed", type=int)
    ap.add_argument("--exclude")
    ap.add_argument("--stats-secs", type=int, default=60)
    a = ap.parse_args()
    h, p = a.dst.split(":")
    px = ChaosProxy((h, int(p)), a.port, a.fail_pct, a.tail_pct, a.tail_ms, a.journal, a.seed, a.exclude)
    print(f"chaos proxy on {px.endpoint} -> {a.dst}: {a.fail_pct}% 503, {a.tail_pct}% +{a.tail_ms} ms", flush=True)
    try:
        while True:
            time.sleep(a.stats_secs)
            print(f"{time.time():.0f} {px.counts}", flush=True)
    except KeyboardInterrupt:
        px.close()


if __name__ == "__main__":
    sys.exit(main())
