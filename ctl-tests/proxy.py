"""A counting HTTP proxy between mongo-server and MinIO: an independent ground truth for the requests the server
makes. The server signs requests for the proxy's address; the proxy forwards them unchanged (same Host header, so the
signature stays valid) and counts them by kind, by path prefix and in bytes."""
import collections
import http.client
import http.server
import socketserver
import threading
import urllib.parse

from lib import MINIO, free_port


def classify(method, path, query, headers):
    q = urllib.parse.parse_qs(query, keep_blank_values=True)
    if method == "GET" and ("list-type" in q or "prefix" in q or "delimiter" in q):
        return "LIST"
    if method == "GET":
        return "GET_RANGE" if "Range" in headers or "range" in headers else "GET"
    if method == "HEAD":
        return "HEAD"
    if method == "PUT":
        if "x-amz-copy-source" in {k.lower() for k in headers}:
            return "COPY"
        if "partNumber" in q:
            return "PUT_PART"
        return "PUT"
    if method == "POST":
        if "uploads" in q:
            return "MP_CREATE"
        if "uploadId" in q:
            return "MP_COMPLETE"
        if "delete" in q:
            return "DELETE_MULTI"
    if method == "DELETE":
        return "DELETE"
    return method


class Counters:
    def __init__(self):
        self.lock = threading.Lock()
        self.reset()

    def reset(self):
        with self.lock:
            self.n = collections.Counter()
            self.bytes_in = collections.Counter()   # bytes the server sent (request bodies)
            self.bytes_out = collections.Counter()  # bytes the server received (response bodies)
            self.by_prefix = collections.defaultdict(collections.Counter)
            self.bytes_by_prefix = collections.defaultdict(collections.Counter)

    def add(self, kind, prefix, req_bytes, resp_bytes):
        with self.lock:
            self.n[kind] += 1
            self.bytes_in[kind] += req_bytes
            self.bytes_out[kind] += resp_bytes
            self.by_prefix[prefix][kind] += 1
            self.bytes_by_prefix[prefix][kind + ":in"] += req_bytes
            self.bytes_by_prefix[prefix][kind + ":out"] += resp_bytes

    def snapshot(self):
        with self.lock:
            n = dict(self.n)
            return {
                "n": n,
                "get_tier": n.get("GET", 0) + n.get("GET_RANGE", 0) + n.get("HEAD", 0),
                "put_tier": n.get("PUT", 0) + n.get("PUT_PART", 0) + n.get("MP_CREATE", 0) + n.get("MP_COMPLETE", 0) + n.get("COPY", 0) + n.get("LIST", 0),
                "put_bytes": sum(v for k, v in self.bytes_in.items() if k in ("PUT", "PUT_PART")),
                "get_bytes": sum(v for k, v in self.bytes_out.items() if k in ("GET", "GET_RANGE")),
                "by_prefix": {k: dict(v) for k, v in self.by_prefix.items()},
                "bytes_by_prefix": {k: dict(v) for k, v in self.bytes_by_prefix.items()},
            }


class _Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def _do(self):
        counters = self.server.counters
        u = urllib.parse.urlsplit(self.path)
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else b""
        host, port = urllib.parse.urlsplit(MINIO).netloc.split(":")
        conn = http.client.HTTPConnection(host, int(port), timeout=120)
        headers = {k: v for k, v in self.headers.items()}
        conn.request(self.command, self.path, body=body or None, headers=headers)
        r = conn.getresponse()
        data = r.read() if self.command != "HEAD" else b""
        parts = [p for p in u.path.split("/") if p]
        key = "/".join(parts[1:3]) if len(parts) > 2 else "/".join(parts[1:])
        prefix = "/".join(parts[1:3]) if len(parts) > 2 else (parts[1] if len(parts) > 1 else "")
        if "prefix" in urllib.parse.parse_qs(u.query):
            pq = urllib.parse.parse_qs(u.query)["prefix"][0]
            prefix = "/".join(pq.split("/")[:2])
        counters.add(classify(self.command, u.path, u.query, self.headers), prefix, len(body), len(data) if r.status < 300 else 0)  # error bodies (404 probes) are not object data
        self.send_response(r.status)
        for k, v in r.getheaders():
            if k.lower() in ("transfer-encoding", "connection", "content-length"):
                continue
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(data)
        conn.close()

    do_GET = do_PUT = do_POST = do_DELETE = do_HEAD = _do


class _Server(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    request_queue_size = 256


class CountingProxy:
    def __init__(self):
        self.port = free_port()
        self.counters = Counters()
        self.srv = _Server(("127.0.0.1", self.port), _Handler)
        self.srv.counters = self.counters
        self.thread = threading.Thread(target=self.srv.serve_forever, daemon=True)
        self.thread.start()

    @property
    def endpoint(self):
        return f"http://127.0.0.1:{self.port}"

    def reset(self):
        self.counters.reset()

    def snapshot(self):
        return self.counters.snapshot()

    def stop(self):
        self.srv.shutdown()
