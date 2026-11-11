"""BlockProxy (tools/lease-stall/blockproxy.py) with a trace: the same byte forwarding and freeze (while frozen no byte
moves in either direction), plus a parser on each direction of each connection that logs every HTTP request and its
response status, and saves the body of every PUT under a chosen path part (WAL and SST objects) for forensic search."""
import os
import socket
import threading
import time


class _Stream:
    """Incremental HTTP/1.1 message parser for one direction of one connection."""

    def __init__(self, on_msg, is_req):
        self.buf = b""
        self.on_msg, self.is_req = on_msg, is_req
        self.state = "head"
        self.need = 0
        self.head = None
        self.body = []
        self.chunked = False
        self.reqs = []   # (for responses) methods of requests in flight, FIFO: HEAD responses have no body

    def feed(self, d):
        self.buf += d
        while True:
            if self.state == "head":
                i = self.buf.find(b"\r\n\r\n")
                if i < 0:
                    return
                head = self.buf[:i].decode("latin1")
                self.buf = self.buf[i + 4:]
                lines = head.split("\r\n")
                hd = {}
                for ln in lines[1:]:
                    if ":" in ln:
                        k, v = ln.split(":", 1)
                        hd[k.strip().lower()] = v.strip()
                self.head = (lines[0], hd)
                self.body = []
                if not self.is_req:
                    m = self.reqs.pop(0) if self.reqs else "?"
                    code = lines[0].split(" ")[1] if " " in lines[0] else "?"
                    if m == "HEAD" or code in ("204", "304") or code.startswith("1"):
                        if code.startswith("1"):
                            self.reqs.insert(0, m)
                        self._done()
                        continue
                if hd.get("transfer-encoding", "").lower() == "chunked":
                    self.chunked, self.state = True, "chunk"
                else:
                    self.chunked = False
                    self.need = int(hd.get("content-length", "0") or 0)
                    self.state = "body"
            elif self.state == "body":
                take = min(self.need, len(self.buf))
                self.body.append(self.buf[:take])
                self.buf = self.buf[take:]
                self.need -= take
                if self.need > 0:
                    return
                self._done()
            elif self.state == "chunk":
                i = self.buf.find(b"\r\n")
                if i < 0:
                    return
                size = int(self.buf[:i].split(b";")[0].strip() or b"0", 16)
                if size == 0:
                    j = self.buf.find(b"\r\n\r\n")
                    if j < 0:
                        return
                    self.buf = self.buf[j + 4:]
                    self._done()
                    continue
                if len(self.buf) < i + 2 + size + 2:
                    return
                self.body.append(self.buf[i + 2:i + 2 + size])
                self.buf = self.buf[i + 2 + size + 2:]

    def _done(self):
        self.state = "head"
        self.on_msg(self.head, b"".join(self.body))


class TraceProxy:
    def __init__(self, dst, logpath, save_dir=None, save_parts=("/wal/", "/compacted/"), port=0):
        self.dst = dst
        self.frozen = threading.Event()
        self.log = open(logpath, "a", buffering=1)
        self.save_dir, self.save_parts = save_dir, save_parts
        if save_dir:
            os.makedirs(save_dir, exist_ok=True)
        self.lock = threading.Lock()
        self.nconn = 0
        self.nsaved = 0
        self.ls = socket.socket()
        self.ls.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.ls.bind(("127.0.0.1", port))
        self.ls.listen(512)
        self.port = self.ls.getsockname()[1]
        threading.Thread(target=self._accept, daemon=True).start()

    @property
    def endpoint(self):
        return f"http://127.0.0.1:{self.port}"

    def note(self, s):
        with self.lock:
            self.log.write(f"{time.time():.3f} NOTE {s}\n")

    def freeze(self):
        self.note("freeze")
        self.frozen.set()

    def thaw(self):
        self.note("thaw")
        self.frozen.clear()

    def _wait_thawed(self):
        while self.frozen.is_set():
            time.sleep(0.05)

    def _accept(self):
        while True:
            try:
                c, _ = self.ls.accept()
            except OSError:
                return
            with self.lock:
                self.nconn += 1
                cid = self.nconn
            threading.Thread(target=self._conn, args=(c, cid), daemon=True).start()

    def _conn(self, c, cid):
        self._wait_thawed()
        try:
            u = socket.create_connection(self.dst)
        except OSError:
            c.close()
            return
        resp = _Stream(lambda h, b: self._on_resp(cid, h, b), False)
        req = _Stream(lambda h, b: self._on_req(cid, h, b, resp), True)
        threading.Thread(target=self._pipe, args=(c, u, req, cid, "c"), daemon=True).start()
        threading.Thread(target=self._pipe, args=(u, c, resp, cid, "s"), daemon=True).start()

    def _on_req(self, cid, head, body, resp):
        line, hd = head
        parts = line.split(" ")
        m, path = parts[0], parts[1] if len(parts) > 1 else "?"
        resp.reqs.append(m)
        extra = ""
        if hd.get("if-none-match"):
            extra += f" if-none-match={hd['if-none-match']}"
        if hd.get("if-match"):
            extra += f" if-match={hd['if-match']}"
        saved = ""
        if self.save_dir and m == "PUT" and any(p in path for p in self.save_parts) and body:
            with self.lock:
                self.nsaved += 1
                n = self.nsaved
            fn = os.path.join(self.save_dir, f"{n:07d}")
            with open(fn, "wb") as f:
                f.write(body)
            saved = f" saved={n:07d}"
        if m == "POST" and "delete" in path.split("?", 1)[-1]:
            keys = [k.split("</Key>")[0] for k in body.decode("latin1", "replace").split("<Key>")[1:]]
            extra += " keys=" + ",".join(keys)
        with self.lock:
            self.log.write(f"{time.time():.3f} REQ c{cid} {m} {path} len={len(body)}{extra}{saved}\n")

    def _on_resp(self, cid, head, body):
        line, hd = head
        code = line.split(" ")[1] if " " in line else "?"
        with self.lock:
            self.log.write(f"{time.time():.3f} RSP c{cid} {code} len={len(body)}\n")

    def _pipe(self, a, b, parser, cid, d):
        try:
            while True:
                x = a.recv(65536)
                if not x:
                    break
                try:
                    parser.feed(x)
                except Exception as e:  # a parse error must not change the forwarding
                    with self.lock:
                        self.log.write(f"{time.time():.3f} PARSEERR c{cid} {d} {e!r}\n")
                    parser.feed = lambda _x: None
                self._wait_thawed()
                b.sendall(x)
        except OSError:
            pass
        finally:
            with self.lock:
                self.log.write(f"{time.time():.3f} CLOSE c{cid} {d}\n")
            for s in (a, b):
                try:
                    s.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
