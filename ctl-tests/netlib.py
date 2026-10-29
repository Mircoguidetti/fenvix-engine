"""Helpers of the network-limits tests: connections that come from a chosen loopback address (127.0.0.x: Linux routes the whole
127.0.0.0/8 to the loopback interface, so a socket bound to 127.0.0.5 reaches a server on 127.0.0.1 and the server sees
127.0.0.5 as the TCP peer), a minimal OP_MSG client with SCRAM-SHA-256 that shows the raw replies, a TCP forwarder that lets
pymongo connect from such an address, and HTTP calls to the admin port from such an address.

The sandbox kernel has no IPv6 at all (not even ::1), so these tests are IPv4 only; IPv6 matching is covered by the unit tests
of the engine (mongo-server/src/netguard.rs)."""
import base64
import hashlib
import hmac
import http.client
import json
import os
import socket
import struct
import threading

import bson
from bson.binary import Binary

from lib import TOKEN

OP_MSG = 2013


class Wire:
    """One raw connection from `src` to the data port."""

    def __init__(self, port, src="127.0.0.1", timeout=10):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=timeout, source_address=(src, 0))
        self.req = 0

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass

    def _recv_exact(self, n):
        out = b""
        while len(out) < n:
            chunk = self.sock.recv(n - len(out))
            if not chunk:
                raise ConnectionError("connection closed by the server")
            out += chunk
        return out

    def send_raw(self, data):
        self.sock.sendall(data)

    @staticmethod
    def frame(doc):
        body = struct.pack("<I", 0) + b"\x00" + bson.encode(doc)
        return struct.pack("<iiii", 16 + len(body), 1, 0, OP_MSG) + body

    def cmd(self, doc):
        self.req += 1
        body = struct.pack("<I", 0) + b"\x00" + bson.encode(doc)
        self.sock.sendall(struct.pack("<iiii", 16 + len(body), self.req, 0, OP_MSG) + body)
        hdr = self._recv_exact(16)
        ln, _, _, op = struct.unpack("<iiii", hdr)
        data = self._recv_exact(ln - 16)
        assert op == OP_MSG, op
        return bson.decode(data[5:])

    def scram(self, db, user, pwd, nonce=None):
        """A SCRAM-SHA-256 conversation (skipEmptyExchange). Returns (saslStart reply, saslContinue reply or None)."""
        nonce = nonce or base64.b64encode(os.urandom(24)).decode()
        bare = f"n={user},r={nonce}"
        r1 = self.cmd({"saslStart": 1, "mechanism": "SCRAM-SHA-256", "payload": Binary(("n,," + bare).encode()), "autoAuthorize": 1,
                       "options": {"skipEmptyExchange": True}, "$db": db})
        if not r1.get("ok"):
            return r1, None
        sf = bytes(r1["payload"]).decode()
        parts = dict(kv.split("=", 1) for kv in sf.split(","))
        salted = hashlib.pbkdf2_hmac("sha256", pwd.encode(), base64.b64decode(parts["s"]), int(parts["i"]))
        ck = hmac.new(salted, b"Client Key", hashlib.sha256).digest()
        sk = hashlib.sha256(ck).digest()
        without = f"c=biws,r={parts['r']}"
        sig = hmac.new(sk, f"{bare},{sf},{without}".encode(), hashlib.sha256).digest()
        proof = bytes(a ^ b for a, b in zip(ck, sig))
        r2 = self.cmd({"saslContinue": 1, "conversationId": r1["conversationId"], "payload": Binary(f"{without},p={base64.b64encode(proof).decode()}".encode()), "$db": db})
        return r1, r2

    def closed_within(self, secs):
        """True if the server closes the connection within `secs` (recv returns EOF or a reset)."""
        self.sock.settimeout(secs)
        try:
            return self.sock.recv(1) == b""
        except socket.timeout:
            return False
        except ConnectionResetError:
            return True


def scram_ok(port, src, db, user, pwd):
    w = Wire(port, src)
    try:
        r1, r2 = w.scram(db, user, pwd)
        return bool(r2 and r2.get("ok") == 1 and r2.get("done"))
    finally:
        w.close()


def shape_of_start(r1):
    """What a client can observe of a saslStart reply, without the random parts (nonce) and the user's salt."""
    if not r1.get("ok"):
        return dict(r1)
    sf = bytes(r1["payload"]).decode()
    keys = [kv.split("=", 1)[0] for kv in sf.split(",")]
    return {"keys": sorted(r1.keys()), "done": r1["done"], "payload_keys": keys, "iterations": dict(kv.split("=", 1) for kv in sf.split(","))["i"]}


class Forward:
    """A TCP forwarder on 127.0.0.1:<port> whose outgoing connections to the server come from `src`: pymongo connects to the
    forwarder and the server sees `src` as the peer."""

    def __init__(self, dst_port, src):
        self.dst_port, self.src = dst_port, src
        self.ls = socket.socket()
        self.ls.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.ls.bind(("127.0.0.1", 0))
        self.ls.listen(64)
        self.port = self.ls.getsockname()[1]
        self.stop = False
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self):
        while not self.stop:
            try:
                c, _ = self.ls.accept()
            except OSError:
                return
            try:
                s = socket.create_connection(("127.0.0.1", self.dst_port), timeout=10, source_address=(self.src, 0))
                s.settimeout(None)
            except OSError:
                c.close()
                continue
            for a, b in ((c, s), (s, c)):
                threading.Thread(target=self._pipe, args=(a, b), daemon=True).start()

    @staticmethod
    def _pipe(a, b):
        try:
            while True:
                d = a.recv(65536)
                if not d:
                    break
                b.sendall(d)
        except OSError:
            pass
        finally:
            for x in (a, b):
                try:
                    x.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

    def close(self):
        self.stop = True
        self.ls.close()

    def client(self, **kw):
        from pymongo import MongoClient
        return MongoClient(f"mongodb://127.0.0.1:{self.port}/?directConnection=true", serverSelectionTimeoutMS=10000, connectTimeoutMS=5000, **kw)


def admin_from(admin_port, src, method, path, body=None, token=TOKEN):
    """An admin-port call that comes from `src`."""
    c = http.client.HTTPConnection("127.0.0.1", admin_port, timeout=30, source_address=(src, 0))
    h = {"Authorization": f"Bearer {token}"} if token else {}
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        h["Content-Type"] = "application/json"
    c.request(method, path, body=data, headers=h)
    r = c.getresponse()
    payload = r.read()
    c.close()
    return r.status, (json.loads(payload) if payload else None)
