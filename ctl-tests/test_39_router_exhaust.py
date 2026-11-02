"""M14, driver senza bilanciatore: `--router` (hello come un mongos) e il hello in streaming (exhaust) dei driver.

1. `--router`: hello risponde `msg: "isdbgrid"`, senza setName né hosts; i comandi di replica non esistono (come su mongos);
   transazioni e scritture ritentabili restano permesse.
2. Exhaust: un OP_MSG hello con topologyVersion, maxAwaitTimeMS e il flag exhaustAllowed riceve una risposta dopo l'altra,
   ognuna con il flag moreToCome e responseTo uguale all'id della risposta prima, senza nuove richieste. Prima della
   correzione il server rispondeva una volta sola senza moreToCome, e il driver Node segnava il server come morto a ogni
   giro del monitor ("Server ended moreToCome unexpectedly"), svuotando il suo pool di connessioni. Vale in ogni modalità
   (anche standalone), quindi si prova anche senza `--router`.
3. Un driver con due camerieri `--router` nella lista dei semi vede una topologia Sharded e usa tutti e due.
"""
import socket
import struct
import time

import bson
from pymongo import MongoClient

from lib import Server, make_bucket

OP_MSG = 2013
MORE_TO_COME = 2
EXHAUST_ALLOWED = 1 << 16


def msg(request_id, doc, flags=0):
    body = struct.pack("<I", flags) + b"\x00" + bson.encode(doc)
    return struct.pack("<iiii", 16 + len(body), request_id, 0, OP_MSG) + body


def read_msg(sock):
    head = b""
    while len(head) < 16:
        chunk = sock.recv(16 - len(head))
        if not chunk:
            return None
        head += chunk
    length, rid, resp_to, op = struct.unpack("<iiii", head)
    rest = b""
    while len(rest) < length - 16:
        rest += sock.recv(length - 16 - len(rest))
    flags = struct.unpack("<I", rest[:4])[0]
    return {"id": rid, "to": resp_to, "op": op, "flags": flags, "doc": bson.decode(rest[5:])}


def test_router_hello_looks_like_mongos():
    srv = Server(make_bucket("rt39"), "rt39", extra=["--router"]).start()
    try:
        c = srv.client()
        h = c.admin.command("hello")
        assert h.get("msg") == "isdbgrid" and h.get("isWritablePrimary") is True
        assert "setName" not in h and "hosts" not in h
        try:
            c.admin.command("replSetGetStatus")
            assert False, "replSetGetStatus should not exist on a router"
        except Exception as e:
            assert getattr(e, "code", None) == 59
        # a retryable write (txnNumber) and a transaction are accepted, as through mongos
        r = MongoClient(f"mongodb://127.0.0.1:{srv.port}/", serverSelectionTimeoutMS=10000)
        r.prova.c.insert_one({"_id": 1})
        with r.start_session() as s:
            s.with_transaction(lambda ss: r.prova.c.insert_one({"_id": 2}, session=ss))
        assert r.prova.c.count_documents({}) == 2
    finally:
        srv.kill9()


def _exhaust(srv, rounds=3, await_ms=200):
    s = socket.create_connection(("127.0.0.1", srv.port), timeout=10)
    try:
        s.sendall(msg(7, {"hello": 1, "$db": "admin"}))
        first = read_msg(s)
        tv = first["doc"]["topologyVersion"]
        s.sendall(msg(8, {"hello": 1, "topologyVersion": tv, "maxAwaitTimeMS": await_ms, "$db": "admin"}, EXHAUST_ALLOWED))
        got, prev, t0 = [], 8, time.time()
        for _ in range(rounds):
            m = read_msg(s)
            assert m is not None, "connection closed during exhaust"
            assert m["doc"]["ok"] == 1.0
            assert m["flags"] & MORE_TO_COME, "reply without moreToCome: the driver ends its stream and marks the server Unknown"
            assert m["to"] == prev, f"responseTo {m['to']} should be {prev}"
            prev = m["id"]
            got.append(m)
        elapsed = time.time() - t0
        # each reply waits maxAwaitTimeMS (nothing changes in the topology)
        assert elapsed >= (rounds - 1) * await_ms / 1000 * 0.8, elapsed
        return got
    finally:
        s.close()


def test_exhaust_hello_streams_replies_router():
    srv = Server(make_bucket("ex39"), "ex39", extra=["--router"]).start()
    try:
        _exhaust(srv)
        # the server is still fine after the client hung up in the middle of the stream
        assert srv.client().admin.command("ping")["ok"] == 1.0
    finally:
        srv.kill9()


def test_exhaust_hello_streams_replies_standalone():
    srv = Server(make_bucket("ex39s"), "ex39s").start()
    try:
        _exhaust(srv)
    finally:
        srv.kill9()


def test_without_exhaust_flag_one_reply():
    """Without exhaustAllowed: one reply, no moreToCome (the polling protocol)."""
    srv = Server(make_bucket("ex39p"), "ex39p").start()
    try:
        s = socket.create_connection(("127.0.0.1", srv.port), timeout=10)
        s.sendall(msg(7, {"hello": 1, "$db": "admin"}))
        tv = read_msg(s)["doc"]["topologyVersion"]
        s.sendall(msg(8, {"hello": 1, "topologyVersion": tv, "maxAwaitTimeMS": 100, "$db": "admin"}))
        m = read_msg(s)
        assert m["to"] == 8 and not (m["flags"] & MORE_TO_COME)
        s.sendall(msg(9, {"ping": 1, "$db": "admin"}))
        assert read_msg(s)["to"] == 9
        s.close()
    finally:
        srv.kill9()


def test_driver_sees_sharded_and_uses_both():
    a = Server(make_bucket("dr39a"), "dr39a", extra=["--router"]).start()
    b = Server(make_bucket("dr39b"), "dr39b", extra=["--router"]).start()
    try:
        used = {}
        from pymongo import monitoring

        class L(monitoring.CommandListener):
            def started(self, e):
                pass

            def succeeded(self, e):
                if e.command_name == "find":
                    used[e.connection_id] = used.get(e.connection_id, 0) + 1

            def failed(self, e):
                pass
        c = MongoClient(f"mongodb://127.0.0.1:{a.port},127.0.0.1:{b.port}/", serverSelectionTimeoutMS=10000, event_listeners=[L()])
        for i in range(400):
            c.prova.c.find_one({"_id": i})
        assert c.topology_description.topology_type_name == "Sharded"
        assert len(used) == 2 and min(used.values()) > 50, used
    finally:
        a.kill9()
        b.kill9()
