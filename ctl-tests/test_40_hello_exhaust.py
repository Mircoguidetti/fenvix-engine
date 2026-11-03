"""C6 (from M14): the awaitable `hello` of the drivers' monitoring connection, streamed (exhaust), as mongod does.

A driver's monitor sends one OP_MSG `hello` with `topologyVersion`, `maxAwaitTimeMS` and the flag `exhaustAllowed`, then
expects a reply every `maxAwaitTimeMS`, each flagged `moreToCome` and answering the previous reply (`responseTo` = the id of
the reply before), with no new request. Before the fix the server answered once without `moreToCome`: the Node driver
(5.x+ with streaming on by default) then logged "Server ended moreToCome unexpectedly", marked the server Unknown and cleared
its connection pool at every monitor round (every ~10 s), for every Node client of today's engine.

1. exhaust streams replies (standalone and `--replica-set` mode);
2. without the `exhaustAllowed` flag: one reply, no `moreToCome` (the polling protocol), the connection stays usable;
3. a `topologyVersion` of another process (a restart) is answered at once, once, then the stream waits again;
4. the official Node driver (mongo-tests/node, `npm ci` there): no failed heartbeat and no pool clear. Skipped if node or the
   driver is missing.
"""
import json
import os
import shutil
import socket
import struct
import subprocess
import time

import bson
from bson import ObjectId

from lib import ROOT, Server, make_bucket

OP_MSG = 2013
MORE_TO_COME = 2
EXHAUST_ALLOWED = 1 << 16
NODE_DIR = os.path.join(ROOT, "mongo-tests", "node")


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
        chunk = sock.recv(length - 16 - len(rest))
        if not chunk:
            return None
        rest += chunk
    flags = struct.unpack("<I", rest[:4])[0]
    return {"id": rid, "to": resp_to, "op": op, "flags": flags, "doc": bson.decode(rest[5:])}


def _exhaust(srv, rounds=4, await_ms=200, tv=None):
    s = socket.create_connection(("127.0.0.1", srv.port), timeout=10)
    try:
        s.sendall(msg(7, {"hello": 1, "$db": "admin"}))
        first = read_msg(s)
        assert first["to"] == 7 and not (first["flags"] & MORE_TO_COME)
        tv = tv or first["doc"]["topologyVersion"]
        s.sendall(msg(8, {"hello": 1, "topologyVersion": tv, "maxAwaitTimeMS": await_ms, "$db": "admin"}, EXHAUST_ALLOWED))
        got, prev, times = [], 8, []
        t0 = time.time()
        for _ in range(rounds):
            m = read_msg(s)
            assert m is not None, "connection closed during exhaust"
            assert m["doc"]["ok"] == 1.0
            assert m["flags"] & MORE_TO_COME, "reply without moreToCome: the driver ends its stream and marks the server Unknown"
            assert m["to"] == prev, f"responseTo {m['to']} should be {prev}"
            prev = m["id"]
            times.append(time.time() - t0)
            got.append(m)
        return got, times
    finally:
        s.close()


def test_exhaust_streams_standalone():
    srv = Server(make_bucket("ex40s"), "ex40s").start()
    try:
        _, times = _exhaust(srv)
        # each reply waits maxAwaitTimeMS (nothing changes in the topology)
        assert times[-1] >= 3 * 0.2 * 0.8, times
        # the server is still fine after the client hung up in the middle of the stream
        assert srv.client().admin.command("ping")["ok"] == 1.0
    finally:
        srv.kill9()


def test_exhaust_streams_replica_set_mode():
    srv = Server(make_bucket("ex40r"), "ex40r", extra=["--replica-set", "rs0"]).start()
    try:
        got, _ = _exhaust(srv, rounds=3)
        assert all(m["doc"].get("setName") == "rs0" for m in got)
    finally:
        srv.kill9()


def test_without_exhaust_flag_one_reply():
    srv = Server(make_bucket("ex40p"), "ex40p").start()
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


def test_stale_process_id_answered_at_once_then_waits():
    """A topologyVersion from another process (the driver knew this address before a restart): the first reply comes at
    once (the driver's view is stale), carrying this process's topologyVersion; the next ones wait maxAwaitTimeMS again
    (no busy loop)."""
    srv = Server(make_bucket("ex40t"), "ex40t").start()
    try:
        stale = {"processId": ObjectId(), "counter": 0}
        got, times = _exhaust(srv, rounds=3, await_ms=500, tv=stale)
        assert times[0] < 0.3, times
        assert got[0]["doc"]["topologyVersion"]["processId"] != stale["processId"]
        assert times[2] - times[0] >= 2 * 0.5 * 0.8, times
    finally:
        srv.kill9()


NODE_MONITOR = r"""
const { MongoClient } = require(process.argv[1]);
(async () => {
  const c = new MongoClient(process.argv[2], { heartbeatFrequencyMS: 500, serverSelectionTimeoutMS: 10000 });
  let ok = 0, failed = 0, cleared = 0, awaited = 0, errs = [];
  c.on('serverHeartbeatSucceeded', e => { ok++; if (e.awaited) awaited++; });
  c.on('serverHeartbeatFailed', e => { failed++; errs.push(String(e.failure && e.failure.message)); });
  c.on('connectionPoolCleared', () => { cleared++; });
  await c.connect();
  const end = Date.now() + Number(process.argv[3]);
  let pings = 0;
  while (Date.now() < end) { await c.db('admin').command({ ping: 1 }); pings++; await new Promise(r => setTimeout(r, 100)); }
  await c.close();
  console.log(JSON.stringify({ ok, failed, cleared, awaited, pings, errs: errs.slice(0, 3) }));
})().catch(e => { console.log(JSON.stringify({ fatal: String(e) })); process.exit(1); });
"""


def _node_driver():
    node = shutil.which("node")
    mod = os.path.join(NODE_DIR, "node_modules", "mongodb")
    if not node or not os.path.isdir(mod):
        import pytest
        pytest.skip("node or the Node driver missing (run `npm ci` in mongo-tests/node)")
    return node, mod


def test_node_driver_monitor_never_fails():
    node, mod = _node_driver()
    srv = Server(make_bucket("ex40n"), "ex40n").start()
    try:
        out = subprocess.run([node, "-e", NODE_MONITOR, mod, f"mongodb://127.0.0.1:{srv.port}/?directConnection=true", "8000"],
                             capture_output=True, text=True, timeout=60)
        assert out.stdout.strip(), out.stderr
        res = json.loads(out.stdout.strip().splitlines()[-1])
        print("node monitor:", res)
        assert "fatal" not in res, res
        # streaming on: heartbeats arrive as awaited replies; none fails and the pool is never cleared
        assert res["failed"] == 0 and res["cleared"] == 0, res
        assert res["awaited"] >= 5, res
    finally:
        srv.kill9()
