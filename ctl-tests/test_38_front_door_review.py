"""The front door after its review (P3, PR #26): the corrections asked by the external reviewer.

* R1: when the process runs out of open files, `accept` fails; the server must log it (at most once a minute), wait a
  little and go on, not exit. Before the correction it exited with code 1 ("Too many open files") without closing its
  databases.
* The total limit of connections per process (--max-conns, like mongod's net.maxIncomingConnections): by default below
  the open-files limit read at start, with a margin; beyond it a new connection is closed at once, before TLS, whatever
  the address and whether or not its owner has a password (it also bounds R3: many databases x the per-database limit).
* R2: a connection that has not authenticated (with --auth) and sends nothing for --unauth-idle-timeout-secs is closed.
  A driver's monitoring connections send a hello at least every ~10 s and are not touched.

The servers here trust only 127.0.0.1/32 (the harness); 127.0.0.x are outside clients."""
import socket
import subprocess
import time

import pytest

from lib import BIN, Server, make_bucket, wait_until
from netlib import Wire

PW = "Front-door-review-SECRET-81d0"
HELLO = Wire.frame({"hello": 1, "$db": "admin"})


def supports(flag):
    out = subprocess.run([BIN, "--help"], capture_output=True, text=True).stdout
    return f"{flag} <" in out


def start(tag, extra=(), nofile=None, auth=False):
    bucket = make_bucket(tag)
    srv = Server(bucket, tag, idle=300, extra=(["--auth"] if auth else []) + ["--trusted-networks", "127.0.0.1/32"] + list(extra), nofile=nofile).start()
    if auth:
        c = srv.client()
        c.admin.command("createUser", "root", pwd="rootpw", roles=[{"role": "root", "db": "admin"}])
        c.close()
    return srv


def add_db(srv, db, user="u1", pwd=PW):
    assert srv.admin.post("/v1/databases", {"name": db})[0] == 201
    r = srv.client(username="root", password="rootpw", authSource="admin")
    r[db].command("createUser", user, pwd=pwd, roles=[{"role": "readWrite", "db": db}])
    r.close()


def network(srv):
    st, r = srv.admin.get("/v1/network")
    assert st == 200, r
    return r


def raw_open(n, srcs, port):
    """n sockets spread over the source addresses, each sends one hello and nothing more (no reply is read)."""
    out = []
    for i in range(n):
        s = socket.socket()
        s.settimeout(5)
        s.bind((srcs[i % len(srcs)], 0))
        try:
            s.connect(("127.0.0.1", port))
            s.sendall(HELLO)
        except OSError:
            pass
        out.append(s)
    return out


def close_all(socks):
    for s in socks:
        try:
            s.close()
        except OSError:
            pass


def answers(port, src="127.0.0.1"):
    try:
        w = Wire(port, src, timeout=5)
        ok = w.cmd({"hello": 1, "$db": "admin"}).get("ok") == 1
        w.close()
        return ok
    except (ConnectionError, OSError):
        return False


def test_out_of_open_files_does_not_stop_the_server():
    """R1 of the review: 300 open files, about 400 connections without a password from 3 addresses (each far below the
    4000 per address). The total limit is off here, so the open files do run out."""
    extra = ["--max-conns", "0"] if supports("--max-conns") else []
    s = start("fdr-emfile", extra, nofile=300)
    try:
        socks = raw_open(400, ["127.0.0.81", "127.0.0.82", "127.0.0.83"], s.port)
        time.sleep(3)
        assert s.proc.poll() is None, f"server exited with {s.proc.returncode}: " + s.log_text()[-600:]
        assert "accept failed" in s.log_text()                     # it did run out, and said so
        close_all(socks)
        # the connections close: it serves again, from an outside address and from the harness
        wait_until(lambda: answers(s.port, "127.0.0.84"), timeout=20, what="server answers again")
        assert answers(s.port)
        assert network(s)["totals"]["accept_errors"] >= 1
        # one log line per minute at most, not one per failed accept
        assert s.log_text().count("accept failed") == 1, s.log_text().count("accept failed")
    finally:
        s.term()
    assert "shutting down: closing tenants cleanly" in s.log_text()


def test_total_limit_default_is_below_the_open_files():
    """600 open files: the default total is half of them (300; with 4000 or more it is the limit minus 1000). 400
    connections from 3 addresses: 300 are served, the others closed at once, and the open files never run out."""
    s = start("fdr-total", nofile=600)
    try:
        cfg = network(s)["config"]
        assert cfg["max_conns"] == 300, cfg
        socks = raw_open(400, ["127.0.0.91", "127.0.0.92", "127.0.0.93"], s.port)
        time.sleep(2)
        net = network(s)
        assert net["open_connections"] == 300, net["open_connections"]
        assert net["totals"]["rejected_total_limit"] >= 100, net["totals"]
        assert not answers(s.port, "127.0.0.94")                   # full, from a new address too
        assert answers(s.port)                                     # the harness (trusted) is counted but never refused
        assert s.proc.poll() is None
        assert "accept failed" not in s.log_text()
        assert "netguard: connection from 127.0.0.9" in s.log_text() and "connections open in this process" in s.log_text()
        close_all(socks)
        wait_until(lambda: network(s)["open_connections"] == 0, timeout=20, what="all closed")
        assert answers(s.port, "127.0.0.94")
    finally:
        s.term()


def test_total_limit_bounds_authenticated_connections_from_one_address():
    """R3 of the review: with a password, the per-database limit times the number of databases is the only bound from one
    address. The total limit closes it: 2 databases with 40 each, but 30 in total."""
    s = start("fdr-r3", ["--max-conns", "30", "--max-conns-per-db", "40"], auth=True)
    try:
        add_db(s, "r3a")
        add_db(s, "r3b")
        # the harness's own connections (trusted, but counted) are gone first
        wait_until(lambda: network(s)["open_connections"] == 0, timeout=20, what="harness connections closed")
        held = []
        for i in range(30):
            w = Wire(s.port, "127.0.0.100")
            r1, r2 = w.scram(f"r3{'ab'[i % 2]}", "u1", PW)
            assert r2 is not None and r2["ok"] == 1, (i, r2)
            held.append(w)
        assert not answers(s.port, "127.0.0.100")
        net = network(s)
        assert net["config"]["max_conns"] == 30 and net["open_connections"] == 30, net
        assert net["totals"]["rejected_total_limit"] >= 1
        held.pop().close()
        wait_until(lambda: network(s)["open_connections"] == 29, timeout=10, what="one closed")
        assert answers(s.port, "127.0.0.100")
        for w in held:
            w.close()
    finally:
        s.term()


def test_silent_unauthenticated_connection_is_closed():
    """R2 of the review: a connection without a password sends one hello, then nothing. With a handshake timeout of 2 s it
    stayed open for ever; now it is closed after --unauth-idle-timeout-secs (5 here). An authenticated connection silent
    for as long is not touched."""
    s = start("fdr-idle", ["--handshake-timeout-secs", "2", "--unauth-idle-timeout-secs", "5"], auth=True)
    try:
        add_db(s, "idl")
        quiet = Wire(s.port, "127.0.0.110")
        assert quiet.cmd({"hello": 1, "$db": "admin"})["ok"] == 1
        authed = Wire(s.port, "127.0.0.111")
        assert authed.scram("idl", "u1", PW)[1]["ok"] == 1
        # a connection that talks every 3 s stays open
        talker = Wire(s.port, "127.0.0.112")
        for _ in range(3):
            assert talker.cmd({"hello": 1, "$db": "admin"})["ok"] == 1
            time.sleep(3)
        time.sleep(0.5)
        with pytest.raises((ConnectionError, OSError)):
            quiet.cmd({"hello": 1, "$db": "admin"})
        assert authed.cmd({"find": "c", "filter": {}, "$db": "idl"})["ok"] == 1
        assert talker.cmd({"hello": 1, "$db": "admin"})["ok"] == 1
        net = network(s)
        assert net["totals"]["idle_unauthenticated_closed"] >= 1, net["totals"]
        assert net["config"]["unauth_idle_timeout_secs"] == 5
        assert "closed: not authenticated and silent for 5 s" in s.log_text()
        for w in (quiet, authed, talker):
            w.close()
    finally:
        s.term()


def test_idle_driver_keeps_working():
    """A pymongo client that does nothing for 40 s (8 times the silence timeout of 5 s): its monitoring connections send a
    hello every 10 s by default... which is more than 5 s, so here the timeout is 15 s (the default is 120 s; a 3-minute run
    with the default is in the report). The client works afterwards and no connection was closed for silence."""
    s = start("fdr-drv", ["--unauth-idle-timeout-secs", "15"], auth=True)
    try:
        add_db(s, "drv")
        from pymongo import MongoClient
        c = MongoClient(f"mongodb://u1:{PW}@127.0.0.1:{s.port}/drv?directConnection=true", serverSelectionTimeoutMS=5000)
        c.drv.c.insert_one({"a": 1})
        time.sleep(40)
        assert c.drv.c.find_one({"a": 1})["a"] == 1
        c.close()
        assert network(s)["totals"]["idle_unauthenticated_closed"] == 0
    finally:
        s.term()
