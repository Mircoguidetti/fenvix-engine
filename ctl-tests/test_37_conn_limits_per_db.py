"""Connection limits for serverless platforms (P3, "the front door"), on a server with --auth.

The functions of a platform such as Vercel or Lambda reach the engine from a few shared NAT addresses, so the limits are:
* --max-unauth-conns-per-ip: connections from one address that have NOT authenticated (default 1000). A connection stops
  counting when it authenticates and counts again when it logs out of every database;
* --max-conns-per-db: authenticated connections on one database, from any address (default 5000); an authentication beyond it
  is refused with a clear error (code 13), is not counted as a failed authentication, and the connection stays open;
* --max-conns-per-ip: all connections from one address, now off by default (0).
The servers here trust only 127.0.0.1/32 (the harness); 127.0.0.x are outside clients."""
import time

import pytest

from lib import Server, make_bucket
from netlib import Wire

PW = "Front-door-SECRET-5c2a"


def start(tag, extra=()):
    bucket = make_bucket(tag)
    srv = Server(bucket, tag, idle=300, extra=["--auth", "--trusted-networks", "127.0.0.1/32"] + list(extra)).start()
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


def authed(src, port, db, user="u1", pwd=PW):
    """A connection from `src` authenticated on `db` (asserts the authentication worked)."""
    w = Wire(port, src)
    r1, r2 = w.scram(db, user, pwd)
    assert r2 is not None and r2.get("ok") == 1 and r2.get("done"), (r1, r2)
    return w


def refused_at_open(port, src):
    w = Wire(port, src)
    try:
        w.cmd({"hello": 1, "$db": "admin"})
        return False
    except (ConnectionError, OSError):
        return True
    finally:
        w.close()


def test_many_authenticated_connections_from_one_address():
    """300 authenticated connections from one address on 3 databases, with only 5 unauthenticated allowed per address and no
    total per address: the old limit (200 per address, everything counted) would have refused the 201st."""
    s = start("fd-nat", ["--max-unauth-conns-per-ip", "5"])
    try:
        for db in ("nat1", "nat2", "nat3"):
            add_db(s, db)
        held = [authed("127.0.0.40", s.port, f"nat{1 + i % 3}") for i in range(300)]
        assert held[0].cmd({"find": "c", "filter": {}, "$db": "nat1"})["ok"] == 1          # held[0] is on nat1
        net = network(s)
        assert {"ip": "127.0.0.40", "open": 300, "unauthenticated": 0} in net["connections"], net["connections"]
        per_db = {d["database"]: d["authenticated"] for d in net["databases"]}
        assert per_db == {"nat1": 100, "nat2": 100, "nat3": 100}, per_db
        # 5 connections that never authenticate are fine; the 6th is closed at once
        idle = [Wire(s.port, "127.0.0.40") for _ in range(5)]
        for w in idle:
            assert w.cmd({"hello": 1, "$db": "admin"})["ok"] == 1
        assert refused_at_open(s.port, "127.0.0.40")
        assert not refused_at_open(s.port, "127.0.0.41")                    # another address: its own count
        net = network(s)
        assert net["totals"]["rejected_unauthenticated"] >= 1, net["totals"]
        assert net["config"]["max_conns_per_ip"] == 0
        # one of the idle ones authenticates: its place is free again
        r1, r2 = idle[0].scram("nat1", "u1", PW)
        assert r2["ok"] == 1
        extra = Wire(s.port, "127.0.0.40")
        assert extra.cmd({"hello": 1, "$db": "admin"})["ok"] == 1
        for w in held + idle + [extra]:
            w.close()
    finally:
        s.term()


def test_per_database_limit():
    s = start("fd-db", ["--max-conns-per-db", "10", "--auth-fail-max-per-db", "3", "--auth-fail-max", "5"])
    try:
        add_db(s, "full")
        add_db(s, "free")
        held = [authed(f"127.0.0.{50 + i % 3}", s.port, "full") for i in range(10)]
        # the 11th, from any address, is refused with a clear error, several times: never a block
        for _ in range(6):
            w = Wire(s.port, "127.0.0.55")
            r1, r2 = w.scram("full", "u1", PW)
            assert r2 is not None and r2["ok"] == 0 and r2["code"] == 13, r2
            assert "too many connections authenticated on database full" in r2["errmsg"], r2
            # the connection is still open and not authenticated: another database works on it
            r1, r2b = w.scram("free", "u1", PW)
            assert r2b["ok"] == 1, r2b
            w.close()
        net = network(s)
        assert net["totals"]["refused_db_full"] == 6, net["totals"]
        assert net["totals"]["auth_failures"] == 0 and net["totals"]["blocks"] == 0, net["totals"]
        assert net["blocked"] == [], net["blocked"]
        assert {"database": "full", "authenticated": 10} in net["databases"], net["databases"]
        # one closes: one more fits
        held.pop().close()
        time.sleep(0.3)
        held.append(authed("127.0.0.56", s.port, "full"))
        w = Wire(s.port, "127.0.0.57")
        assert w.scram("full", "u1", PW)[1]["code"] == 13
        w.close()
        # the harness (trusted) is counted but never refused
        t = authed("127.0.0.1", s.port, "full")
        assert {"database": "full", "authenticated": 11} in network(s)["databases"]
        for x in held + [t]:
            x.close()
        time.sleep(0.3)
        assert all(d["database"] != "full" for d in network(s)["databases"]), network(s)["databases"]
    finally:
        s.term()


def test_logout_counts_again_as_unauthenticated():
    s = start("fd-out", ["--max-unauth-conns-per-ip", "2"])
    try:
        add_db(s, "lo")
        a = authed("127.0.0.60", s.port, "lo")
        b = Wire(s.port, "127.0.0.60")
        c = Wire(s.port, "127.0.0.60")
        assert b.cmd({"hello": 1, "$db": "admin"})["ok"] == 1 and c.cmd({"hello": 1, "$db": "admin"})["ok"] == 1
        assert refused_at_open(s.port, "127.0.0.60")                          # 2 unauthenticated: full
        assert a.cmd({"logout": 1, "$db": "lo"})["ok"] == 1                   # a counts again: 3 of 2
        b.close()
        time.sleep(0.3)
        assert refused_at_open(s.port, "127.0.0.60")                          # still 2 (a and c)
        conns = network(s)["connections"]
        assert {"ip": "127.0.0.60", "open": 2, "unauthenticated": 2} in conns, conns
        a.close()
        c.close()
    finally:
        s.term()


def test_defaults():
    s = start("fd-def")
    try:
        cfg = network(s)["config"]
        assert (cfg["max_conns_per_ip"], cfg["max_unauth_conns_per_ip"], cfg["max_conns_per_db"]) == (0, 1000, 5000), cfg
        held = [Wire(s.port, "127.0.0.70") for _ in range(1000)]
        held[-1].cmd({"hello": 1, "$db": "admin"})
        assert refused_at_open(s.port, "127.0.0.70")
        for w in held:
            w.close()
    finally:
        s.term()
