"""Limits on authentication failures and on connections (B1-B4 of the network-limits work), on a server with --auth.

* after N failed authentications from one address within T seconds the address is blocked for D seconds (doubling on each new
  block, up to a maximum); failures are also counted per (address, database), with their own threshold, and that block only
  covers the database; a blocked address gets the generic authentication error and its password is not checked;
* at most --max-conns-per-ip connections at once from one address;
* --handshake-timeout-secs: TLS handshake and first message within the time, and every message before authentication must arrive
  whole within it once it started (slow senders are closed); --pre-auth-max-message-bytes caps messages before authentication;
* GET /v1/network shows the state (no user names, no passwords); DELETE /v1/network/blocks/{ip} lifts a block;
* trusted networks (--trusted-networks) are never blocked or limited.
The servers here trust only 127.0.0.1/32 (the harness); 127.0.0.x are outside clients."""
import json
import socket
import struct
import time

import pytest
from pymongo.errors import OperationFailure

from lib import Server, make_bucket, wait_until
from netlib import Wire, Forward, scram_ok

PW = "Limit-SECRET-2e8d4f6a"
WRONG = "Wrong-SECRET-9a7b3c1d"


def start(tag, extra=(), trusted="127.0.0.1/32"):
    bucket = make_bucket(tag)
    srv = Server(bucket, tag, idle=300, extra=["--auth", "--trusted-networks", trusted] + list(extra)).start()
    c = srv.client()
    c.admin.command("createUser", "root", pwd="rootpw", roles=[{"role": "root", "db": "admin"}])
    c.close()
    return srv


def add_db(srv, db, user="u1", pwd=PW):
    assert srv.admin.post("/v1/databases", {"name": db})[0] == 201
    r = srv.client(username="root", password="rootpw", authSource="admin")
    r[db].command("createUser", user, pwd=pwd, roles=[{"role": "readWrite", "db": db}])
    r.close()


def attempt(srv, src, db, pwd, user="u1"):
    """(saslStart reply, saslContinue reply) of one attempt from `src`."""
    w = Wire(srv.port, src)
    try:
        return w.scram(db, user, pwd)
    finally:
        w.close()


def last_error(r):
    r1, r2 = r
    return r2 if r2 is not None else r1


GENERIC = {"ok": 0.0, "errmsg": "Authentication failed.", "code": 18, "codeName": "AuthenticationFailed"}


def network(srv):
    st, r = srv.admin.get("/v1/network")
    assert st == 200, r
    return r


def blocked(srv, ip, db=None):
    return [b for b in network(srv)["blocked"] if b["ip"] == ip and b["database"] == db]


@pytest.fixture(scope="module")
def srv():
    s = start("lim", ["--auth-fail-max", "10", "--auth-fail-window-secs", "60", "--auth-block-secs", "300", "--auth-fail-max-per-db", "1000"])
    for db in ("bf", "bf2", "bf3"):
        add_db(s, db)
    yield s
    s.term()


def test_brute_force_is_blocked(srv):
    src = "127.0.0.4"
    for i in range(10):
        assert last_error(attempt(srv, src, "bf", WRONG + str(i))) == GENERIC
    # the 11th attempt, with the right password: the same generic error (the password is not checked)
    r = attempt(srv, src, "bf", PW)
    assert last_error(r) == GENERIC, r
    assert not scram_ok(srv.port, src, "bf", "u1", PW)
    # on every database, through pymongo too
    assert not scram_ok(srv.port, src, "bf2", "u1", PW)
    f = Forward(srv.port, src)
    c = f.client(username="u1", password=PW, authSource="bf")
    try:
        with pytest.raises(OperationFailure) as e:
            c.bf.command("ping")
        assert e.value.code == 18 and e.value.details["errmsg"] == "Authentication failed."
    finally:
        c.close()
        f.close()
    # another address is not affected
    assert scram_ok(srv.port, "127.0.0.5", "bf", "u1", PW)
    b = blocked(srv, src)
    assert len(b) == 1 and 290 <= b[0]["remaining_secs"] <= 300 and b[0]["strike"] == 1, b
    # the state shows no user name and no password, and the attempts while blocked were counted, not checked
    net = network(srv)
    text = json.dumps(net)
    for secret in (PW, WRONG, "u1", "rootpw"):
        assert secret not in text, secret
    assert net["totals"]["refused_while_blocked"] >= 4, net["totals"]


def test_unblock_from_the_admin_port(srv):
    src = "127.0.0.40"
    for i in range(10):
        attempt(srv, src, "bf", WRONG)
    assert not scram_ok(srv.port, src, "bf", "u1", PW)
    st, r = srv.admin.delete(f"/v1/network/blocks/{src}")
    assert st == 200 and r["unblocked"] is True, r
    assert scram_ok(srv.port, src, "bf", "u1", PW)
    assert srv.admin.delete("/v1/network/blocks/not-an-ip")[0] == 400
    assert srv.admin.call("DELETE", f"/v1/network/blocks/{src}", token=None)[0] == 401
    assert srv.admin.call("GET", "/v1/network", token=None)[0] == 401


def test_trusted_address_is_never_blocked(srv):
    for i in range(30):
        attempt(srv, "127.0.0.1", "bf", WRONG)
    assert scram_ok(srv.port, "127.0.0.1", "bf", "u1", PW)
    assert blocked(srv, "127.0.0.1") == []


def test_block_expires_and_grows():
    s = start("limexp", ["--auth-fail-max", "3", "--auth-fail-window-secs", "60", "--auth-block-secs", "2", "--auth-block-max-secs", "5"])
    try:
        add_db(s, "e")
        src = "127.0.0.6"
        for _ in range(3):
            attempt(s, src, "e", WRONG)
        assert not scram_ok(s.port, src, "e", "u1", PW)
        time.sleep(2.5)
        assert scram_ok(s.port, src, "e", "u1", PW)                 # the first block (2 s) has expired
        for _ in range(3):
            attempt(s, src, "e", WRONG)
        b = blocked(s, src)
        assert b and b[0]["strike"] == 2 and 3 <= b[0]["remaining_secs"] <= 4, b       # second block: 4 s
        time.sleep(2.5)
        assert not scram_ok(s.port, src, "e", "u1", PW)             # still blocked after 2.5 s
        time.sleep(2)
        assert scram_ok(s.port, src, "e", "u1", PW)
        for _ in range(3):
            attempt(s, src, "e", WRONG)
        b = blocked(s, src)
        assert b and b[0]["strike"] == 3 and b[0]["remaining_secs"] <= 5, b             # 8 s capped at 5 s
        # failures outside the window do not add up
        s2 = start("limwin", ["--auth-fail-max", "3", "--auth-fail-window-secs", "2", "--auth-block-secs", "60"])
        try:
            add_db(s2, "w")
            for _ in range(2):
                attempt(s2, "127.0.0.7", "w", WRONG)
            time.sleep(2.5)
            for _ in range(2):
                attempt(s2, "127.0.0.7", "w", WRONG)
            assert scram_ok(s2.port, "127.0.0.7", "w", "u1", PW)
        finally:
            s2.term()
    finally:
        s.term()


def test_counters_per_address_and_per_database():
    # per (address, database): 3 failures block that pair only
    s = start("limpair", ["--auth-fail-max", "100", "--auth-fail-max-per-db", "3", "--auth-block-secs", "300"])
    try:
        for db in ("p1", "p2"):
            add_db(s, db)
        for _ in range(3):
            attempt(s, "127.0.0.8", "p1", WRONG)
        assert not scram_ok(s.port, "127.0.0.8", "p1", "u1", PW)
        assert last_error(attempt(s, "127.0.0.8", "p1", PW)) == GENERIC
        assert scram_ok(s.port, "127.0.0.8", "p2", "u1", PW)       # same address, other database
        assert scram_ok(s.port, "127.0.0.9", "p1", "u1", PW)       # other address, same database
        assert blocked(s, "127.0.0.8", "p1") and not blocked(s, "127.0.0.8")
    finally:
        s.term()
    # per address, across databases: 2 + 2 failures on two databases block the address everywhere
    s = start("limip", ["--auth-fail-max", "4", "--auth-fail-max-per-db", "100", "--auth-block-secs", "300"])
    try:
        for db in ("q1", "q2", "q3"):
            add_db(s, db)
        for db in ("q1", "q1", "q2", "q2"):
            attempt(s, "127.0.0.10", db, WRONG)
        assert not scram_ok(s.port, "127.0.0.10", "q3", "u1", PW)
        assert scram_ok(s.port, "127.0.0.11", "q3", "u1", PW)
        # unknown users count too (password spraying over user names)
        for i in range(4):
            attempt(s, "127.0.0.12", "q1", WRONG, user=f"nobody{i}")
        assert not scram_ok(s.port, "127.0.0.12", "q1", "u1", PW)
    finally:
        s.term()


def test_connection_limit_per_address():
    s = start("limconn", ["--max-conns-per-ip", "5"])
    try:
        held = [Wire(s.port, "127.0.0.13") for _ in range(5)]
        for w in held:
            assert w.cmd({"hello": 1, "$db": "admin"})["ok"] == 1
        extra = Wire(s.port, "127.0.0.13")
        with pytest.raises((ConnectionError, OSError)):
            extra.cmd({"hello": 1, "$db": "admin"})
        extra.close()
        other = Wire(s.port, "127.0.0.14")                          # other address: fine
        assert other.cmd({"hello": 1, "$db": "admin"})["ok"] == 1
        other.close()
        held.pop().close()
        time.sleep(0.3)
        again = Wire(s.port, "127.0.0.13")                          # one slot free again
        assert again.cmd({"hello": 1, "$db": "admin"})["ok"] == 1
        trusted = [Wire(s.port, "127.0.0.1") for _ in range(12)]    # trusted: no limit
        for w in trusted:
            assert w.cmd({"hello": 1, "$db": "admin"})["ok"] == 1
        net = network(s)
        assert net["totals"]["rejected_connections"] >= 1, net["totals"]
        assert {"ip": "127.0.0.13", "open": 5} in net["connections"], net["connections"]
        for w in held + [again] + trusted:
            w.close()
    finally:
        s.term()


def test_default_connection_limit_is_200():
    s = start("limconn200", [])
    try:
        held = []
        for i in range(200):
            held.append(Wire(s.port, "127.0.0.15"))
        held[-1].cmd({"hello": 1, "$db": "admin"})
        extra = Wire(s.port, "127.0.0.15")
        with pytest.raises((ConnectionError, OSError)):
            extra.cmd({"hello": 1, "$db": "admin"})
        extra.close()
        assert network(s)["config"]["max_conns_per_ip"] == 200
        for w in held:
            w.close()
    finally:
        s.term()


def test_slow_handshake_is_closed():
    s = start("limslow", ["--handshake-timeout-secs", "2"])
    try:
        add_db(s, "slow")
        # nothing sent at all
        w = Wire(s.port, "127.0.0.16")
        t = time.time()
        assert w.closed_within(6)
        assert 1.5 <= time.time() - t <= 5, time.time() - t
        w.close()
        # a message sent one byte at a time, slower than the timeout
        w = Wire(s.port, "127.0.0.17")
        msg = Wire.frame({"hello": 1, "$db": "admin"})
        t = time.time()
        closed = False
        for b in msg:
            try:
                w.send_raw(bytes([b]))
            except OSError:
                closed = True
                break
            time.sleep(0.4)
            if time.time() - t > 6:
                break
        assert closed or w.closed_within(3), "a slow sender was not closed"
        assert time.time() - t < 8
        w.close()
        # a normal client is not affected, and an idle connection that already sent its hello (a driver's monitor) stays open
        w = Wire(s.port, "127.0.0.18")
        assert w.cmd({"hello": 1, "$db": "admin"})["ok"] == 1
        time.sleep(3)
        assert w.cmd({"hello": 1, "$db": "admin"})["ok"] == 1
        w.close()
        f = Forward(s.port, "127.0.0.18")
        c = f.client(username="u1", password=PW, authSource="slow")
        c.slow.c.insert_one({"a": 1})
        time.sleep(3)
        assert c.slow.c.count_documents({}) == 1
        c.close()
        f.close()
        assert network(s)["totals"]["handshake_timeouts"] >= 2
    finally:
        s.term()


def test_message_size_before_authentication():
    s = start("limsize", ["--pre-auth-max-message-bytes", "4096"])
    try:
        add_db(s, "big")
        w = Wire(s.port, "127.0.0.19")
        assert w.cmd({"hello": 1, "$db": "admin"})["ok"] == 1
        with pytest.raises((ConnectionError, OSError)):
            w.cmd({"insert": "c", "documents": [{"pad": "x" * 8000}], "$db": "big"})
        w.close()
        # a header announcing a huge message is refused before anything is allocated or read
        w = Wire(s.port, "127.0.0.19")
        w.send_raw(struct.pack("<iiii", 40_000_000, 1, 0, 2013))
        assert w.closed_within(3)
        w.close()
        # after authentication large messages are fine
        w = Wire(s.port, "127.0.0.19")
        assert w.scram("big", "u1", PW)[1]["ok"] == 1
        r = w.cmd({"insert": "c", "documents": [{"pad": "x" * 1_000_000}], "$db": "big"})
        assert r["ok"] == 1 and r["n"] == 1, r
        w.close()
        assert network(s)["totals"]["oversized_before_auth"] >= 2
    finally:
        s.term()


def test_no_password_or_secret_in_the_log(srv):
    src = "127.0.0.30"
    for i in range(12):
        attempt(srv, src, "bf3", WRONG)
    attempt(srv, src, "bf3", PW)
    log = srv.log_text()
    assert src in log and "blocked" in log
    for secret in (PW, WRONG, "rootpw", "p=", "c=biws"):
        assert secret not in log, secret
