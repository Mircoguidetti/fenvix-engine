"""Per-database list of allowed client networks (A1-A8 of the network-limits work), on a server with --auth.

The list is managed only on the admin port (PUT / GET / DELETE /v1/databases/{db}/ip-allowlist), stored with the database's
metadata (control/dbs/{db}/meta.json), and checked when a client authenticates (SCRAM) on that database: a client whose TCP peer
address is not in the list gets exactly what a wrong password gets. --trusted-networks lists the networks that skip it.

The servers here trust only 127.0.0.1/32 (the test harness and root), so connections bound to 127.0.0.2 ... 127.0.0.250 are
"outside" clients. IPv6 cannot be tried end to end here (the sandbox kernel has no IPv6): see the unit tests in netguard.rs."""
import json
import statistics
import time

import pytest
from pymongo.errors import OperationFailure

from lib import Server, make_bucket, list_prefix, wait_until, TOKEN
from netlib import Wire, Forward, scram_ok, shape_of_start, admin_from

PW = "Allow-SECRET-7c41e9b2"
NO_LIMITS = ["--auth-fail-max", "1000000", "--auth-fail-max-per-db", "1000000"]   # the lists are tested apart from the blocks


def start(bucket, tag, extra=(), trusted="127.0.0.1/32", idle=300):
    flags = ["--auth"] + (["--trusted-networks", trusted] if trusted is not None else []) + NO_LIMITS + list(extra)
    srv = Server(bucket, tag, idle=idle, extra=flags).start()
    c = srv.client()
    try:
        c.admin.command("createUser", "root", pwd="rootpw", roles=[{"role": "root", "db": "admin"}])  # localhost exception
    except OperationFailure:
        pass
    c.close()
    return srv


def root(srv):
    return srv.client(username="root", password="rootpw", authSource="admin")


def new_db(srv, db, user="u1", pwd=PW):
    st, r = srv.admin.post("/v1/databases", {"name": db})
    assert st == 201, r
    r = root(srv)
    r[db].command("createUser", user, pwd=pwd, roles=[{"role": "readWrite", "db": db}])
    r.close()


def put_list(srv, db, entries):
    return srv.admin.put(f"/v1/databases/{db}/ip-allowlist", {"entries": entries})


def get_list(srv, db):
    return srv.admin.get(f"/v1/databases/{db}/ip-allowlist")


@pytest.fixture(scope="module")
def env():
    bucket = make_bucket("allow")
    srv = start(bucket, "allow", idle=4)
    yield srv, bucket
    srv.term()


# ------------------------------------------------------------------------------------------------ admin commands

def test_set_get_clear(env):
    srv, _ = env
    new_db(srv, "lst")
    st, r = get_list(srv, "lst")
    assert st == 200 and r["entries"] == [] and r["allow_all"] is True, r
    st, r = put_list(srv, "lst", ["10.0.0.0/8", "192.168.1.7", "2001:db8::/32", "::ffff:172.16.0.0/108", "2001:DB8:1::1"])
    assert st == 200, r
    # stored in canonical form: a single address gets its full prefix, a mapped IPv4 network becomes the IPv4 one, IPv6 is lower case
    want = ["10.0.0.0/8", "192.168.1.7/32", "2001:db8::/32", "172.16.0.0/12", "2001:db8:1::1/128"]
    assert r["entries"] == want and r["allow_all"] is False and r["enforced"] is True, r
    assert get_list(srv, "lst")[1]["entries"] == want
    st, r = srv.admin.delete("/v1/databases/lst/ip-allowlist")
    assert st == 200 and r["entries"] == [] and r["allow_all"] is True, r
    assert get_list(srv, "lst")[1]["entries"] == []
    # an empty list is the same as no list
    assert put_list(srv, "lst", [])[1]["allow_all"] is True
    # unknown database, reserved and invalid names
    assert get_list(srv, "ghost")[0] == 404
    assert put_list(srv, "ghost", ["10.0.0.0/8"])[0] == 404
    assert srv.admin.delete("/v1/databases/ghost/ip-allowlist")[0] == 404
    for bad in ("admin", "local", "config", "bad.name"):
        assert put_list(srv, bad, ["10.0.0.0/8"])[0] == 400, bad
    # without the token: nothing
    for tok in (None, "wrong-token-0123456789"):
        assert srv.admin.call("PUT", "/v1/databases/lst/ip-allowlist", {"entries": ["1.2.3.4"]}, token=tok)[0] == 401
        assert srv.admin.call("GET", "/v1/databases/lst/ip-allowlist", token=tok)[0] == 401
        assert srv.admin.call("DELETE", "/v1/databases/lst/ip-allowlist", token=tok)[0] == 401
    assert get_list(srv, "lst")[1]["entries"] == []


HOSTILE = [
    "10.0.0.0/33", "::/129", "10.0.0.1/8", "2001:db8::1/32", "300.1.1.1", "1.2.3", "1.2.3.4.5", "010.0.0.1", "10.0.0.0/08", "10.0.0.0/+8",
    "10.0.0.0/-1", "10.0.0.0/", "/8", "", " ", " 10.0.0.0/8", "10.0.0.0/8 ", "10.0.0.0/8\n", "10.0.0.0/8\x00", "fe80::1%eth0", "fe80::1%25eth0",
    "::ffff:1.2.3.4/200", "::ffff:10.0.0.0/90", "abc", "localhost", "*", "0/0", "١٠.0.0.0/8", "10.0.0.0/8;rm -rf /", "<script>alert(1)</script>",
    "10.0.0.0/8,11.0.0.0/8", "10.0.0.0 /8", "1.2.3.4/32/32", "0x0a.0.0.1", "10.0.0.0\t/8", "1" * 5000, "10.0.0.0/8" + " " * 100,
]


@pytest.mark.parametrize("entry", HOSTILE, ids=[repr(h)[:30] for h in HOSTILE])
def test_hostile_entries_are_refused(env, entry):
    srv, _ = env
    if get_list(srv, "hostile")[0] == 404:
        new_db(srv, "hostile")
    assert put_list(srv, "hostile", ["10.1.0.0/16"])[0] == 200
    st, r = put_list(srv, "hostile", [entry])
    assert st == 400, (entry, st, r)
    assert r["error"]["code"] == "invalid_allowlist", r
    msg = r["error"]["message"]
    assert "entry" in msg and len(msg) < 400, msg
    assert get_list(srv, "hostile")[1]["entries"] == ["10.1.0.0/16"]          # unchanged


@pytest.mark.parametrize("body", [
    {"entries": ["10.0.0.0/8", "10.0.0.0/8"]},                          # duplicate
    {"entries": ["1.2.3.4", "1.2.3.4/32"]},                             # duplicate once canonical
    {"entries": ["10.0.0.0/8", "::ffff:10.0.0.0/104"]},                 # duplicate: the mapped form of the same network
    {"entries": ["2001:db8::/32", "2001:DB8::/32"]},
    {"entries": [f"10.0.{i // 256}.{i % 256}/32" for i in range(101)]},  # more than 100
    {"entries": "10.0.0.0/8"},                                          # not a list
    {"entries": [10]}, {"entries": [None]}, {"entries": [["10.0.0.0/8"]]}, {"entries": [{"cidr": "10.0.0.0/8"}]},
    {},                                                                 # missing
    {"entries": ["10.0.0.0/8"], "mode": "deny"},                        # unknown field
    {"entries": None},
], ids=["dup", "dup-canonical", "dup-mapped", "dup-case", "101", "string", "int", "null", "nested", "object", "missing", "unknown", "none"])
def test_hostile_bodies_are_refused(env, body):
    srv, _ = env
    if get_list(srv, "hostile2")[0] == 404:
        new_db(srv, "hostile2")
    assert put_list(srv, "hostile2", ["10.1.0.0/16"])[0] == 200
    st, r = srv.admin.put("/v1/databases/hostile2/ip-allowlist", body)
    assert st == 400, (body, st, r)
    assert r["error"]["code"] in ("invalid_allowlist", "bad_request"), r
    assert get_list(srv, "hostile2")[1]["entries"] == ["10.1.0.0/16"]


def test_body_that_is_not_json_and_the_limit_of_100(env):
    srv, _ = env
    new_db(srv, "hundred")
    st, r = srv.admin.call("PUT", "/v1/databases/hundred/ip-allowlist", None, headers={"Content-Type": "application/json"})
    assert st == 400
    import urllib.request
    req = urllib.request.Request(f"http://127.0.0.1:{srv.admin_port}/v1/databases/hundred/ip-allowlist", data=b"{entries: [1.2.3.4]", method="PUT",
                                 headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"})
    try:
        urllib.request.urlopen(req)
        assert False, "accepted a body that is not JSON"
    except urllib.error.HTTPError as e:
        assert e.code == 400
    entries = [f"10.0.{i // 256}.{i % 256}/32" for i in range(100)]
    st, r = put_list(srv, "hundred", entries)
    assert st == 200 and len(r["entries"]) == 100, r
    st, r = put_list(srv, "hundred", entries + ["10.9.9.9/32"])
    assert st == 400 and "100" in r["error"]["message"], r
    assert len(get_list(srv, "hundred")[1]["entries"]) == 100


def test_no_equivalent_command_on_the_data_port(env):
    srv, _ = env
    new_db(srv, "dataport")
    assert put_list(srv, "dataport", ["10.1.0.0/16"])[0] == 200
    r = root(srv)
    for cmd in ({"setIpAllowlist": "dataport", "entries": []}, {"ipAllowlist": 1}, {"clearIpAllowlist": "dataport"}, {"getIpAllowlist": "dataport"},
                {"setNetworkLimits": 1}, {"networkLimits": 1}, {"unblockIp": "127.0.0.2"}):
        for db in ("admin", "dataport"):
            with pytest.raises(OperationFailure) as e:
                r[db].command(cmd)
            assert e.value.code in (13, 59), (cmd, e.value.code)
    # the list lives in the control area, which no data-port command reaches (it is not a collection of any database)
    assert "ip_allowlist" not in json.dumps(r["dataport"].list_collection_names())
    r.close()
    assert get_list(srv, "dataport")[1]["entries"] == ["10.1.0.0/16"]


# ------------------------------------------------------------------------------------------------ enforcement

def test_allowed_and_not_allowed(env):
    srv, _ = env
    new_db(srv, "app1")
    assert scram_ok(srv.port, "127.0.0.3", "app1", "u1", PW)                    # no list: everyone
    assert put_list(srv, "app1", ["127.0.0.2/32"])[0] == 200
    assert scram_ok(srv.port, "127.0.0.2", "app1", "u1", PW)
    assert not scram_ok(srv.port, "127.0.0.3", "app1", "u1", PW)
    # pymongo through a connection from each address
    for src, ok in (("127.0.0.2", True), ("127.0.0.3", False)):
        f = Forward(srv.port, src)
        c = f.client(username="u1", password=PW, authSource="app1")
        try:
            if ok:
                c.app1.c.insert_one({"from": src})
                assert c.app1.c.count_documents({}) >= 1
            else:
                with pytest.raises(OperationFailure) as e:
                    c.app1.c.insert_one({"from": src})
                assert e.value.code == 18 and e.value.details["errmsg"] == "Authentication failed.", e.value.details
        finally:
            c.close()
            f.close()
    assert srv.admin.delete("/v1/databases/app1/ip-allowlist")[0] == 200
    assert scram_ok(srv.port, "127.0.0.3", "app1", "u1", PW)


@pytest.mark.parametrize("entries,src,ok", [
    (["0.0.0.0/0"], "127.0.0.3", True),
    (["127.0.0.2/31"], "127.0.0.2", True), (["127.0.0.2/31"], "127.0.0.3", True), (["127.0.0.2/31"], "127.0.0.4", False),
    (["127.0.0.4/32"], "127.0.0.4", True), (["127.0.0.4/32"], "127.0.0.5", False),
    (["127.0.0.0/8"], "127.0.0.250", True), (["127.0.0.0/9"], "127.0.0.250", True), (["127.128.0.0/9"], "127.0.0.250", False),
    (["::ffff:127.0.0.6/128"], "127.0.0.6", True),     # an IPv4 peer matches the IPv4-mapped form of its address
    (["::ffff:127.0.0.0/104"], "127.0.0.7", True),
    (["::/0"], "127.0.0.3", False),                    # an IPv6 network never matches an IPv4 peer (documented)
    (["2001:db8::/32", "10.0.0.0/8"], "127.0.0.3", False),
    (["2001:db8::/32", "127.0.0.3/32"], "127.0.0.3", True),
])
def test_boundary_networks(env, entries, src, ok):
    srv, _ = env
    if get_list(srv, "edge")[0] == 404:
        new_db(srv, "edge")
    assert put_list(srv, "edge", entries)[0] == 200
    assert scram_ok(srv.port, src, "edge", "u1", PW) is ok, (entries, src)


def test_refusal_is_identical_to_a_wrong_password(env):
    """Same replies, same codes and messages, wire level and through pymongo, and similar times; the list is not revealed."""
    srv, _ = env
    new_db(srv, "same")
    assert put_list(srv, "same", ["127.0.0.2/32"])[0] == 200
    allowed, denied = "127.0.0.2", "127.0.0.3"

    def run(src, pwd, user="u1"):
        w = Wire(srv.port, src)
        try:
            return w.scram("same", user, pwd)
        finally:
            w.close()

    a1, a2 = run(allowed, "wrong-password")            # wrong password from an allowed address
    d1, d2 = run(denied, PW)                            # right password from an address that is not allowed
    assert shape_of_start(a1) == shape_of_start(d1), (a1, d1)
    assert bytes(a1["payload"]).decode().split(",")[1] == bytes(d1["payload"]).decode().split(",")[1]   # the same salt
    assert a2 == d2, (a2, d2)
    assert a2 == {"ok": 0.0, "errmsg": "Authentication failed.", "code": 18, "codeName": "AuthenticationFailed"}, a2
    # an unknown user: the same from both addresses
    assert run(allowed, PW, "nobody") == run(denied, PW, "nobody")
    # wrong password from the address that is not allowed: also the same
    assert run(denied, "wrong-password")[1] == a2
    # pymongo: same exception, code and message
    errs = []
    for src, pwd in ((allowed, "wrong-password"), (denied, PW)):
        f = Forward(srv.port, src)
        c = f.client(username="u1", password=pwd, authSource="same")
        try:
            with pytest.raises(OperationFailure) as e:
                c.same.command("ping")
            errs.append((type(e.value), e.value.code, e.value.details))
        finally:
            c.close()
            f.close()
    assert errs[0] == errs[1], errs
    # times: the refused path does the same work up to the proof (user lookup, server-first), then stops
    def timed(src, pwd):
        w = Wire(srv.port, src)
        try:
            t = time.perf_counter()
            w.scram("same", "u1", pwd)
            return time.perf_counter() - t
        finally:
            w.close()
    for _ in range(5):
        timed(allowed, "wrong-password"), timed(denied, PW)
    ta = statistics.median(timed(allowed, "wrong-password") for _ in range(40))
    td = statistics.median(timed(denied, PW) for _ in range(40))
    print(f"median wrong password {ta * 1000:.2f} ms, not allowed {td * 1000:.2f} ms")
    assert abs(ta - td) < max(0.003, 0.5 * ta), (ta, td)


def test_refusal_is_logged_without_secrets(env):
    srv, _ = env
    new_db(srv, "logged", pwd="Logged-SECRET-31d9a7")
    assert put_list(srv, "logged", ["127.0.0.2/32"])[0] == 200
    w = Wire(srv.port, "127.0.0.9")
    r1, r2 = w.scram("logged", "u1", "Logged-SECRET-31d9a7", nonce="NONCE-MARKER-4b1c9e2f7a")
    w.close()
    assert r2["code"] == 18
    log = wait_until(lambda: (lambda t: t if "127.0.0.9" in t else None)(srv.log_text()), 5, what="log line")
    lines = [l for l in log.splitlines() if "127.0.0.9" in l]
    assert any("not in the allow list" in l and "logged" in l for l in lines), lines
    for secret in ("Logged-SECRET-31d9a7", "NONCE-MARKER-4b1c9e2f7a", "p=", "c=biws", "u1"):
        assert not any(secret in l for l in lines), (secret, lines)


def test_list_survives_sleep_wake_and_restart(env):
    srv, bucket = env
    new_db(srv, "sleepy")
    r = root(srv)
    r.sleepy.c.insert_one({"x": 1})
    r.close()
    assert put_list(srv, "sleepy", ["127.0.0.2/32"])[0] == 200
    assert not scram_ok(srv.port, "127.0.0.3", "sleepy", "u1", PW)
    # stored in the metadata of the database, in the bucket
    keys = [k for k, _ in list_prefix(bucket, "control/dbs/sleepy/")]
    assert keys == ["control/dbs/sleepy/meta.json"], keys
    wait_until(lambda: srv.admin.get("/v1/databases/sleepy?storage=false")[1]["state"] == "asleep", 30, what="sleep")
    assert not scram_ok(srv.port, "127.0.0.3", "sleepy", "u1", PW)
    assert scram_ok(srv.port, "127.0.0.2", "sleepy", "u1", PW)
    srv.restart()
    assert get_list(srv, "sleepy")[1]["entries"] == ["127.0.0.2/32"]
    assert not scram_ok(srv.port, "127.0.0.3", "sleepy", "u1", PW)
    assert scram_ok(srv.port, "127.0.0.2", "sleepy", "u1", PW)


def test_fork_does_not_inherit_the_list(env):
    srv, _ = env
    new_db(srv, "parent")
    r = root(srv)
    r.parent.c.insert_one({"x": 1})
    assert put_list(srv, "parent", ["127.0.0.2/32"])[0] == 200
    st, f = srv.admin.post("/v1/databases/parent/fork", {"target": "child"})
    assert st == 201, f
    assert get_list(srv, "child")[1]["entries"] == []
    r.child.command("createUser", "u1", pwd=PW, roles=[{"role": "readWrite", "db": "child"}])     # a fork has no users either
    r.close()
    assert scram_ok(srv.port, "127.0.0.3", "child", "u1", PW)
    assert not scram_ok(srv.port, "127.0.0.3", "parent", "u1", PW)
    assert get_list(srv, "parent")[1]["entries"] == ["127.0.0.2/32"]


def test_erase_deletes_the_list(env):
    srv, bucket = env
    new_db(srv, "gone")
    assert put_list(srv, "gone", ["127.0.0.2/32"])[0] == 200
    assert not scram_ok(srv.port, "127.0.0.3", "gone", "u1", PW)
    st, r = srv.admin.delete("/v1/databases/gone")
    assert st == 200, r
    assert list_prefix(bucket, "control/dbs/gone/") == []
    assert get_list(srv, "gone")[0] == 404
    # a new database with the same name starts without a list
    new_db(srv, "gone")
    assert get_list(srv, "gone")[1]["entries"] == []
    assert scram_ok(srv.port, "127.0.0.3", "gone", "u1", PW)


def test_restore_leaves_the_list_alone():
    bucket = make_bucket("allowrst")
    srv = start(bucket, "allowrst", extra=["--retention-interval-secs", "2", "--retention-prune-secs", "3"])
    try:
        new_db(srv, "rst")
        r = root(srv)
        r.rst.c.insert_one({"v": 1})
        assert srv.admin.put("/v1/databases/rst/retention", {"window_secs": 3600, "interval_secs": 2})[0] == 200
        time.sleep(1.5)
        t_before = int(time.time() * 1000)
        time.sleep(1.5)
        r.rst.c.insert_one({"v": 2})
        assert put_list(srv, "rst", ["127.0.0.2/32"])[0] == 200        # set after the restore point
        st, out = srv.admin.post("/v1/databases/rst/restore", {"to": t_before})
        assert st == 200, out
        assert [d["v"] for d in r.rst.c.find({}, {"_id": 0})] == [1]
        assert get_list(srv, "rst")[1]["entries"] == ["127.0.0.2/32"]
        assert not scram_ok(srv.port, "127.0.0.3", "rst", "u1", PW)
        # and a list cleared after a point stays cleared after a restore to that point
        assert srv.admin.delete("/v1/databases/rst/ip-allowlist")[0] == 200
        st, out = srv.admin.post("/v1/databases/rst/restore", {"to": int(time.time() * 1000) - 500})
        assert st == 200, out
        assert get_list(srv, "rst")[1]["entries"] == []
        r.close()
    finally:
        srv.term()


def test_trusted_networks_skip_the_list():
    bucket = make_bucket("trust")
    # default: loopback only, so every 127.0.0.x is trusted
    srv = start(bucket, "trust-default", trusted=None)
    try:
        new_db(srv, "t1")
        assert put_list(srv, "t1", ["10.9.9.9/32"])[0] == 200
        for src in ("127.0.0.1", "127.0.0.2", "127.0.0.200"):
            assert scram_ok(srv.port, src, "t1", "u1", PW), src
        st, net = srv.admin.get("/v1/network")
        assert st == 200 and net["config"]["trusted_networks"] == ["127.0.0.0/8", "::1/128"], net["config"]
    finally:
        srv.term()
    srv = start(bucket, "trust-24", trusted="127.0.0.1/32,127.0.0.16/28")
    try:
        assert put_list(srv, "t1", ["10.9.9.9/32"])[0] == 200
        assert get_list(srv, "t1")[1]["entries"] == ["10.9.9.9/32"]
        assert scram_ok(srv.port, "127.0.0.1", "t1", "u1", PW)
        assert scram_ok(srv.port, "127.0.0.20", "t1", "u1", PW)
        assert not scram_ok(srv.port, "127.0.0.2", "t1", "u1", PW)
        assert not scram_ok(srv.port, "127.0.0.32", "t1", "u1", PW)
    finally:
        srv.term()


@pytest.mark.parametrize("bad", ["127.0.0.0/33", "loopback", "127.0.0.1/8", "10.0.0.0/8,,", "*"])
def test_invalid_trusted_networks_refuse_to_start(bad):
    bucket = make_bucket("trustbad")
    srv = Server(bucket, "trustbad", extra=["--auth", "--trusted-networks", bad])
    with pytest.raises(RuntimeError):
        srv.start()
    assert "trusted-networks" in srv.log_text()


def test_admin_port_is_not_subject_to_the_list(env):
    srv, _ = env
    new_db(srv, "adm")
    assert put_list(srv, "adm", ["10.9.9.9/32"])[0] == 200
    st, r = admin_from(srv.admin_port, "127.0.0.3", "GET", "/v1/databases/adm/ip-allowlist")
    assert st == 200 and r["entries"] == ["10.9.9.9/32"]
    st, r = admin_from(srv.admin_port, "127.0.0.3", "DELETE", "/v1/databases/adm/ip-allowlist")
    assert st == 200
    assert scram_ok(srv.port, "127.0.0.3", "adm", "u1", PW)


def test_proxy_protocol_and_client_metadata_are_not_trusted(env):
    """The address is the TCP peer's: a PROXY protocol header or a client field claiming another address changes nothing."""
    srv, _ = env
    new_db(srv, "noproxy")
    assert put_list(srv, "noproxy", ["10.9.9.9/32"])[0] == 200
    w = Wire(srv.port, "127.0.0.3")
    w.send_raw(b"PROXY TCP4 10.9.9.9 127.0.0.1 40000 27017\r\n")
    assert w.closed_within(5)              # not a MongoDB message: the connection is closed
    w.close()
    w = Wire(srv.port, "127.0.0.3")
    w.cmd({"hello": 1, "client": {"application": {"name": "x"}, "driver": {"name": "x", "version": "1"}, "os": {"type": "Linux"},
                                   "env": {"name": "10.9.9.9"}}, "$db": "admin", "X-Forwarded-For": "10.9.9.9"})
    _, r2 = w.scram("noproxy", "u1", PW)
    w.close()
    assert r2["code"] == 18
    assert not scram_ok(srv.port, "127.0.0.3", "noproxy", "u1", PW)


def test_connections_already_authenticated_lose_the_database_when_the_list_excludes_them(env):
    srv, _ = env
    new_db(srv, "live")
    w = Wire(srv.port, "127.0.0.3")
    _, r2 = w.scram("live", "u1", PW)
    assert r2["ok"] == 1
    assert w.cmd({"insert": "c", "documents": [{"a": 1}], "$db": "live"})["ok"] == 1
    w2 = Wire(srv.port, "127.0.0.2")
    assert w2.scram("live", "u1", PW)[1]["ok"] == 1
    assert put_list(srv, "live", ["127.0.0.2/32"])[0] == 200
    r = w.cmd({"insert": "c", "documents": [{"a": 2}], "$db": "live"})
    assert r["ok"] == 0 and r["code"] == 13, r
    assert w2.cmd({"insert": "c", "documents": [{"a": 3}], "$db": "live"})["ok"] == 1      # an allowed one keeps working
    w.close()
    w2.close()


def test_list_is_reported_as_not_enforced_without_auth():
    bucket = make_bucket("noauth")
    srv = Server(bucket, "noauth", extra=["--trusted-networks", "127.0.0.1/32"]).start()
    try:
        assert srv.admin.post("/v1/databases", {"name": "na"})[0] == 201
        st, r = put_list(srv, "na", ["10.0.0.0/8"])
        assert st == 200 and r["enforced"] is False, r
    finally:
        srv.term()
