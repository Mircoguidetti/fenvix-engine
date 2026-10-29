"""DELETE /v1/network/blocks/{ip}?database=<db> (engine-round2, point 4): lift ONLY the block of the pair (address, database),
leaving the address-wide block and the blocks of the other databases in place. Without the parameter the call is unchanged:
every block and count of the address goes (the operator's tool)."""
import pytest

from test_14_auth_limits import PW, WRONG, GENERIC, start, add_db, attempt, last_error, network
from netlib import scram_ok

LIMITS = ["--auth-fail-max", "5", "--auth-fail-max-per-db", "2", "--auth-block-secs", "300"]


def blocks_of(srv, ip):
    return sorted((b["database"] or "*") for b in network(srv)["blocked"] if b["ip"] == ip)


@pytest.fixture(scope="module")
def srv():
    s = start("r2unb", LIMITS)
    for db in ("ua", "ub", "uc", "ud"):
        add_db(s, db)
    yield s
    s.term()


def block_pairs(srv, src, dbs):
    for db in dbs:
        for i in range(2):
            attempt(srv, src, db, WRONG + str(i))


def test_pair_unblock_leaves_the_other_databases(srv):
    src = "127.0.0.50"
    block_pairs(srv, src, ("ua", "ub"))                        # 4 failures: two pair blocks, no address block
    assert blocks_of(srv, src) == ["ua", "ub"]
    st, r = srv.admin.delete(f"/v1/network/blocks/{src}?database=ua")
    assert st == 200 and r == {"ip": src, "database": "ua", "unblocked": True}, r
    assert blocks_of(srv, src) == ["ub"]
    assert scram_ok(srv.port, src, "ua", "u1", PW)             # this database is open again
    assert last_error(attempt(srv, src, "ub", PW)) == GENERIC  # the other one is still blocked
    assert scram_ok(srv.port, src, "uc", "u1", PW)


def test_pair_unblock_leaves_the_address_block(srv):
    src = "127.0.0.51"
    block_pairs(srv, src, ("ua", "ub"))
    attempt(srv, src, "uc", WRONG)                             # the 5th failure of the address: blocked everywhere
    assert blocks_of(srv, src) == ["*", "ua", "ub"]
    st, r = srv.admin.delete(f"/v1/network/blocks/{src}?database=ua")
    assert st == 200 and r["unblocked"] is True, r
    assert blocks_of(srv, src) == ["*", "ub"]
    for db in ("ua", "ub", "ud"):
        assert last_error(attempt(srv, src, db, PW)) == GENERIC, db   # the address block still covers every database
    # a pair with nothing to lift: nothing changes
    st, r = srv.admin.delete(f"/v1/network/blocks/{src}?database=ud")
    assert st == 200 and r["unblocked"] is False, r
    assert blocks_of(srv, src) == ["*", "ub"]
    # without the parameter: everything goes, as before
    st, r = srv.admin.delete(f"/v1/network/blocks/{src}")
    assert st == 200 and r == {"ip": src, "unblocked": True}, r
    assert blocks_of(srv, src) == []
    for db in ("ua", "ub", "ud"):
        assert scram_ok(srv.port, src, db, "u1", PW), db


def test_pair_unblock_forgets_the_pair_counts_only(srv):
    src = "127.0.0.52"
    attempt(srv, src, "ua", WRONG)                             # one failure on ua, one on ub: no block yet
    attempt(srv, src, "ub", WRONG)
    st, r = srv.admin.delete(f"/v1/network/blocks/{src}?database=ua")
    assert st == 200 and r["unblocked"] is True, r
    fail = {f["database"]: f["failures_in_window"] for f in network(srv)["failing"] if f["ip"] == src}
    assert fail == {None: 2, "ub": 1}, fail                    # the address count and ub's count are kept


def test_bad_requests(srv):
    assert srv.admin.delete("/v1/network/blocks/127.0.0.53?database=")[0] == 400
    assert srv.admin.delete("/v1/network/blocks/not-an-ip?database=ua")[0] == 400
    assert srv.admin.call("DELETE", "/v1/network/blocks/127.0.0.53?database=ua", token=None)[0] == 401
    st, r = srv.admin.delete("/v1/network/blocks/127.0.0.54?database=ua")       # unknown address
    assert st == 200 and r["unblocked"] is False, r
