"""Default thresholds of the authentication blocks (engine-round2, point 2), on a server started WITHOUT any --auth-fail-*
or --auth-block-* flag:

* per address: 100 failed authentications in 60 s block the address on every database (was 10). A platform with one database
  per customer connects from few addresses: after a wrong password rotation, 10 failures would have blocked every database of
  the platform for up to an hour;
* per (address, database): 5 failures in 60 s block that database only (unchanged);
* every threshold and duration has a flag with a help text, and GET /v1/network shows the values in force.
The server trusts only 127.0.0.1/32 (the harness); 127.0.0.x are outside clients."""
import re
import subprocess

import pytest

from lib import BIN
from test_14_auth_limits import PW, WRONG, GENERIC, start, add_db, attempt, last_error, network, blocked
from netlib import scram_ok

DBS = [f"d{i:02d}" for i in range(26)]

DEFAULTS = {
    "auth_fail_max": 100, "auth_fail_max_per_db": 5, "auth_fail_window_secs": 60, "auth_block_secs": 300,
    "auth_block_max_secs": 3600, "auth_block_forget_secs": 86400, "max_conns_per_ip": 200, "handshake_timeout_secs": 10,
    "pre_auth_max_message_bytes": 65536, "ip_allowlist_refresh_secs": 5, "ipv6_group_prefix": 64, "netguard_max_tracked": 100000,
    "netguard_max_dbs_per_ip": 1000,
}

# flag -> key of GET /v1/network config
FLAGS = {
    "--auth-fail-max": "auth_fail_max", "--auth-fail-max-per-db": "auth_fail_max_per_db", "--auth-fail-window-secs": "auth_fail_window_secs",
    "--auth-block-secs": "auth_block_secs", "--auth-block-max-secs": "auth_block_max_secs", "--auth-block-forget-secs": "auth_block_forget_secs",
    "--max-conns-per-ip": "max_conns_per_ip", "--handshake-timeout-secs": "handshake_timeout_secs",
    "--pre-auth-max-message-bytes": "pre_auth_max_message_bytes", "--ip-allowlist-refresh-secs": "ip_allowlist_refresh_secs",
    "--ipv6-group-prefix": "ipv6_group_prefix", "--netguard-max-tracked": "netguard_max_tracked", "--netguard-max-dbs-per-ip": "netguard_max_dbs_per_ip",
}


@pytest.fixture(scope="module")
def srv():
    s = start("r2lim")          # no limit flag at all: the defaults
    for db in DBS:
        add_db(s, db)
    yield s
    s.term()


def test_status_shows_the_defaults(srv):
    cfg = network(srv)["config"]
    for k, v in DEFAULTS.items():
        assert cfg.get(k) == v, (k, cfg.get(k), v)


def test_99_failures_on_different_databases_do_not_block_the_100th_does(srv):
    src = "127.0.0.30"
    # 4 failures on each of 24 databases + 3 on the 25th = 99 failures; no (address, database) pair reaches 5
    plan = [db for db in DBS[:24] for _ in range(4)] + [DBS[24]] * 3
    assert len(plan) == 99
    for i, db in enumerate(plan):
        assert last_error(attempt(srv, src, db, WRONG + str(i))) == GENERIC
    assert blocked(srv, src) == [] and not [b for b in network(srv)["blocked"] if b["ip"] == src]
    fail = [f for f in network(srv)["failing"] if f["ip"] == src and f["database"] is None]
    assert fail and fail[0]["failures_in_window"] == 99, fail
    # still open after 99: the right password works, on a database with failures and on one without
    assert scram_ok(srv.port, src, DBS[0], "u1", PW)
    assert scram_ok(srv.port, src, DBS[25], "u1", PW)
    # the 100th failure blocks the address on every database
    assert last_error(attempt(srv, src, DBS[25], WRONG)) == GENERIC
    b = blocked(srv, src)
    assert len(b) == 1 and b[0]["strike"] == 1 and 290 <= b[0]["remaining_secs"] <= 300, b
    for db in (DBS[0], DBS[25], DBS[10]):
        assert last_error(attempt(srv, src, db, PW)) == GENERIC, db
    # another address is not affected
    assert scram_ok(srv.port, "127.0.0.31", DBS[0], "u1", PW)


def test_5_failures_on_one_pair_block_only_that_database(srv):
    src = "127.0.0.32"
    for i in range(4):
        attempt(srv, src, DBS[3], WRONG + str(i))
    assert scram_ok(srv.port, src, DBS[3], "u1", PW)           # 4 failures: still open
    attempt(srv, src, DBS[3], WRONG)                           # the 5th
    assert last_error(attempt(srv, src, DBS[3], PW)) == GENERIC
    assert blocked(srv, src, DBS[3]) and not blocked(srv, src)
    for db in (DBS[4], DBS[20]):
        assert scram_ok(srv.port, src, db, "u1", PW), db       # the other databases stay open for this address
    assert scram_ok(srv.port, "127.0.0.33", DBS[3], "u1", PW)  # and the database for other addresses


def test_status_shows_the_values_in_force():
    vals = {"--auth-fail-max": 37, "--auth-fail-max-per-db": 7, "--auth-fail-window-secs": 45, "--auth-block-secs": 120,
            "--auth-block-max-secs": 1800, "--auth-block-forget-secs": 7200, "--max-conns-per-ip": 50, "--handshake-timeout-secs": 7,
            "--pre-auth-max-message-bytes": 32768, "--ip-allowlist-refresh-secs": 3, "--ipv6-group-prefix": 56, "--netguard-max-tracked": 5000,
            "--netguard-max-dbs-per-ip": 300}
    s = start("r2limcfg", [x for k, v in vals.items() for x in (k, str(v))])
    try:
        cfg = network(s)["config"]
        for flag, v in vals.items():
            assert cfg[FLAGS[flag]] == v, (flag, cfg[FLAGS[flag]])
    finally:
        s.term()


def test_every_threshold_and_duration_has_a_documented_flag():
    out = subprocess.run([BIN, "--help"], capture_output=True, text=True, timeout=30).stdout
    # clap prints "      --flag <VALUE>\n          help text..." ; the help text of each flag must say something
    for flag in FLAGS:
        m = re.search(r"^\s+" + re.escape(flag) + r" <[A-Z0-9_]+>\n((?:\s{8,}\S.*\n)+)", out, re.M)
        assert m, f"{flag}: not in --help or without a description"
        text = m.group(1)
        assert len(text.split()) >= 4, (flag, text)
        assert "[default:" in text, (flag, text)
