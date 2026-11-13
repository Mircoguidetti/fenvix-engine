"""sim-test-3 (2026-11-13): conformance of the simulator's port of the replicated log's client rules with the real code.

The simulator (src/fx/sim/cluster/ext3.rs) cannot call mongo-server/src/locallog.rs (a process-wide singleton behind the
server's lease table): it ports its rules. A port that drifts from the code would give false 0s. This test drives the
REAL engine (two engine processes, three copies of the log, the same harness as test_91) through test_91's order, with the
fix's rules and with the base's rules (`MONGO_TEST_KEPT_RETRY_OLD=1`), reads every decision of the log's client rules
from the engine's own log lines, and checks them against `sim_log_conformance.json`. The simulator's story
`fx::sim::tests::sim_story_log_takeover_retry` checks its own decisions against the same table.

Decisions (see the JSON): the restart read the kept records back; its replay was refused by the lease; copies of the kept
records written to the log or not; the retry told the log only what it read at its start or not; the retry ended; the
takeover applied W2 from the log; W2 lost.

Needs MinIO and the log's binary `pq`, as test_91.
"""
import json
import os
import re
import tempfile
import time

import pytest

from lib import make_bucket, wait_until, ROOT
from test_91_takeover_retry import DB, W, Log, engine, put_rules, insert, read_all

TABLE = json.load(open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "sim_log_conformance.json")))


def decisions(old):
    log = Log().start()
    bucket = make_bucket("slc")
    fa, fb = tempfile.mktemp(prefix="slcfa"), tempfile.mktemp(prefix="slcfb")
    put_rules(fa)
    put_rules(fb)
    b = engine(bucket, "slcb", log, fb)
    a = engine(bucket, "slca", log, fa, env={"MONGO_TEST_KEPT_RETRY_OLD": "1"} if old else None)
    try:
        b.start()
        w0 = [f"w0-{i}" for i in range(W)]
        insert(b, w0)
        time.sleep(3)
        put_rules(fb, "hang_put /wal/", "log_put /wal/")
        time.sleep(1.5)
        w1 = [f"w1-{i}" for i in range(W)]
        insert(b, w1)
        wait_until(lambda: "FAULT hang_put" in b.log_text(), 30, what="B's upload of W1 to be held")
        put_rules(fa, "hold_kept_records 1")
        a.start(wait=False)
        wait_until(lambda: f"FAULT hold_kept_records {DB}" in a.log_text(), 60, what="A to stop at hold_kept_records")
        mark = len(b.log_text())
        put_rules(fb, "log_put /wal/")
        wait_until(lambda: re.search(r"PUTLOG ok \S*/wal/", b.log_text()[mark:]), 30, what="B's held upload to finish")
        time.sleep(4)
        put_rules(fb, "hang_put /wal/", "log_put /wal/")
        time.sleep(1.5)
        mark = len(b.log_text())
        w2 = [f"w2-{i}" for i in range(W)]
        insert(b, w2)
        wait_until(lambda: "FAULT hang_put" in b.log_text()[mark:], 30, what="B's upload of W2 to be held")
        put_rules(fa)
        a.wait_ready(60)
        wait_until(lambda: re.search(rf"database {DB}: kept records .*by the retry", a.log_text()), 60, what="A's retry to end")
        time.sleep(1)
        b.kill9()
        got = set(read_all(a))
        lost = [i for i in w0 + w1 + w2 if i not in got]
        t = a.log_text()
        m = re.search(r"restart read (\d+) records", t)
        tk = re.search(rf"takeover of {DB}: (\d+) records applied from the log", t)
        d = {
            "restart_read_kept": bool(m and int(m.group(1)) >= W),
            "replay_refused_by_lease": bool(re.search(rf"replay of database {DB} failed \(tenant {DB} is owned by server process", t)),
            # the fix's branch says so; the base's rules (the control switch) take the other branch, which writes the copies
            "kept_copies_in_log": not re.search(rf"quaderno: database {DB}: its replay was refused before it took the lease; its \d+ records stay in the log as they are \(no copies\)", t),
            "retry_confirms_read_pos": bool(re.search(rf"quaderno: database {DB}: not the lease holder: the log is told only up to position \d+, the highest this process read at its start", t)),
            "retry_done": bool(re.search(rf"database {DB}: kept records applied by the retry", t)),
            "takeover_applies_w2": bool(tk and int(tk.group(1)) >= W),
            "w2_lost": any(i.startswith("w2-") for i in lost),
        }
        if old:
            assert "TEST: the old handling of kept records is on" in t, "the base's rules were not switched on in A"
        others = [i for i in lost if not i.startswith("w2-")]
        print(f"old={old}: decisions {d}; lost {len(lost)} (outside W2: {len(others)})")
        print("A:", "\n".join(l for l in t.splitlines() if "quaderno" in l or "local log" in l or "FAULT" in l))
        return d, others
    finally:
        for s in (a, b):
            try:
                s.kill9()
            except Exception:
                pass
        log.stop()


@pytest.mark.parametrize("rules", ["fixed", "base"])
def test_real_engine_decides_as_the_simulator(rules):
    d, others = decisions(old=(rules == "base"))
    want = TABLE[rules]
    diff = {k: (d.get(k), v) for k, v in want.items() if d.get(k) != v}
    assert not diff, f"the real engine ({rules} rules) decides differently from the simulator's port: {diff} (real, table)"
    assert not others, f"writes outside W2 lost: {others[:5]}"
