"""Fix check of erase-cost-1tb c17e19e (review, 2026-11-13): an erasure that arrives while a scrub pass in parts runs must
never make erased documents readable between two parts. Through the server's real pass (`run_pass_scrub`), not a copy
of its loop.

The scrub path (`--sealed-erasure-ride false`) runs in parts of about 2.5 s (GC grace 10 s / 4; hold life 8 s, so reads survive the delayed GETs); every GET of
a run is delayed (fault file), so a part rewrites few runs; every part pauses 1.5 s after its prepare (failpoint
`erase.after_prepare`), so readers see the state each part published. Customer u1 is erased first; right after the first part
of its phase 1, customer u2 is erased (before the fix it joined the pass at phase 2's first part). A reader polls u2's documents
without pause until both erasures are scrubbed: none may ever be readable after u2's deleteMany answered. Also: u1 stays
erased, the other customers stay whole, and the trace shows whether u2 joined u1's pass.
"""
import json
import os

import pytest
import tempfile
import threading
import time

from lib import Server, make_bucket, wait_until

# the judge's and CI's "off" mode sets FENVIX_SEALED_RUNS=off: these tests need sealed runs and the compactor
pytestmark = pytest.mark.skipif(os.environ.get("FENVIX_SEALED_RUNS") == "off", reason="needs sealed runs (FENVIX_SEALED_RUNS=off)")

DB = "ecj"
BASE = ["--sealed-runs", "2", "--sealed-t-pin-ms", "1000", "--sealed-e1-wait-ms", "0", "--sealed-test-short-e1-wait",
        "--sealed-erasure-poll-ms", "200", "--sealed-hold-life-ms", "8000", "--sealed-put-timeout-ms", "500", "--sealed-gc-grace-ms", "10000",
        "--sealed-remote-lease-ms", "9000", "--sealed-version-retention-secs", "0", "--sealed-erasure-check-ms", "500",
        "--sealed-erasure-ride", "false", "--sealed-erasure-batch-ms", "0", "--memtable-max-mb", "1"]
USERS = ("u1", "u2", "u3", "u4", "u5")
ROUNDS, N = int(os.environ.get("R78_ROUNDS", "12")), 20


def traces(srv):
    out = []
    for line in srv.log_text().splitlines():
        if line.startswith("fx-erase-trace "):
            try:
                out.append(json.loads(line[len("fx-erase-trace "):]))
            except ValueError:
                pass
    return out


def erasures(srv):
    st, r = srv.admin.get(f"/v1/databases/{DB}/erasures")
    assert st == 200, r
    return r["erasures"]


def test_an_erasure_arriving_in_phase_two_is_never_readable_between_parts():
    ff = tempfile.NamedTemporaryFile("w", delete=False, suffix=".faults")
    ff.close()
    bucket = make_bucket("ecj")
    env = {"FX_ERASE_TRACE": "1", "FENVIX_SEALED_COMPACTOR": os.environ.get("R78_COMPACTOR", "small"), "MONGO_TEST_FAULTS_FILE": ff.name, "CTL_FAILPOINT": "erase.after_prepare",
           "CTL_FAILPOINT_PAUSE_MS": os.environ.get("R78_PAUSE_MS", "1500")}
    srv = Server(bucket, "ecj", idle=600, extra=BASE, env=env).start()
    try:
        c = srv.client()
        c[DB].create_collection("msgs", customerKey="userId")
        # many rounds, so each customer's rows are spread over many delta runs
        for r in range(ROUNDS):
            c[DB]["msgs"].insert_many([{"_id": f"{u}-{r}-{i}", "userId": u, "t": r * 100 + i, "pad": os.urandom(1500).hex()}
                                       for u in USERS for i in range(N)])
            time.sleep(0.3)
        time.sleep(3)
        with open(ff.name, "w") as fh:
            fh.write(f"delay_get tenants/{DB}/fx/run/ {os.environ.get('R78_DELAY_MS', '700')}\n")
        n1 = c[DB]["msgs"].delete_many({"userId": "u1"}).deleted_count
        # wait for phase 2 of u1's pass
        # u2 is deleted right after phase 1's first part: the next part (listed before u2 exists) completes phase 1 here,
        # and the part after it is phase 2's first, listed after u2 is absorbed. Before the fix u2 joined there.
        wait_until(lambda: any(t.get("ev") == "batch_part" and t.get("keep_envelopes") for t in traces(srv)), 300, every=0.05,
                   what="the first part of phase 1")
        parts_p1 = sum(1 for t in traces(srv) if t.get("ev") == "batch_part")
        n2 = c[DB]["msgs"].delete_many({"userId": "u2"}).deleted_count
        t_del = time.time()
        seen, polls, stop = [], [0], threading.Event()

        def poll():
            from pymongo import MongoClient
            cr = MongoClient(srv.uri, serverSelectionTimeoutMS=10000, socketTimeoutMS=60000)
            while not stop.is_set():
                try:
                    ids = [d["_id"] for d in cr[DB]["msgs"].find({"userId": {"$in": ["u1", "u2"]}}, {"_id": 1})]
                    one = cr[DB]["msgs"].find_one({"_id": "u2-0-3"})
                except Exception as e:  # noqa: BLE001
                    seen.append(("error", str(e)[:100], time.time() - t_del))
                    continue
                polls[0] += 1
                if ids or one:
                    seen.append((len(ids), sorted(ids)[:3], bool(one), round(time.time() - t_del, 2)))
        th = threading.Thread(target=poll, daemon=True)
        th.start()
        wait_until(lambda: len(erasures(srv)) == 2 and all(e["state"] != "pending" for e in erasures(srv)), 400, every=0.5,
                   what="both erasures scrubbed")
        time.sleep(2)
        stop.set()
        th.join(timeout=60)
        with open(ff.name, "w") as fh:
            fh.write("")
        tr = traces(srv)
        batches = [t.get("erasures") for t in tr if t.get("ev") == "batch"]
        parts = [t.get("keep_envelopes") for t in tr if t.get("ev") == "batch_part"]
        passes = [t.get("batch") for t in tr if t.get("ev") == "pass"]
        others = {d["_id"] for d in c[DB]["msgs"].find({"userId": {"$in": ["u3", "u4", "u5"]}}, {"_id": 1})}
        exp = {f"{u}-{r}-{i}" for u in ("u3", "u4", "u5") for r in range(ROUNDS) for i in range(N)}
        print(f"phase-two join: deleteMany u1 {n1}, u2 {n2} (after {parts_p1} parts of u1's pass); {polls[0]} polls; readable after "
              f"u2's delete: {seen[:8]} ({len(seen)}); batch sizes per part {batches}; parts keep_envelopes {parts}; pass batch sizes {passes}; "
              f"others {len(others & exp)} of {len(exp)}")
        assert polls[0] > 20, "the reader did not poll"
        assert not [s for s in seen if s[0] != "error"], f"erased documents readable between parts: {seen[:8]}"
        assert exp <= others, "acknowledged documents of other customers missing"
        # the real pass's batch: u2 (deleted after u1's phase 1 ended) must wait for a pass of its own, never join u1's
        first = []
        for t in tr:
            if t.get("ev") in ("batch", "batch_part"):
                first.append(t)
            if t.get("ev") == "pass" and first:
                break
        assert all(t.get("erasures", 1) == 1 for t in first if t.get("ev") == "batch"), f"u2 joined u1's pass: {batches}"
        assert passes.count(1) >= 2, f"u2 was not scrubbed by a pass of its own: {passes}"
    finally:
        srv.term()
        os.unlink(ff.name)
