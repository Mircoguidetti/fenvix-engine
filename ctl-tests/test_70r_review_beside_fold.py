"""Review of fix-write-stalls-on-i4 (d660ea9): with the drop on and aged runs, the fold of the absorbed L0 tail into a new
run runs beside a merge of sorted runs. These tests try to break that rule from the product side.

A merge of sorted runs is held at its first row (FENVIX_TEST_HOLD_MERGE=1, the owner's switch) while writes go on, so the
folds run beside it and publish first. Unlike the owner's test_66, the writes during the hold also overwrite and delete
OLD documents, whose earlier versions sit in the runs the held merge reads: the fold's filter starts later (a higher
floor) than the merge's, so the fold may drop a new version while the merge keeps the old one. Every acknowledged value
must read back, and every acknowledged delete must stay deleted, through:
- test_kill9_mid_merge_with_overwrites_then_takeover: kill -9 with the merge held, another process takes over;
- test_erasure_beside_the_fold_then_kill9: a customer's erasure (deleteMany on the customer key) while the merge is held,
  scrubbed, then kill -9 and takeover; the erased customer stays erased, the rest stays whole;
- test_close_with_a_held_merge_and_folds_beside: the database closes (idle) with the merge held and folds done beside it;
  the compactor stops, the close-time pass runs on what is left, then the database is reopened and the process restarted.

Rule 8: zero acknowledged writes lost. Also: no document visible twice (find with no filter, ids counted).
"""
import os
import time

from pymongo.errors import PyMongoError

from lib import Server, make_bucket, wait_until

DB = "bf"
SEALED = ["--sealed-t-pin-ms", "1000", "--sealed-e1-wait-ms", "0", "--sealed-test-short-e1-wait"]
EXTRA = SEALED + ["--memtable-max-mb", "1"]


def stats(client):
    return client.admin.command("serverStatus")["fenvix"]["compactor"]


class Model:
    """Acknowledged state: the value each live _id must read back, and the _ids whose delete was acknowledged."""

    def __init__(self, coll):
        self.coll, self.want, self.gone, self.n = coll, {}, set(), 0

    def insert(self, i, v, pad=2000):
        self.coll.insert_one({"_id": i, "v": v, "thread_id": f"t{i % 7}", "pad": os.urandom(pad // 2).hex()})
        self.want[i] = v
        self.gone.discard(i)
        self.n += 1

    def update(self, i, v):
        r = self.coll.update_one({"_id": i}, {"$set": {"v": v}})
        self.n += 1
        if r.matched_count:
            self.want[i] = v

    def delete(self, i):
        r = self.coll.delete_one({"_id": i})
        self.n += 1
        if r.deleted_count:
            self.want.pop(i, None)
            self.gone.add(i)

    def check(self, coll):
        ids = [d["_id"] for d in coll.find({}, {"_id": 1})]
        dup = len(ids) - len(set(ids))
        got = {d["_id"]: d["v"] for d in coll.find({}, {"v": 1})}
        missing = [i for i in self.want if i not in got]
        wrong = [i for i in self.want if i in got and got[i] != self.want[i]]
        back = [i for i in self.gone if i in got]
        # point reads too (a different read path from the scan)
        sample = sorted(self.want)[:: max(1, len(self.want) // 300)] + sorted(self.gone)[:: max(1, len(self.gone) // 300 or 1)]
        pt_bad = 0
        for i in sample:
            d = coll.find_one({"_id": i}, {"v": 1})
            if (d["v"] if d else None) != self.want.get(i):
                pt_bad += 1
        assert not (dup or missing or wrong or back or pt_bad), (
            f"{dup} visible twice, {len(missing)} missing {missing[:5]}, {len(wrong)} wrong {wrong[:5]}, "
            f"{len(back)} deleted but back {back[:5]}, {pt_bad} wrong point reads (of {len(self.want)} live, {len(self.gone)} deleted)")
        return len(self.want), len(self.gone)


def churn(m, i, old_hi):
    """50 new documents, plus overwrites and deletes of OLD documents (ids below old_hi, in the sorted runs)."""
    for _ in range(50):
        m.insert(i, f"n-{i}")
        if i % 3 == 0 and old_hi:
            m.update((i * 7919) % old_hi, f"o-{i}")
        if i % 11 == 0 and old_hi:
            m.delete((i * 104729) % old_hi)
        i += 1
    return i


def until_merge_held(c, m):
    i, t0 = 0, time.time()
    while not stats(c).get("mergeHeld"):
        i = churn(m, i, i // 2)
        assert time.time() - t0 < 240, f"no merge of sorted runs was proposed: {stats(c)}"
    return i


def during_hold(c, m, i, folds=3, limit=180):
    """Churn while the merge is held, until `folds` L0 folds ran beside it; old_hi is fixed to the ids written before the
    hold, whose versions are in the runs the merge reads."""
    old_hi, t0 = i, time.time()
    b0 = stats(c)["l0JobsBesideMerge"]
    while stats(c)["l0JobsBesideMerge"] - b0 < folds:
        i = churn(m, i, old_hi)
        assert time.time() - t0 < limit, f"no L0 fold beside the held merge: {stats(c)}"
    # a few more seconds of churn so the folds' floors pass some of these overwrites
    t1 = time.time()
    while time.time() - t1 < 4:
        i = churn(m, i, old_hi)
    s = stats(c)
    assert s["mergeHeld"] == 1, s
    return i, s["l0JobsBesideMerge"] - b0


def find_key(d, key):
    if isinstance(d, dict):
        for k, v in d.items():
            if k == key:
                return v
            r = find_key(v, key)
            if r is not None:
                return r
    return None


def pass_counters(c):
    f = c.admin.command("serverStatus")["fenvix"]
    comp = find_key(f, "compaction") or {}
    return {"closePassesForPending": find_key(f, "closePassesForPending"), "passes": comp.get("passes"),
            "stopSlateAborted": find_key(f, "stopSlateAborted")}


def taken_over(c):
    wait_until(lambda: _try(lambda: c[DB]["c"].find_one({}, {"_id": 1}) is not None), timeout=90, what="the takeover")


def _try(f):
    try:
        return f()
    except PyMongoError:
        return False


def test_kill9_mid_merge_with_overwrites_then_takeover():
    bucket = make_bucket("bf9")
    srv = Server(bucket, "bf9a", idle=600, extra=EXTRA, env={"FENVIX_TEST_HOLD_MERGE": "1"}).start()
    c = srv.client()
    m = Model(c[DB]["c"])
    i = until_merge_held(c, m)
    i, beside = during_hold(c, m, i)
    n_held = m.check(c[DB]["c"])
    srv.kill9()
    srv2 = Server(bucket, "bf9b", idle=600, extra=EXTRA).start()
    c2 = srv2.client()
    taken_over(c2)
    n1 = m.check(c2[DB]["c"])
    m.coll = c2[DB]["c"]
    t0 = time.time()
    while time.time() - t0 < 20:
        i = churn(m, i, 2000)
    n2 = m.check(c2[DB]["c"])
    s2 = stats(c2)
    srv2.term()
    srv2.start()
    c3 = srv2.client()
    n3 = m.check(c3[DB]["c"])
    srv2.term()
    print(f"kill -9 mid-merge with overwrites: {m.n} writes acknowledged, {beside} folds beside the held merge; live/deleted "
          f"read back while held {n_held}, after the takeover {n1}, after 20 s more {n2}, after a restart {n3}; new owner jobs {s2.get('slateJobs', s2.get('jobs'))}")


def test_erasure_beside_the_fold_then_kill9():
    bucket = make_bucket("bfer")
    extra = EXTRA + ["--sealed-erasure-poll-ms", "200", "--sealed-erasure-batch-ms", "0"]
    srv = Server(bucket, "bfera", idle=600, extra=extra, env={"FENVIX_TEST_HOLD_MERGE": "1"}).start()
    c = srv.client()
    c[DB].create_collection("c", customerKey="thread_id")
    m = Model(c[DB]["c"])
    i = until_merge_held(c, m)
    i, beside = during_hold(c, m, i, folds=2)
    cut = i
    n = c[DB]["c"].delete_many({"thread_id": "t3"}).deleted_count
    for k in [k for k in m.want if k % 7 == 3]:
        m.want.pop(k)
        m.gone.add(k)
    i, beside2 = during_hold(c, m, i, folds=2)
    e = wait_until(lambda: (x := srv.admin.get(f"/v1/databases/{DB}/erasures")[1]["erasures"][0])["state"] in ("scrubbed", "done") and x, timeout=120, what="the scrub")
    assert stats(c)["mergeHeld"] == 1
    old_t3 = c[DB]["c"].count_documents({"thread_id": "t3", "_id": {"$lt": cut}})
    assert old_t3 == 0, f"{old_t3} erased documents readable"
    n_held = m.check(c[DB]["c"])
    srv.kill9()
    srv2 = Server(bucket, "bferb", idle=600, extra=extra).start()
    c2 = srv2.client()
    taken_over(c2)
    n1 = m.check(c2[DB]["c"])
    assert c2[DB]["c"].count_documents({"thread_id": "t3", "_id": {"$lt": cut}}) == 0
    m.coll = c2[DB]["c"]
    t0 = time.time()
    while time.time() - t0 < 15:
        i = churn(m, i, cut)
    n2 = m.check(c2[DB]["c"])
    assert c2[DB]["c"].count_documents({"thread_id": "t3", "_id": {"$lt": cut}}) == 0
    st, r = srv2.admin.get(f"/v1/databases/{DB}/erasures")
    srv2.term()
    print(f"erasure beside the fold: deleteMany {n}, {beside}+{beside2} folds beside the held merge, scrub {e['state']} rows {e.get('rows_erased')}; "
          f"read back held {n_held}, after takeover {n1}, after 15 s more {n2}; erasure on the new owner {[x['state'] for x in r.get('erasures', [])]}")


def test_close_with_a_held_merge_and_folds_beside():
    bucket = make_bucket("bfcl")
    srv = Server(bucket, "bfcl", idle=4, extra=EXTRA, env={"FENVIX_TEST_HOLD_MERGE": "1"}).start()
    c = srv.client()
    m = Model(c[DB]["c"])
    i = until_merge_held(c, m)
    i, beside = during_hold(c, m, i)
    n_held = m.check(c[DB]["c"])
    p0 = pass_counters(c)
    # idle close: the compactor stops with the merge held; the close-time pass runs on what is left
    time.sleep(25)
    log = srv.log_text()
    closes = log.count("writer_close") + log.count("closed tenant")
    p1 = pass_counters(c)
    assert closes >= 1, "the database did not close"
    assert (p1["passes"] or 0) > (p0["passes"] or 0), f"no close-time pass ran: {p0} -> {p1}"
    n1 = m.check(c[DB]["c"])         # reopens as a reader
    t0 = time.time()
    while time.time() - t0 < 10:      # and as a writer again
        i = churn(m, i, 2000)
    n2 = m.check(c[DB]["c"])
    time.sleep(25)                    # a second close, with whatever the hold left
    srv.term()
    srv.start()
    n3 = m.check(srv.client()[DB]["c"])
    log = srv.log_text()
    srv.term()
    print(f"close with a held merge: {beside} folds beside it; close lines {closes}; read back held {n_held}, after the close {n1}, "
          f"after writes {n2}, after a restart {n3}; counters before the close {p0}, after {p1}")


def test_kill9_with_a_fold_in_flight_beside_the_held_merge():
    """Neither job published: the merge is held and a fold beside it is slowed at its output PUT (fault file), then
    kill -9; the new owner finds both in SlateDB's job list. Every acknowledged write reads back."""
    import tempfile
    ff = tempfile.NamedTemporaryFile("w", delete=False, suffix=".faults")
    ff.close()
    bucket = make_bucket("bf9f")
    srv = Server(bucket, "bf9fa", idle=600, extra=EXTRA, env={"FENVIX_TEST_HOLD_MERGE": "1", "MONGO_TEST_FAULTS_FILE": ff.name}).start()
    try:
        c = srv.client()
        m = Model(c[DB]["c"])
        i = until_merge_held(c, m)
        i, beside = during_hold(c, m, i, folds=1)
        with open(ff.name, "w") as fh:
            fh.write(f"delay_put tenants/{DB}/compacted/ 20000\n")
        b0 = stats(c)["l0JobsBesideMerge"]
        t0 = time.time()
        old_hi = i
        while stats(c)["l0JobsBesideMerge"] == b0:
            i = churn(m, i, old_hi)
            assert time.time() - t0 < 120, "no fold started beside the held merge"
        t_fold = time.time()
        while time.time() - t_fold < 3:      # the fold's output PUT waits 20 s; writes go on meanwhile
            i = churn(m, i, old_hi)
        n_held = m.check(c[DB]["c"])
        srv.kill9()
        with open(ff.name, "w") as fh:
            fh.write("")
        srv2 = Server(bucket, "bf9fb", idle=600, extra=EXTRA).start()
        c2 = srv2.client()
        taken_over(c2)
        n1 = m.check(c2[DB]["c"])
        m.coll = c2[DB]["c"]
        t1 = time.time()
        while time.time() - t1 < 20:
            i = churn(m, i, 2000)
        n2 = m.check(c2[DB]["c"])
        srv2.term()
        srv2.start()
        n3 = m.check(srv2.client()[DB]["c"])
        srv2.term()
        print(f"kill -9 with a fold in flight beside the held merge: {m.n} writes acknowledged, {beside}+1 folds beside; read back held {n_held}, "
              f"after the takeover {n1}, after 20 s more {n2}, after a restart {n3}")
    finally:
        srv.term()
        os.unlink(ff.name)
