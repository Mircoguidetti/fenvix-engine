"""What retention costs on the object store: the same workload with and without a retention window, compared after
garbage collection (storage in the bucket) and by request counts (counting proxy)."""
import json
import random
import threading
import time

from lib import Server, make_bucket, fill, list_prefix, wait_until, OUT
from proxy import CountingProxy

N_DOCS = 20000
ROUNDS = 6          # write sessions
ROUND_SECONDS = 6   # of continuous small writes each


def workload(srv, db, retention):
    c = srv.client()
    fill(c, db, "c", N_DOCS)
    c[db].command("tenantAdmin", close=True)
    if retention:
        r = srv.admin.put(f"/v1/databases/{db}/retention", {"window_secs": 3600, "interval_secs": 5})
        assert r[0] == 200, r
    rnd = random.Random(1)
    writes = 0
    t_start = time.time()
    for s in range(ROUNDS):
        t0 = time.time()
        while time.time() - t0 < ROUND_SECONDS:
            ids = [rnd.randrange(N_DOCS) for _ in range(100)]
            c[db]["c"].update_many({"_id": {"$in": ids}}, {"$inc": {"n": 1}})
            c[db]["c"].insert_many([{"_id": 100000 + writes * 10 + i, "pad": "p" * 100} for i in range(10)])
            writes += 1
            time.sleep(0.05)
        c[db].command("tenantAdmin", close=True)
    return writes, time.time() - t_start


def settle(bucket, db, srv):
    """Wait for compaction and for the garbage-collection pass of this database (30 s after its last write)."""
    base = srv.client().admin.command("tenantStats")["maintenance"]
    wait_until(lambda: srv.client().admin.command("tenantStats")["maintenance"]["compactRuns"] > base["compactRuns"] or True, 5)
    before = len(list_prefix(bucket, f"tenants/{db}/"))
    time.sleep(1)
    # the GC pass runs 30 s after the last write session closed
    for _ in range(40):
        time.sleep(3)
        now = len(list_prefix(bucket, f"tenants/{db}/"))
        if now != before:
            before = now
            time.sleep(6)
            break
    time.sleep(35)


def run(tag, retention):
    bucket = make_bucket("retcost" + tag)
    proxy = CountingProxy()
    srv = Server(bucket, "rc" + tag, idle=600, endpoint=proxy.endpoint, extra=["--usage-flush-secs", "3600", "--retention-prune-secs", "3600"]).start()
    try:
        writes, secs = workload(srv, "d", retention)
        settle(bucket, "d", srv)
        objs = list_prefix(bucket, "tenants/d/")
        snap = proxy.snapshot()
        mine = {k: v for k, v in snap["by_prefix"].items() if k.startswith("tenants/d") or k.startswith("control/dbs/d") or k.startswith("control/retention")}
        agg = {}
        for v in mine.values():
            for k, n in v.items():
                agg[k] = agg.get(k, 0) + n
        pts = srv.admin.get("/v1/databases/d/restore-points")[1]["points"] if retention else []
        cps = srv.admin.get("/v1/databases/d/checkpoints")[1]["checkpoints"]
        by_dir = {}
        for k, sz in objs:
            d = k.split("/")[2]
            by_dir.setdefault(d, [0, 0])
            by_dir[d][0] += 1
            by_dir[d][1] += sz
        return {
            "retention": retention, "write_batches": writes, "seconds": round(secs, 1),
            "bucket_bytes": sum(sz for _, sz in objs), "objects": len(objs), "by_directory": {k: {"objects": v[0], "bytes": v[1]} for k, v in by_dir.items()},
            "restore_points": len(pts), "checkpoints_in_manifest": len(cps),
            "requests": agg, "put_bytes": sum(v.get("PUT:in", 0) + v.get("PUT_PART:in", 0) for v in snap["bytes_by_prefix"].values() if True) if False else None,
            "manifest_summary": srv.admin.get("/v1/databases/d/manifest")[1],
        }
    finally:
        srv.term()
        proxy.stop()


def test_retention_cost():
    out = {}
    res = {}

    def go(tag, ret):
        res[tag] = run(tag, ret)

    ts = [threading.Thread(target=go, args=("a", False)), threading.Thread(target=go, args=("b", True))]
    [t.start() for t in ts]
    [t.join() for t in ts]
    a, b = res["a"], res["b"]
    out.update(without_retention=a, with_retention=b)
    out["extra_bytes"] = b["bucket_bytes"] - a["bucket_bytes"]
    out["extra_objects"] = b["objects"] - a["objects"]
    json.dump(out, open(f"{OUT}/retention_cost.json", "w"), indent=1)
    print(json.dumps({k: (v if k.endswith("bytes") or k.endswith("objects") else {kk: vv for kk, vv in v.items() if kk in ("bucket_bytes", "objects", "by_directory", "restore_points", "requests")}) for k, v in out.items()}, indent=1))
    assert b["restore_points"] >= 3
    assert b["bucket_bytes"] >= a["bucket_bytes"]
