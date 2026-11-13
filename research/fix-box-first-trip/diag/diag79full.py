"""Per-operation counters for test_79's two failing shapes (the first-request loop and the save sequence)."""
import sys, json, time
sys.path.insert(0, "/home/user/fenvix-engine/ctl-tests")
import test_79_big_speed as T
from lib import Server, make_bucket
T0 = time.time()
import os
WIDE = os.environ.get("WIDE") == "1"
KEYS = ("waiter.trips", "s3.get", "s3.getRange", "s3.list", "s3.head", "s3.put")

def flat(d, p=""):
    out = {}
    for k, v in d.items():
        if isinstance(v, dict): out.update(flat(v, p + k + "."))
        elif isinstance(v, (int, float)) and not isinstance(v, bool): out[p + k] = v
    return out

class M:
    def __init__(s, c): s.c = c
    def __call__(s, label, fn):
        a = flat(T.fx(s.c)); ua = time.time(); r = fn(); b = flat(T.fx(s.c))
        d = {k: b[k] - a.get(k, 0) for k in b if b[k] != a.get(k, 0) and (k in KEYS or "enewal" in k or "PastRenewal" in k or k.startswith("customerBox.w") or "fences" in k or (WIDE and not any(x in k.lower() for x in ("micros","ms","bytes","uptime","peak","now","depth","inflight"))))}
        print("%7.3f u0=%.1f u1=%.1f %-28s %s" % (time.time() - T0, ua*1000, time.time()*1000, label, json.dumps(d, sort_keys=True)))
        return r

bucket = make_bucket("bigspeed")
s = Server(bucket, "bigspeed", idle=600, env=T.SIM).start()
c = s.client(); want = T.load(c.app); c.close(); open("/tmp/claude-0/runs/serverlog.txt","w").write(s.log_text()); s.term()
mode = sys.argv[1]
s = Server(bucket, "bigspeed", idle=600, env=dict(T.SIM, FENVIX_SIM_TRACE="1", FX_WAITS=os.environ.get("FXW","0"), FX_WAITS_GETS="1")).start()
c = s.client(); db = c.app; m = M(c)
T.warm_up_db(c, db)
if mode == "first":
    for i in range(0, T.THREADS - 1, 2):
        t = T.tid(i)
        m(t + " FIRST", lambda: T.resume(db, t))
        m("  hist", lambda: list(db.checkpoints.find({"thread_id": t, "checkpoint_ns": ""}, sort=[("checkpoint_id", -1)])))
        m("  last10", lambda: list(db.checkpoints.find({"thread_id": t, "checkpoint_ns": ""}, sort=[("checkpoint_id", -1)], limit=10)))
        old = sorted(cc for (tt, cc) in want if tt == t)[3]
        m("  tt", lambda: db.checkpoints.find_one({"thread_id": t, "checkpoint_ns": "", "checkpoint_id": old}))
        m("  writes", lambda: list(db.checkpoint_writes.find({"thread_id": t, "checkpoint_ns": "", "checkpoint_id": old})))
        m("  resume2", lambda: T.resume(db, t))
else:
    other = T.tid(T.THREADS - 1)
    m("other ckpt", lambda: db.checkpoints.update_one({"thread_id": other, "checkpoint_ns": "", "checkpoint_id": "1ef99999-other"}, {"$set": {"checkpoint": "x"}}, upsert=True))
    m("other write", lambda: db.checkpoint_writes.update_one({"thread_id": other, "checkpoint_ns": "", "checkpoint_id": "1ef99999-other", "task_id": "t", "task_path": "", "idx": 0}, {"$setOnInsert": {"value": "o"}}, upsert=True))
    t = T.tid(5)
    m("save FIRST", lambda: db.checkpoints.update_one({"thread_id": t, "checkpoint_ns": "", "checkpoint_id": "1ef99999-new"}, {"$set": {"type": "json", "checkpoint": "new", "metadata": {}}}, upsert=True))
    f = {"thread_id": t, "checkpoint_ns": "", "checkpoint_id": "1ef99999-new", "task_id": "t", "task_path": "", "idx": 0}
    m("save NEXT 1", lambda: db.checkpoint_writes.update_one(f, {"$setOnInsert": {"value": "first"}}, upsert=True))
    m("save NEXT 2", lambda: db.checkpoint_writes.update_one(f, {"$setOnInsert": {"value": "second"}}, upsert=True))
open("/tmp/claude-0/runs/serverlog.txt","w").write(s.log_text()); s.term()
