import sys, json
sys.path.insert(0, "/home/user/fenvix-engine/ctl-tests")
import test_79_big_speed as T, time
T0=time.time()
from lib import Server, make_bucket

def flat(d, p=""):
    out = {}
    for k, v in d.items():
        if isinstance(v, dict): out.update(flat(v, p + k + "."))
        elif isinstance(v, (int, float)) and not isinstance(v, bool): out[p + k] = v
    return out

bucket = make_bucket("bigspeed")
s = Server(bucket, "bigspeed", idle=600, env=T.SIM).start()
c = s.client(); want = T.load(c.app); c.close(); print([l for l in s.log_text().splitlines() if "TRACE_REFRESH" in l]); s.term()
extra = sys.argv[1:]; T.SIM = dict(T.SIM, FENVIX_TRACE_REFRESH="1")
s = Server(bucket, "bigspeed", idle=600, env=T.SIM, extra=extra).start()
c = s.client(); db = c.app
print("WARMUP start t=%.3f" % (time.time()-T0)); T.warm_up_db(c, db); print("WARMUP end t=%.3f" % (time.time()-T0))
for i in range(0, T.THREADS - 1, 2):
    t = T.tid(i)
    a = flat(T.fx(c)); print("REQ", t, "t=%.3f" % (time.time()-T0))
    T.resume(db, t)
    b = flat(T.fx(c))
    d = {k: b[k] - a.get(k, 0) for k in b if b[k] != a.get(k, 0) and (k.startswith("waits") or "enewal" in k or k in ("waiter.trips","s3.get","s3.list","s3.getRange"))}
    print(t, "trips", d.get("waiter.trips"), json.dumps(d, sort_keys=True))
print([l for l in s.log_text().splitlines() if "TRACE_REFRESH" in l]); s.term()
