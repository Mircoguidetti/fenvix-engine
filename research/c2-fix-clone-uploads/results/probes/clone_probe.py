import sys, json, time
sys.path.insert(0, "/home/user/fenvix-engine/ctl-tests"); sys.path.insert(0, "/tmp/claude-0/-home-user/4a1b6ada-e9c2-50ef-9c30-0069427cd458/scratchpad/probe")
from lib import Server, make_bucket, fill, wait_until
from proxy import CountingProxy
n = int(sys.argv[1]) if len(sys.argv) > 1 else 100000
bucket = make_bucket("probe")
proxy = CountingProxy()
srv = Server(bucket, "probe", idle=600, endpoint=proxy.endpoint, extra=["--usage-flush-secs", "3600", "--retention-interval-secs", "100000"]).start()
try:
    c = srv.client(); db = f"c{n}"
    fill(c, db, "c", n); c[db]["c"].create_index([("grp", 1)])
    def asleep():
        c[db].command("tenantAdmin", close=True)
        wait_until(lambda: srv.admin.get(f"/v1/databases/{db}?storage=0")[1]["state"] == "asleep", 10, what="asleep")
    asleep(); c[db]["c"].find_one({"_id": 5}); asleep(); time.sleep(1)
    def flat(d, p=""):
        o = {}
        for k, v in d.items():
            if isinstance(v, dict): o.update(flat(v, p + k + "."))
            elif isinstance(v, (int, float)) and not isinstance(v, bool): o[p + k] = v
        return o
    f0 = flat(c.admin.command("serverStatus")["fenvix"])
    proxy.reset(); time.sleep(4);
    f1 = flat(c.admin.command("serverStatus")["fenvix"]); print("CHANGED", {k: f1[k] - f0.get(k, 0) for k in f1 if f1[k] != f0.get(k)});
    proxy.reset(); time.sleep(4); s9 = proxy.snapshot(); print("second idle 4s", s9["get_bytes"])
    import subprocess
    out = []
    for _ in range(0):
        r = subprocess.run(["gdb", "-p", str(srv.proc.pid), "-batch", "-ex", "thread apply all bt 60"], capture_output=True, text=True)
        out.append(r.stdout)
    open("/tmp/claude-0/-home-user/4a1b6ada-e9c2-50ef-9c30-0069427cd458/scratchpad/probe/gdb.txt", "w").write("\n=====\n".join(out))
    proxy.reset(); time.sleep(4); s0 = proxy.snapshot(); print("idle 4s get bytes", s0["get_bytes"], "get_tier", s0["get_tier"]); print({k: v for k, v in s0["by_prefix"].items() if sum(v.values()) > 0})
    proxy.reset()
    r = srv.admin.post(f"/v1/databases/{db}/fork", {"target": f"fk{n}"})
    time.sleep(0.4)
    s = proxy.snapshot()
    print("status", r[0])
    rows = []
    for k, kinds in s["bytes_by_prefix"].items():
        g = kinds.get("GET:out", 0) + kinds.get("GET_RANGE:out", 0)
        rows.append((g, k, s["by_prefix"][k]))
    rows.sort(reverse=True)
    tot = sum(r[0] for r in rows)
    print("total get bytes", tot, "get_tier", s["get_tier"])
    for g, k, kinds in rows[:40]:
        print(g, k, kinds)
finally:
    srv.term(); proxy.stop()
