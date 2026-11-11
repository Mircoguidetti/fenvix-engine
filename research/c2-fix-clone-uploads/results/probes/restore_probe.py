import sys, time, subprocess, threading
sys.path.insert(0, "/home/user/fenvix-engine/ctl-tests")
from lib import Server, make_bucket, fill, wait_until
srv = Server(make_bucket("rp"), "rp", idle=600, extra=["--usage-flush-secs", "3600", "--retention-interval-secs", "100000", "--pin-lease-ttl-secs", "120"]).start()
try:
    c = srv.client(); a = srv.admin; db = "c100000"; n = 100000
    def asleep():
        c[db].command("tenantAdmin", close=True)
        wait_until(lambda: a.get(f"/v1/databases/{db}?storage=0")[1]["state"] == "asleep", 10, what="asleep")
    fill(c, db, "c", n); c[db]["c"].create_index([("grp", 1)]); asleep()
    c[db]["c"].find_one({"_id": 5}); asleep()
    print(a.post(f"/v1/databases/{db}/fork", {"target": "fk"})[0], a.post(f"/v1/databases/{db}/fork", {"target": "fc", "mode": "copy"})[0])
    print(a.put(f"/v1/databases/{db}/retention", {"window_secs": 3600, "interval_secs": 100000})[0])
    time.sleep(1.2); t = int(time.time() * 1000); time.sleep(1.2)
    c[db]["c"].update_many({"_id": {"$lt": 1000}}, {"$set": {"changed": 1}}); asleep(); time.sleep(1)
    info = c[db].command("fenvixSstInfo", db); fx = c.admin.command("serverStatus")["fenvix"]
    print("L0", len(info["l0"]), "runs", [len(r["ssts"]) for r in info["runs"]], "l0 blocks", sum(s["blocks"] for s in info["l0"]), "passes", fx["compaction"]["passes"], "compactor", {k: fx["compactor"][k] for k in ("started","absorbs","reseals","merges")})
    c[db].command("tenantAdmin", close=True); time.sleep(1)
    res = {}
    th = threading.Thread(target=lambda: res.update(r=a.post(f"/v1/databases/{db}/restore", {"to": t}, timeout=600)))
    t0 = time.time(); th.start(); th.join(60)
    if th.is_alive():
        print("restore still running after 60 s: sampling stacks")
        out = []
        for _ in range(15):
            out.append(subprocess.run(["gdb", "-p", str(srv.proc.pid), "-batch", "-ex", "thread apply all bt 80"], capture_output=True, text=True).stdout)
        open(sys.argv[1], "w").write("\n=====\n".join(out))
        th.join(600)
    print("restore", res.get("r", ("?",))[0], round(time.time() - t0, 1), "s", str(res.get("r"))[:300])
finally:
    srv.term()
