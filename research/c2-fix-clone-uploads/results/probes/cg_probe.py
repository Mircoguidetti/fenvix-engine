import sys, time
sys.path.insert(0, "/home/user/fenvix-engine/ctl-tests")
from lib import Server, make_bucket, fill, list_prefix
B = make_bucket("cgp"); srv = Server(B, "cgp", idle=300).start()
try:
    c = srv.client()
    st = lambda: dict(c.admin.command("serverStatus")["fenvix"]["compactor"])
    fill(c, "cg", "c", 1500); c["cg"].command("tenantAdmin", close=True)
    print("after fill", st())
    print(srv.admin.post("/v1/databases/cg/fork", {"target": "cg_f"})[0])
    for s in range(7):
        fill(c, "cg", "c", 200, start=10000 + s * 200); time.sleep(2)
        print(s, "versions", [k.rsplit('/',1)[1] for k,_ in list_prefix(B, "tenants/cg/fx/manifest/")], "passes", c.admin.command("serverStatus")["fenvix"]["compaction"]["passes"], st(), c["cg"].command("fenvixSstInfo", "cg").get("l0") and len(c["cg"].command("fenvixSstInfo", "cg")["l0"]))
        c["cg"].command("tenantAdmin", close=True); time.sleep(0.3)
finally:
    srv.term()
