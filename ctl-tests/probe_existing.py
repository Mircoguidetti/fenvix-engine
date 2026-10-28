"""What already existed, probed by running the baseline binary (branch compat-indexes, commit 1a29f0f, built from a
clean worktree). Prints a JSON report; results-ops.md quotes it. Usage:
    MONGO_SERVER_BIN=/path/to/baseline/mongo-server python probe_existing.py"""
import json
import os
import random
import subprocess
import sys
import threading
import time

from lib import Server, make_bucket, list_prefix, fill, OUT, wait_until
from pymongo.errors import OperationFailure

BASE = os.environ["MONGO_SERVER_BIN"]
MONGODUMP = os.environ.get("MONGODUMP", "/tmp/claude-0/mongo-img/root/usr/bin/mongodump")
report = {}


def cmd(c, db, **kw):
    try:
        return c[db].command(*kw.pop("args"), **kw)
    except OperationFailure as e:
        return {"error": e.code, "codeName": e.details.get("codeName"), "errmsg": str(e)[:160]}


def srv(tag, idle=3):
    b = make_bucket(tag)
    return Server(b, tag, idle=idle, admin=False, binary=BASE).start(), b


# 1. create / list / get ------------------------------------------------------------------------------
s, b = srv("p1")
c = s.client()
fill(c, "alpha", "c", 1000)
c["alpha"].command("tenantAdmin", close=True)
rep = {}
rep["listDatabases"] = c.admin.command("listDatabases")["databases"]
rep["dbStats"] = {k: v for k, v in c["alpha"].command("dbStats").items()}
rep["tenantAdmin"] = c["alpha"].command("tenantAdmin")
fill(c, "beta", "c", 10)
rep["tenantAdmin_other_db_same_global_requests"] = c["beta"].command("tenantAdmin")["requests"] == c["alpha"].command("tenantAdmin")["requests"]
rep["create_command"] = cmd(c, "gamma", args=("create", "x"))
rep["gamma_listed_after_create"] = "gamma" in [d["name"] for d in c.list_databases()]
rep["admin_http"] = "none (no HTTP interface, no token auth)"
report["1_create_list_get"] = rep
s.term()

# 2. fork ---------------------------------------------------------------------------------------------
s, b = srv("p2", idle=60)
c = s.client()
fill(c, "src", "c", 20000)
c["src"].command("tenantAdmin", close=True)
rep = {}
for name in ("copydb", "cloneCollection", "cloneCatalog", "cloneDatabase"):
    rep[name] = cmd(c, "admin", args=(name,), **({"fromdb": "src", "todb": "dst"} if name == "copydb" else {}))
t0 = time.time()
try:
    list(c["src"]["c"].aggregate([{"$out": {"db": "dst_out", "coll": "c"}}]))
    rep["aggregate_$out_other_db"] = {"seconds": round(time.time() - t0, 2), "docs": c["dst_out"]["c"].count_documents({})}
except Exception as e:
    rep["aggregate_$out_other_db"] = {"error": str(e)[:200]}
rep["rename_collection_across_databases"] = cmd(c, "admin", args=("renameCollection",), **{"to": "dst2.c"}) if False else "not probed"
rep["copy_on_write_or_manifest_clone"] = "no: a $out/$merge copy re-reads and re-writes every document through the command layer"
report["2_fork"] = rep
s.term()

# 3. restore ------------------------------------------------------------------------------------------
s, b = srv("p3")
c = s.client()
c["d"]["c"].insert_one({"_id": 1})
rep = {n: cmd(c, "admin", args=(n,)) for n in ("restoreToPointInTime", "restore", "rollback")}
rep["checkpoints_in_the_bucket_after_writes"] = "none kept: " + str(len([k for k, _ in list_prefix(b, "tenants/d/")])) + " objects, no retention, GC min age 15 s"
report["3_restore"] = rep
s.term()

# 4. export -------------------------------------------------------------------------------------------
s, b = srv("p4", idle=60)
c = s.client()
fill(c, "ex", "c", 20000)
c["ex"]["c"].create_index([("grp", 1)])
rep = {}
if os.path.exists(MONGODUMP):
    out = f"{OUT}/probe-dump"
    t0 = time.time()
    p = subprocess.run([MONGODUMP, "--uri", s.uri, "--db", "ex", "--out", out], capture_output=True, text=True, timeout=300)
    rep["mongodump_against_the_server"] = {"returncode": p.returncode, "seconds": round(time.time() - t0, 2), "stderr_tail": p.stderr[-300:], "files": sorted(os.listdir(f"{out}/ex")) if os.path.isdir(f"{out}/ex") else None}
else:
    rep["mongodump_against_the_server"] = "mongodump binary not available"
rep["server_side_export"] = "none; mongodump works through the wire protocol (every document through find/getMore), not resumable, no snapshot"
report["4_export"] = rep
s.term()

# 5. erase: dropDatabase, and a kill -9 in the middle of it ----------------------------------------------
rep = {"iterations": []}
rnd = random.Random(4)
s, b = srv("p5", idle=60)
for i in range(12):
    c = s.client()
    for k in range(30):
        c["victim"][f"c{k}"].insert_many([{"_id": j, "pad": "y" * 200} for j in range(40)])
        if k % 5 == 4:
            c["victim"].command("tenantAdmin", close=True)
    c["victim"].command("tenantAdmin", close=True)
    docs_before = sum(c["victim"][f"c{k}"].count_documents({}) for k in range(30))
    objs_before = len(list_prefix(b, "tenants/victim/"))
    th = threading.Thread(target=lambda: _ if False else (c.drop_database("victim") if True else None))
    th.daemon = True
    def go():
        try:
            c.drop_database("victim")
        except Exception:
            pass
    th = threading.Thread(target=go)
    th.start()
    time.sleep(rnd.uniform(0.0, 0.12))
    s.kill9()
    th.join(timeout=20)
    left = list_prefix(b, "tenants/victim/")
    names = [k.split("/")[-1] for k, _ in left]
    s.proc = None
    s.start()
    c = s.client()
    outcome = {"objects_before": objs_before, "objects_left_after_kill": len(left), "has_manifest": any("/manifest/" in k for k, _ in left)}
    try:
        n = sum(c["victim"][f"c{k}"].count_documents({}) for k in range(30))
        outcome["read_after_restart"] = f"{n} of {docs_before} documents readable"
    except Exception as e:
        outcome["read_after_restart"] = "ERROR: " + str(e)[:150]
    outcome["listed_as_database"] = "victim" in [d["name"] for d in c.list_databases()]
    # the name is used again: does anything of the old database come back, or does the new write fail?
    try:
        c["victim"]["c0"].insert_one({"_id": "new"})
        outcome["after_reuse_of_the_name"] = f"{sum(c['victim'][f'c{k}'].count_documents({}) for k in range(30))} documents (1 expected)"
    except Exception as e:
        outcome["after_reuse_of_the_name"] = "ERROR: " + str(e)[:150]
    rep["iterations"].append(outcome)
    try:
        c.drop_database("victim")
    except Exception as e:
        outcome["second_drop"] = "ERROR: " + str(e)[:120]
    outcome["objects_after_second_drop"] = len(list_prefix(b, "tenants/victim/"))
s.term()
rep["summary"] = {
    "kills_that_left_some_but_not_all_objects": sum(1 for o in rep["iterations"] if 0 < o["objects_left_after_kill"] < o["objects_before"]),
    "left_objects_without_manifest (database 'exists' but cannot be opened)": sum(1 for o in rep["iterations"] if o["objects_left_after_kill"] and not o["has_manifest"]),
    "left_manifest_but_some_data_deleted (opens, data missing or errors)": sum(1 for o in rep["iterations"] if o["has_manifest"] and o["objects_left_after_kill"] < o["objects_before"]),
    "read_errors_after_restart": sum(1 for o in rep["iterations"] if str(o["read_after_restart"]).startswith("ERROR")),
    "name_reuse_after_kill_went_wrong (error or old documents back)": sum(1 for o in rep["iterations"] if not str(o["after_reuse_of_the_name"]).startswith("1 documents")),
}
report["5_erase"] = rep

# 6. keep awake ---------------------------------------------------------------------------------------
s, b = srv("p6", idle=3)
c = s.client()
c["a"]["c"].insert_one({"_id": 1})
time.sleep(6)
rep = {"idle_close_after_3s": c["a"].command("tenantAdmin")["mode"], "per_database_pin": "none: only the process-wide --idle-secs; `tenantAdmin` has no keep-awake option"}
report["6_keep_awake"] = rep
s.term()

# 7. usage --------------------------------------------------------------------------------------------
s, b = srv("p7", idle=60)
c = s.client()
fill(c, "a", "c", 500)
fill(c, "b", "c", 50)
ts = c.admin.command("tenantStats")
rep = {"tenantStats_keys": sorted(ts.keys()), "requests_are_process_wide": ts["requests"], "tenantAdmin_a": c["a"].command("tenantAdmin"), "tenantAdmin_b": c["b"].command("tenantAdmin"),
       "per_db_storage_bytes": "dbStats: dataSize from document sizes of an open database (reads every document), not object-store bytes",
       "per_day": "none", "wake_latency": "none (wakes counted, not timed)"}
report["7_usage"] = rep
s.term()

print(json.dumps(report, indent=1, default=str))
json.dump(report, open(f"{OUT}/existing.json", "w"), indent=1, default=str)
