"""After bench.py: the erasure batch, measured. Usage: post.py URI ADMIN TOKEN OUT GRACE_S [LOAD_S]
1. erased thread ids from OUT/ops.jsonl -> OUT/erased_ids.txt; the erasure records before;
2. a load on threads that were not erased (get_tuple, list(limit=10), and a save step on scratch threads) for LOAD_S
   seconds with no batch, then the batch (POST .../erasures/run) under the same load, then LOAD_S more seconds;
3. S3 GETs and bytes of the batch (serverStatus, process-wide) per erased user;
4. retire: POST .../erasures/run every 20 s until every erasure is done (after the GC grace), then the proof tool."""
import json, os, random, subprocess, sys, threading, time, urllib.request
from pymongo import MongoClient
sys.path.insert(0, "/home/user/bench-harness/bench/agent-checkpoints")
uri, admin, token, out, grace = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], int(sys.argv[5])
load_s = int(sys.argv[6]) if len(sys.argv) > 6 else 20
DB = "app_demo"

def api(method, path):
    r = urllib.request.Request(admin + path, method=method, headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"}, data=b"{}" if method == "POST" else None)
    with urllib.request.urlopen(r, timeout=3600) as f:
        return json.loads(f.read())

ops = [json.loads(l) for l in open(os.path.join(out, "ops.jsonl"))]
erased = sorted({d["thread_id"] for d in ops if d.get("op") == "delete_thread"})
users = sorted({d["user"] for d in ops if d.get("op") == "erase_user"})
written = sorted({d.get("thread_id") for d in ops if d.get("thread_id") and d.get("phase") == "write"} - set(erased) - {None})
open(os.path.join(out, "erased_ids.txt"), "w").write("\n".join(erased) + "\n")
res = {"erased_threads": len(erased), "erased_users": len(users)}
st = api("GET", f"/v1/databases/{DB}/erasures")
res["records_before"] = {"n": len(st["erasures"]), "pending": sum(e["state"] == "pending" for e in st["erasures"])}

client = MongoClient(uri)
def s3():
    return client.admin.command("serverStatus")["fenvix"]["s3"]

from langgraph.checkpoint.mongodb import MongoDBSaver
stop = threading.Event()
lat = []  # (ts, op, ms)
def load(seed):
    r = random.Random(seed)
    saver = MongoDBSaver(MongoClient(uri), db_name=DB)
    n = 0
    while not stop.is_set():
        tid = r.choice(written)
        cfg = {"configurable": {"thread_id": tid, "checkpoint_ns": ""}}
        t0 = time.perf_counter(); saver.get_tuple(cfg); lat.append((time.time(), "resume", (time.perf_counter() - t0) * 1e3))
        t0 = time.perf_counter(); list(saver.list(cfg, limit=10)); lat.append((time.time(), "list10", (time.perf_counter() - t0) * 1e3))
        scfg = {"configurable": {"thread_id": f"scratch-{seed}-{n % 50}", "checkpoint_ns": ""}}
        ck = {"v": 1, "id": f"1ef{n:013d}-{seed}", "ts": "2026-11-11T00:00:00+00:00", "channel_values": {"messages": ["x" * 500]}, "channel_versions": {}, "versions_seen": {}, "pending_sends": []}
        t0 = time.perf_counter(); saver.put(scfg, ck, {"step": n}, {}); lat.append((time.time(), "save", (time.perf_counter() - t0) * 1e3))
        n += 1
threads = [threading.Thread(target=load, args=(i,), daemon=True) for i in range(2)]
for t in threads: t.start()
time.sleep(load_s)
t_b0 = time.time(); c0 = s3()
r = api("POST", f"/v1/databases/{DB}/erasures/run")
t_b1 = time.time(); c1 = s3()
time.sleep(load_s)
stop.set()
for t in threads: t.join()
gets = (c1["get"] - c0["get"]) + (c1["getRange"] - c0["getRange"])
res["batch"] = {"seconds": round(t_b1 - t_b0, 2), "gets": gets, "get_bytes": c1["getBytes"] - c0["getBytes"], "puts": c1["put"] - c0["put"] + c1["multipartPart"] - c0["multipartPart"],
                "gets_per_user": round(gets / max(1, len(users)), 1), "mb_per_user": round((c1["getBytes"] - c0["getBytes"]) / max(1, len(users)) / 1e6, 2),
                "states": {s: sum(e["state"] == s for e in r["erasures"]) for s in ("pending", "scrubbed")},
                "batch_ids": sorted({e.get("batch") for e in r["erasures"] if e.get("batch")}), "rows_erased": max([e.get("rows_erased", 0) for e in r["erasures"]] or [0])}
def erased_readable():
    """Every erased thread read back after the batch: latest, list, and every checkpoint id the client wrote."""
    saver = MongoDBSaver(MongoClient(uri), db_name=DB)
    left = 0
    for tid in erased:
        cfg = {"configurable": {"thread_id": tid, "checkpoint_ns": ""}}
        if saver.get_tuple(cfg) is not None or list(saver.list(cfg, limit=1)):
            left += 1
    n_docs = client[DB]["checkpoints"].count_documents({"thread_id": {"$in": erased}}) + client[DB]["checkpoint_writes"].count_documents({"thread_id": {"$in": erased}})
    return {"threads_readable": left, "documents_found": n_docs}
res["after_batch_readable"] = erased_readable()

def pct(v, q):
    v = sorted(v); return round(v[min(len(v) - 1, int(q * len(v)))], 2) if v else None
for name, (a, b) in {"before_batch": (0, t_b0), "during_batch": (t_b0, t_b1), "after_batch": (t_b1, 1e12)}.items():
    res[name] = {op: {"n": len(x), "p50": pct(x, .5), "p99": pct(x, .99), "max": max(x) if x else None} for op in ("resume", "list10", "save") for x in [[m for ts, o, m in lat if o == op and a <= ts < b]]}
print(json.dumps(res, indent=1), flush=True)
# retire: after the grace
t0 = time.time()
while True:
    r = api("POST", f"/v1/databases/{DB}/erasures/run")
    done = sum(e.get("done_ms") is not None for e in r["erasures"])
    if done == len(r["erasures"]) or time.time() - t0 > grace + 600:
        break
    time.sleep(20)
res["retire"] = {"seconds_after_batch": round(time.time() - t0, 1), "done": done, "of": len(r["erasures"]), "overdue": sum(e["overdue"] for e in r["erasures"]),
                 "kept_by": sorted({k for e in r["erasures"] for x in e["retired"] for k in x["kept_by"]})}
res["after_retire_readable"] = erased_readable()
env = dict(os.environ, AWS_ACCESS_KEY_ID="minioadmin", AWS_SECRET_ACCESS_KEY="minioadmin", AWS_REGION="us-east-1")
p = subprocess.run(["/home/user/fenvix-engine/target/release/fx_erase_proof", os.environ["PROOF_ENDPOINT"], os.environ["PROOF_BUCKET"], DB, os.path.join(out, "erased_ids.txt")], capture_output=True, text=True, env=env)
res["proof"] = json.loads(p.stdout) if p.returncode == 0 else {"error": p.stderr[-2000:]}
# an erasure delete against an ordinary delete of the same documents: 40 scratch threads of 40 checkpoints each; half
# are deleted with {thread_id} (a whole customer: an erasure), half with {thread_id, checkpoint_ns: ""} (the same
# documents, an ordinary delete); interleaved, client-side milliseconds
cc = client[DB]["checkpoints"]
cmp = {"erasure": [], "ordinary": []}
for k in range(40):
    tid = f"cmp-{k:03d}"
    cc.insert_many([{"thread_id": tid, "checkpoint_ns": "", "checkpoint_id": f"{j:05d}", "checkpoint": b"x" * 2000, "metadata": {}} for j in range(40)])
for k in range(40):
    tid = f"cmp-{k:03d}"
    kind = "erasure" if k % 2 == 0 else "ordinary"
    q = {"thread_id": tid} if kind == "erasure" else {"thread_id": tid, "checkpoint_ns": ""}
    t0 = time.perf_counter(); n = cc.delete_many(q).deleted_count; cmp[kind].append((time.perf_counter() - t0) * 1e3)
    assert n == 40, n
res["delete_compare"] = {k: {"n": len(v), "p50": pct(v, .5), "p99": pct(v, .99), "max": round(max(v), 2)} for k, v in cmp.items()}
json.dump(res, open(os.path.join(out, "post.json"), "w"), indent=1)
print(json.dumps({k: res[k] for k in ("after_batch_readable", "after_retire_readable", "retire", "proof", "delete_compare")}, indent=1))
