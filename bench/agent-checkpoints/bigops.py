"""P-big-speed (2026-11-12): three operations whose cost depends on the size of the data, on a line left up by
`lines.py run LINE --big --keep` after its measured rounds (the full `big` data set loaded).

  bigops.py fenvix OUT_DIR      ours (fenvix-log): copy = fork (control layer), restore = point-in-time restore of the
                                live database, second server = a new server process on the same bucket until it answers
  bigops.py mongo  OUT_DIR      mongod (mongo-rs3): copy = mongodump | mongorestore into a new database, restore =
                                mongorestore --drop of a dump taken before the change, second server = a new replica
                                set member (initial sync) until it answers a read

Every operation is timed from its start until the copy (or the restored database, or the new server) answers a read
whose answer is checked against the source. Each has a cap (CAP_S); past it the operation is reported as not finished.
Results: OUT_DIR/bigops.json. Run with nothing else on the host (the timings are wall clock).

Disk: the mongod line holds about 22 GB (three copies and oplogs) on this ~30 GB sandbox, so a copy into the same
replica set (three more copies) does not fit. Before its operations the two secondaries are removed from the replica
set and their data deleted (the primary keeps serving alone, w:1); the second-server operation then adds a new member
back with an empty data directory (initial sync). Ours stores everything in MinIO; a fork copies no data.
"""
from __future__ import annotations

import json
import os
import random
import subprocess
import sys
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import lines  # noqa: E402

CAP_S = int(os.environ.get("BIGOPS_CAP_S", "3600"))
DB = "app_demo"


def now():
    return time.time()


def sample_threads(c, n=20, seed=5):
    ids = sorted(c[DB].checkpoints.distinct("thread_id"))
    return random.Random(seed).sample(ids, min(n, len(ids)))


def newest(c, db, t):
    d = next(iter(c[db].checkpoints.find({"thread_id": t, "checkpoint_ns": ""}, sort=[("checkpoint_id", -1)], limit=1)), None)
    return d and (d["checkpoint_id"], len(d["checkpoint"]))


# ---- ours ------------------------------------------------------------------------------------------------------------

def admin(method, path, body=None, timeout=CAP_S):
    url = f"http://{lines.IP['cook']}:8080{path}"
    req = urllib.request.Request(url, method=method, data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Authorization": f"Bearer {os.environ['BENCH_PW']}", "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def fenvix_client(name="cook"):
    from pymongo import MongoClient
    return MongoClient(lines.uri_of("fenvix-log").replace(lines.IP["cook"], lines.IP[name]) if name != "cook" else lines.uri_of("fenvix-log"),
                       serverSelectionTimeoutMS=CAP_S * 1000, socketTimeoutMS=CAP_S * 1000)


def wait_op_done(db, t0):
    while now() - t0 < CAP_S:
        st, r = admin("GET", f"/v1/databases/{db}")
        if st == 200 and not r.get("operation"):
            return r
        time.sleep(0.5)
    return None


def saver_writes(uri, threads, turns_each):
    """New turns on some threads with the saver itself (graph_runner), so a restore has something to undo."""
    import generator
    import graph_runner
    import bigmix
    from langgraph.checkpoint.mongodb import MongoDBSaver
    from pymongo import MongoClient
    plan, _ = generator.plan(42, "big")
    th = {t.thread_id: t for t in plan}
    c = MongoClient(uri)
    ts = graph_runner.TimedSaver(MongoDBSaver(c, db_name=DB), lambda row: None, expected=None)
    script = graph_runner.Script()
    graph = graph_runner.build_graph(ts, script)
    for t in threads:
        base = th[t]
        n0 = c[DB].checkpoints.count_documents({"thread_id": t})
        ext = generator.extend_thread(seed=42, th=base, extra=turns_each + 50)
        for j in range(turns_each):
            graph_runner.run_turn(graph, script, 42, ext, base.turns + 40 + j)
    c.close()


def ours(out):
    res = {"line": "fenvix-log", "delay_ms_now": open(os.path.join(lines.CTL_DIR, "delay_ms")).read().strip() if os.path.exists(os.path.join(lines.CTL_DIR, "delay_ms")) else None}
    c = fenvix_client()
    ths = sample_threads(c)
    want = {t: newest(c, DB, t) for t in ths}
    st = c[DB].command("dbStats")
    res["source"] = {"threads_checked": len(ths), "dataSize": st.get("dataSize"), "storageSize": st.get("storageSize")}
    # 1. copy: a fork of the whole database, until the fork answers every sampled thread like its source
    t0 = now()
    stc, r = admin("POST", f"/v1/databases/{DB}/fork", {"target": "app_copy"})
    wait_op_done("app_copy", t0)
    ok = all(newest(c, "app_copy", t) == want[t] for t in ths)
    res["copy"] = {"api": [stc, r], "seconds": round(now() - t0, 2), "answers_like_source": ok}
    print("copy", res["copy"], flush=True)
    # 2. restore to a past time: retention on (a restore point now), new turns, T, more turns, restore to T
    sta, ra = admin("PUT", f"/v1/databases/{DB}/retention", {"window_secs": 7200, "interval_secs": 30})
    t_on = now()
    for _ in range(120):
        sp, rp = admin("GET", f"/v1/databases/{DB}/restore-points")
        if sp == 200 and rp.get("points"):
            break
        time.sleep(1)
    w1 = ths[:5]
    saver_writes(lines.uri_of("fenvix-log"), w1, 2)
    # with the 3-zone log on, writes reach the store as WAL objects in blocks every --local-log-upload-secs (30 s), and
    # the restore places a WAL object in time by its object-store timestamp: T is taken once the writes before it are
    # uploaded (pilot: with T 1.5 s after them, the restore also removed them; reported as a finding)
    time.sleep(40)
    t_target = int(now() * 1000)
    at_t = {t: newest(c, DB, t) for t in w1}
    time.sleep(1.5)
    saver_writes(lines.uri_of("fenvix-log"), w1, 2)
    after = {t: newest(c, DB, t) for t in w1}
    t0 = now()
    strs, rr = admin("POST", f"/v1/databases/{DB}/restore", {"to": t_target})
    wait_op_done(DB, t0)
    back = {t: newest(c, DB, t) for t in w1}
    res["restore"] = {"retention_on": [sta, ra], "points": rp, "api": [strs, rr], "seconds": round(now() - t0, 2),
                      "restored_like_target": back == at_t, "changed_since_target": at_t != after}
    print("restore", res["restore"], flush=True)
    # 3. a second server on the same data: the cook stops cleanly (lease released), a NEW server process with no local
    #    state starts in its place (fresh container), and is timed until it answers the sampled threads like before
    want2 = {t: newest(c, DB, t) for t in ths}
    c.close()
    # the warm spare would take the database over when the cook stops: it is stopped first (the new server is alone)
    lines.sh(["docker", "stop", "-t", "60", "spare"], check=False, timeout=200)
    lines.sh(["docker", "stop", "-t", "60", "cook"], check=False, timeout=200)
    lines.sh(["docker", "rm", "-f", "cook"], check=False)
    t0 = now()
    lines.run_ours("cook", lines.server_cmd("cook", True))
    c2 = None
    answered = False
    while now() - t0 < CAP_S:
        try:
            c2 = fenvix_client()
            answered = all(newest(c2, DB, t) == want2[t] for t in ths[:1])
            if answered:
                break
        except Exception:  # noqa: BLE001 - not up yet
            time.sleep(0.2)
    first_answer = round(now() - t0, 2)
    all_ok = all(newest(c2, DB, t) == want2[t] for t in ths) if answered else False
    res["second_server"] = {"seconds_to_first_answer": first_answer, "all_sampled_threads_right": all_ok,
                            "seconds_to_all_sampled": round(now() - t0, 2)}
    print("second server", res["second_server"], flush=True)
    json.dump(res, open(os.path.join(out, "bigops.json"), "w"), indent=1, default=str)


# ---- mongod ----------------------------------------------------------------------------------------------------------

def tools(cmd: str, timeout=CAP_S):
    """mongodump / mongorestore in a tools container on the bench network (not capped, like our client). Past the cap
    the container is removed and the step is reported as not finished."""
    t0 = now()
    try:
        r = subprocess.run(["docker", "run", "--rm", "--name", "bigops-tools", "--network", lines.NET, "-v", "/tmp/claude-0/bigops:/dump", "mongo:7.0", "bash", "-c", cmd],
                           capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        subprocess.run(["docker", "rm", "-f", "bigops-tools"], capture_output=True)
        return {"rc": None, "seconds": round(now() - t0, 2), "timed_out": True, "tail": ""}
    return {"rc": r.returncode, "seconds": round(now() - t0, 2), "tail": (r.stdout + r.stderr)[-600:]}


def mongo(out):
    from pymongo import MongoClient
    res = {"line": "mongo-rs3"}
    a = lines.mongo_client("mongo-a")
    st = a.admin.command("replSetGetStatus")
    prim = next(m["name"].split(":")[0] for m in st["members"] if m["stateStr"] == "PRIMARY")
    p = lines.mongo_client(prim)
    ths = sample_threads(p)
    want = {t: newest(p, DB, t) for t in ths}
    ds = p[DB].command("dbStats")
    res["source"] = {"primary": prim, "dataSize": ds.get("dataSize"), "storageSize": ds.get("storageSize"), "indexSize": ds.get("indexSize")}
    # disk: the two secondaries leave the replica set and their data is deleted (see the module doc)
    secs = [m["name"].split(":")[0] for m in st["members"] if m["stateStr"] != "PRIMARY"]
    cfg = p.admin.command("replSetGetConfig")["config"]
    cfg["members"] = [m for m in cfg["members"] if m["host"].split(":")[0] == prim]
    cfg["version"] += 1
    p.admin.command("replSetReconfig", cfg, force=True)
    for s in secs:
        lines.sh(["docker", "rm", "-f", s], check=False)
    res["secondaries_removed"] = secs
    os.makedirs("/tmp/claude-0/bigops", exist_ok=True)
    # 1. copy: mongodump | mongorestore into app_copy (no file in between), until it answers like its source
    r = tools(f"mongodump --host {prim} --db {DB} --archive | mongorestore --host {prim} --archive --nsFrom '{DB}.*' --nsTo 'app_copy.*'")
    ok = all(newest(p, "app_copy", t) == want[t] for t in ths)
    res["copy"] = r | {"answers_like_source": ok}
    print("copy", res["copy"], flush=True)
    p.drop_database("app_copy")
    # 2. restore to a past time: the dump taken now is the state at T; new turns; then mongorestore --drop of that dump
    #    into app_demo (its oplog replay up to T would come on top; not needed here: the dump is exactly T)
    dump = tools(f"mongodump --host {prim} --db {DB} --gzip --archive=/dump/app_demo.archive.gz")
    w1 = ths[:5]
    at_t = {t: newest(p, DB, t) for t in w1}
    saver_writes(f"mongodb://{lines.IP[prim]}:27017/?directConnection=true", w1, 2)
    after = {t: newest(p, DB, t) for t in w1}
    r = tools(f"mongorestore --host {prim} --drop --gzip --archive=/dump/app_demo.archive.gz")
    back = {t: newest(p, DB, t) for t in w1}
    res["restore"] = {"dump": dump, "restore": r, "seconds": r["seconds"], "restored_like_target": back == at_t, "changed_since_target": at_t != after}
    print("restore", res["restore"], flush=True)
    try:
        os.remove("/tmp/claude-0/bigops/app_demo.archive.gz")
    except OSError:
        pass
    # 3. a second server on the same data: a new member with an empty data directory joins and syncs, timed until it
    #    answers the sampled threads (secondary read) like the primary
    want2 = {t: newest(p, DB, t) for t in ths}
    new = secs[0]
    t0 = now()
    lines.run_container(new, "mongo:7.0", [], ["mongod", "--replSet", "rs0", "--bind_ip_all"] + lines.MONGO_CACHE())
    lines.wait_until(f"{new} up", lambda: lines.mongo_client(new).admin.command("ping"), 300)
    cfg = p.admin.command("replSetGetConfig")["config"]
    cfg["members"].append({"_id": max(m["_id"] for m in cfg["members"]) + 1, "host": f"{new}:27017"})
    cfg["version"] += 1
    p.admin.command("replSetReconfig", cfg)
    answered = False
    while now() - t0 < CAP_S:
        try:
            s = lines.mongo_client(new, readPreference="secondaryPreferred")
            stt = s.admin.command("replSetGetStatus")
            me = next(m for m in stt["members"] if m.get("self"))
            if me["stateStr"] == "SECONDARY" and all(newest(s, DB, t) == want2[t] for t in ths):
                answered = True
                break
        except Exception:  # noqa: BLE001
            pass
        time.sleep(1)
    res["second_server"] = {"seconds_to_answer": round(now() - t0, 2), "answered": answered, "how": "new replica set member, initial sync"}
    print("second server", res["second_server"], flush=True)
    json.dump(res, open(os.path.join(out, "bigops.json"), "w"), indent=1, default=str)


if __name__ == "__main__":
    which, out = sys.argv[1], sys.argv[2]
    os.makedirs(out, exist_ok=True)
    lines.set_big("1g")
    {"fenvix": ours, "mongo": mongo}[which](out)
