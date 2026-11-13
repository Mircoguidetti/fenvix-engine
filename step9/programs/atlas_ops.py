"""Step 9 · the same three operations on Atlas, at the Atlas size (founder's decision, 2026-11-13: where we are stronger).
Ours: data_ops.py (copy = fork, restore to a past time) and takeovers.py (a second server answering). Atlas, through its
Admin API v2 (https://cloud.mongodb.com/api/atlas/v2, digest auth with a programmatic API key; DECLARED by MongoDB's API
documentation, never called from this sandbox: a fake API in the tests is all that ran here):

  failover  POST /groups/G/clusters/C/restartPrimaries ("Test failover": the primary steps down, a secondary is elected).
            A writer inserts one document every 50 ms with w: majority from before the call; timed: the longest stretch
            with no acknowledged write after the call (the time until a second server answers writes), and the time
            until the primary in `hello` is another member. Ours is the takeover program's kill -> first acknowledged
            write on the other process; Atlas's step-down is graceful (not a kill): said beside the number.
  copy      restore to another cluster: an on-demand snapshot (POST .../backup/snapshots, waited until completed), then
            POST .../backup/restoreJobs {deliveryType automated, snapshotId, targetClusterName}; timed from the restore
            call until the target answers every sampled customer's newest message like the source. The target cluster
            is the founder's (--target-cluster, made empty in the console; or --create-target makes it with the source's
            own specification and deletes it at the end: its creation is timed apart).
  restore   point in time (needs continuous cloud backup on the cluster: pitEnabled): two messages on K customers; a
            pause; T (whole second); two more; POST .../backup/restoreJobs {deliveryType pointInTime,
            pointInTimeUTCSeconds T, targetClusterName = the same cluster}; timed from the call until the job finished
            and the cluster answers; checked: every sampled customer's newest message is the one at T.
Order: failover, copy, restore (the restore rewrites the cluster; it comes last). Each has a cap (--cap-s).

Usage: atlas_ops.py OUT_DIR --uri URI --group G --cluster C --key-file F (public:private, one line) (--states DIR | --db N)
                    [--target-cluster NAME | --create-target] [--ops failover,copy,restore] [--cap-s 5400]
                    [--api https://cloud.mongodb.com/api/atlas/v2]
  -> OUT_DIR/atlas-ops-summary.json, atlas-ops.txt; exit 1 when an operation failed or answered wrong."""
import argparse
import datetime as dt
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import data_ops  # noqa: E402  (newest, write_some: the same checks as ours)
import first_next as fn  # noqa: E402

ACCEPT = "application/vnd.atlas.2023-01-01+json"


class Api:
    def __init__(self, base, public, private, timeout=60):
        self.base = base.rstrip("/")
        pm = urllib.request.HTTPPasswordMgrWithDefaultRealm()
        pm.add_password(None, self.base, public, private)
        self.op = urllib.request.build_opener(urllib.request.HTTPDigestAuthHandler(pm))
        self.timeout = timeout

    def call(self, method, path, body=None):
        req = urllib.request.Request(self.base + path, method=method, data=json.dumps(body).encode() if body is not None else None,
                                     headers={"Accept": ACCEPT, "Content-Type": ACCEPT})
        try:
            with self.op.open(req, timeout=self.timeout) as r:
                return r.status, json.loads(r.read() or b"{}")
        except urllib.error.HTTPError as e:
            try:
                return e.code, json.loads(e.read() or b"{}")
            except Exception:  # noqa: BLE001
                return e.code, {}


def cluster_path(g, c):
    return f"/groups/{g}/clusters/{c}"


def wait_until(fn_, cap_s, poll_s=10, sleep=time.sleep, now=time.monotonic):
    t0 = now()
    while now() - t0 < cap_s:
        v = fn_()
        if v:
            return v
        sleep(poll_s)
    return None


def primary_of(client):
    try:
        return client.admin.command("hello").get("primary")
    except Exception:  # noqa: BLE001
        return None


def op_failover(client, api, g, c, db, cap_s, sleep=time.sleep, now=time.monotonic):
    from pymongo.write_concern import WriteConcern
    coll = client[db]["s9_failover"].with_options(write_concern=WriteConcern(w="majority", wtimeout=5000))
    acks, stop = [], threading.Event()

    def writer():
        i = 0
        while not stop.is_set():
            try:
                coll.insert_one({"i": i, "t": dt.datetime.now(dt.timezone.utc)})
                acks.append(now())
            except Exception:  # noqa: BLE001 - the step-down: retried until another member answers
                pass
            i += 1
            sleep(0.05)
    p0 = primary_of(client)
    th = threading.Thread(target=writer, daemon=True)
    th.start()
    sleep(5)
    t0 = now()
    st, r = api.call("POST", cluster_path(g, c) + "/restartPrimaries")
    changed = None
    if st in (200, 202):
        changed = wait_until(lambda: (primary_of(client) not in (None, p0)) and now(), cap_s, poll_s=0.5, sleep=sleep, now=now)
        # writes after the new primary: 30 s more of the writer, so the stretch after the change is complete
        sleep(30)
    stop.set()
    th.join(timeout=10)
    after = [x for x in acks if x >= t0]
    gaps = [b - a for a, b in zip([t0] + after, after)]
    gap = max(gaps) if gaps else None
    try:
        client[db].drop_collection("s9_failover")
    except Exception:  # noqa: BLE001
        pass
    ok = st in (200, 202) and changed is not None and gap is not None
    return {"ok": ok, "api": [st, r], "primary_before": p0, "primary_after": primary_of(client),
            "seconds_to_new_primary": round(changed - t0, 2) if changed else None,
            "longest_gap_between_acknowledged_writes_s": round(gap, 2) if gap is not None else None,
            "acknowledged_writes_after_call": len(after), "kind": "graceful step-down (Atlas Test Failover)"}


def snapshot(api, g, c, cap_s, sleep, now):
    st, r = api.call("POST", cluster_path(g, c) + "/backup/snapshots", {"description": "s9 copy", "retentionInDays": 1})
    sid = r.get("id")
    if st not in (200, 201, 202) or not sid:
        return None, [st, r], 0
    t0 = now()
    done = wait_until(lambda: api.call("GET", cluster_path(g, c) + f"/backup/snapshots/{sid}")[1].get("status") == "completed",
                      cap_s, sleep=sleep, now=now)
    return (sid if done else None), [st, r], round(now() - t0, 1)


def restore_job(api, g, c, body, cap_s, sleep, now):
    st, r = api.call("POST", cluster_path(g, c) + "/backup/restoreJobs", body)
    jid = r.get("id")
    if st not in (200, 201, 202) or not jid:
        return False, [st, r]

    def finished():
        _, j = api.call("GET", cluster_path(g, c) + f"/backup/restoreJobs/{jid}")
        if j.get("failed") or j.get("cancelled"):
            return "failed"
        return "done" if j.get("finishedAt") else None
    res = wait_until(finished, cap_s, sleep=sleep, now=now)
    return res == "done", [st, r]


def create_target(api, g, src, name, cap_s, sleep, now):
    _, spec = api.call("GET", cluster_path(g, src))
    body = {k: spec[k] for k in ("clusterType", "replicationSpecs", "mongoDBMajorVersion", "backupEnabled") if k in spec}
    body["name"] = name
    t0 = now()
    st, r = api.call("POST", f"/groups/{g}/clusters", body)
    ok = st in (200, 201, 202) and wait_until(
        lambda: api.call("GET", cluster_path(g, name))[1].get("stateName") == "IDLE", cap_s, sleep=sleep, now=now)
    return bool(ok), round(now() - t0, 1), [st, r]


def op_copy(client, target_client, api, g, c, target, db, customers, cap_s, sleep=time.sleep, now=time.monotonic):
    want = {x: data_ops.newest(client, db, x) for x in customers}
    sid, sapi, snap_s = snapshot(api, g, c, cap_s, sleep, now)
    if not sid:
        return {"ok": False, "snapshot_api": sapi, "snapshot_s": snap_s}
    t0 = now()
    done, rapi = restore_job(api, g, c, {"deliveryType": "automated", "snapshotId": sid, "targetClusterName": target,
                                          "targetGroupId": g}, cap_s, sleep, now)
    secs, right = data_ops.wait_answers(target_client, db, want, t0, cap_s, sleep, now) if done else (round(now() - t0, 2), False)
    return {"ok": bool(done and right), "snapshot_s": snap_s, "snapshot_api": sapi, "restore_api": rapi, "seconds": secs,
            "answers_like_source": right, "customers_checked": len(want), "target_cluster": target}


def op_restore(client, api, g, c, db, customers, cap_s, sleep=time.sleep, now=time.monotonic, wall=None):
    wall = wall or time.time
    data_ops.write_some(client, db, customers, 2, "before-T")
    sleep(10)
    t_target = int(wall()) + 1
    while wall() < t_target + 1:
        sleep(0.2)
    at_t = {x: data_ops.newest(client, db, x) for x in customers}
    data_ops.write_some(client, db, customers, 2, "after-T")
    after = {x: data_ops.newest(client, db, x) for x in customers}
    t0 = now()
    done, rapi = restore_job(api, g, c, {"deliveryType": "pointInTime", "pointInTimeUTCSeconds": t_target,
                                          "targetClusterName": c, "targetGroupId": g}, cap_s, sleep, now)
    secs, right = data_ops.wait_answers(client, db, at_t, t0, cap_s, sleep, now) if done else (round(now() - t0, 2), False)
    return {"ok": bool(done and right and at_t != after), "api": rapi, "seconds": secs, "restored_like_target": right,
            "changed_since_target": at_t != after, "customers_checked": len(at_t), "target_utc_s": t_target}


def main(argv=None, sleep=time.sleep, now=time.monotonic, client_factory=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("out")
    ap.add_argument("--uri", required=True)
    ap.add_argument("--target-uri", default="", help="the target cluster's URI (copy); default: --uri with the host replaced is not guessed")
    ap.add_argument("--group", required=True)
    ap.add_argument("--cluster", required=True)
    ap.add_argument("--key-file", required=True)
    ap.add_argument("--states", default="")
    ap.add_argument("--db", default="")
    ap.add_argument("--target-cluster", default="")
    ap.add_argument("--create-target", action="store_true")
    ap.add_argument("--ops", default="failover,copy,restore")
    ap.add_argument("--customers", type=int, default=20)
    ap.add_argument("--write-customers", type=int, default=5)
    ap.add_argument("--cap-s", type=float, default=5400.0)
    ap.add_argument("--api", default="https://cloud.mongodb.com/api/atlas/v2")
    ap.add_argument("--seed", type=int, default=11)
    a = ap.parse_args(argv)
    if client_factory is None:
        import pymongo
        client_factory = lambda u: pymongo.MongoClient(u, serverSelectionTimeoutMS=60000)  # noqa: E731
    os.makedirs(a.out, exist_ok=True)
    pub, priv = open(a.key_file).read().strip().split(":", 1)
    api = Api(a.api, pub, priv)
    client = client_factory(a.uri)
    dbname, st = a.db, None
    if a.states:
        states = fn.find_states(a.states)
        on = set(client.list_database_names())
        cand = sorted((d for d in states if d in on and (not a.db or d == a.db)),
                      key=lambda d: -float((states[d].get("caricamento") or {}).get("byte_bson") or 0))
        if cand:
            dbname, st = cand[0], states[cand[0]]
    s = {"program": "atlas-ops", "db": dbname, "cluster": a.cluster, "ops": {}}
    ops = [o for o in a.ops.split(",") if o]
    if not dbname or not st:
        s.update(ok=False, text="== atlas-ops: no database with a state file on the cluster")
    else:
        sc, sp = api.call("GET", cluster_path(a.group, a.cluster))
        s["cluster_api"] = [sc, {k: sp.get(k) for k in ("stateName", "pitEnabled", "backupEnabled", "providerBackupEnabled",
                                                        "clusterType", "mongoDBVersion", "diskSizeGB") if isinstance(sp, dict)}]
        customers = fn.customers(st, a.customers, a.seed)
        if "failover" in ops:
            s["ops"]["failover"] = op_failover(client, api, a.group, a.cluster, dbname, a.cap_s, sleep, now)
        if "copy" in ops:
            target, made = a.target_cluster, None
            if a.create_target:
                target = target or f"{a.cluster}-s9copy"
                ok, secs, capi = create_target(api, a.group, a.cluster, target, a.cap_s, sleep, now)
                made = {"ok": ok, "seconds": secs, "api": capi}
            if not target or not a.target_uri:
                s["ops"]["copy"] = {"ok": False, "why": "no --target-cluster and --target-uri (or --create-target with --target-uri)",
                                    "create": made}
            else:
                s["ops"]["copy"] = op_copy(client, client_factory(a.target_uri), api, a.group, a.cluster, target, dbname,
                                           customers, a.cap_s, sleep, now) | {"create_target": made}
                if a.create_target:
                    s["ops"]["copy"]["delete_target"] = api.call("DELETE", cluster_path(a.group, target))[0]
        if "restore" in ops:
            if not (isinstance(sp, dict) and sp.get("pitEnabled")):
                s["ops"]["restore"] = {"ok": False, "why": "continuous cloud backup (pitEnabled) is off on the cluster"}
            else:
                s["ops"]["restore"] = op_restore(client, api, a.group, a.cluster, dbname, customers[: a.write_customers], a.cap_s, sleep, now)
        s["ok"] = bool(s["ops"]) and all(o.get("ok") for o in s["ops"].values())
        lines = [f"== atlas-ops on {a.cluster} / {dbname}"]
        f = s["ops"].get("failover")
        if f:
            lines.append(f"second server (Test Failover, graceful): longest gap between acknowledged writes "
                         f"{f.get('longest_gap_between_acknowledged_writes_s')} s; new primary after {f.get('seconds_to_new_primary')} s")
        c = s["ops"].get("copy")
        if c:
            lines.append(f"copy (snapshot {c.get('snapshot_s')} s, then restore to {c.get('target_cluster')}): {c.get('seconds')} s; "
                         f"{'answers like its source' if c.get('answers_like_source') else 'NOT like its source: ' + str(c.get('why', ''))}")
        r = s["ops"].get("restore")
        if r:
            lines.append(f"restore to a past time: {r.get('seconds')} s; {'back exactly as at T' if r.get('restored_like_target') else 'NOT as at T: ' + str(r.get('why', ''))}")
        s["text"] = "\n".join(lines)
    json.dump(s, open(os.path.join(a.out, "atlas-ops-summary.json"), "w"), indent=1, default=str)
    open(os.path.join(a.out, "atlas-ops.txt"), "w").write(s["text"] + "\n")
    print(s["text"])
    print(f"ATLAS-OPS: {'OK' if s['ok'] else 'FAILED'}")
    return 0 if s["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
