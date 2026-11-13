"""Step 9 · two operations whose cost on MongoDB grows with the data, on OUR line (founder's decision, 2026-11-13: where
we are stronger; locally, MEASURED by big-speed on 13.8 GB: copy 3.1 s against mongod's 368 s, restore to a past time
36 s against 339 s). The third, a second server answering, is the takeover program's `takeover_s` (job M2).

Through the cook's admin interface (`--admin-listen`, bearer token; mongo-server/src/ctl/api.rs):
  copy     POST /v1/databases/DB/fork {"target": DB_copy, "mode": "clone"}: timed from the call until the copy answers
           every sampled customer's newest message exactly like its source; then the copy is erased (DELETE).
  restore  PUT /v1/databases/DB/retention {"window_secs": 7200, "interval_secs": 30}; a restore point; two new messages
           on each of K sampled customers (w: majority); --upload-wait-s (the 3-zone log uploads its blocks to S3 every
           30 s, and the restore places a WAL object in time by its object timestamp: big-speed's pilot found that a T
           taken 1.5 s after the writes also removed them); T; two more messages each; POST /v1/databases/DB/restore
           {"to": T}: timed from the call until the database answers; then checked: every sampled customer's newest
           message is the one it had at T, and the messages after T are gone.
Both write into the database (the restore changes it), so the program is a writing program of the round (decide.py).
Each operation has a cap (--cap-s); past it, it is reported as not finished.

Usage: data_ops.py OUT_DIR --uri URI --admin-url http://HOST:8080 --token-file F (--states DIR | --db NAME)
                   [--customers 20] [--write-customers 5] [--upload-wait-s 40] [--cap-s 1800] [--ops copy,restore]
  -> OUT_DIR/data-ops-summary.json, data-ops.txt; exit 1 when an operation failed, did not finish or answered wrong."""
import argparse
import datetime as dt
import json
import os
import random
import sys
import time
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import first_next as fn  # noqa: E402


class Admin:
    def __init__(self, url, token, timeout):
        self.url, self.token, self.timeout = url.rstrip("/"), token, timeout

    def call(self, method, path, body=None):
        req = urllib.request.Request(self.url + path, method=method,
                                     data=json.dumps(body).encode() if body is not None else None,
                                     headers={"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as r:
                return r.status, json.loads(r.read() or b"{}")
        except urllib.error.HTTPError as e:
            try:
                return e.code, json.loads(e.read() or b"{}")
            except Exception:  # noqa: BLE001
                return e.code, {}


def newest(client, db, cli):
    d = client[db]["messaggi"].find_one({"cliente": cli}, sort=[("ts", -1)])
    return None if d is None else str(d.get("_id"))


def wait_done(admin, db, t0, cap_s, sleep=time.sleep, now=time.monotonic):
    """Until the database has no operation in progress (GET /v1/databases/DB), at most cap_s from t0."""
    while now() - t0 < cap_s:
        st, r = admin.call("GET", f"/v1/databases/{db}")
        if st == 200 and not r.get("operation"):
            return True
        sleep(0.5)
    return False


def wait_answers(client, db, want, t0, cap_s, sleep=time.sleep, now=time.monotonic):
    """Until db answers every customer of `want` with that newest message; (seconds, right)."""
    while now() - t0 < cap_s:
        try:
            if all(newest(client, db, c) == w for c, w in want.items()):
                return round(now() - t0, 2), True
        except Exception:  # noqa: BLE001 - not there yet
            pass
        sleep(0.2)
    return round(now() - t0, 2), False


def write_some(client, db, customers, k, tag):
    from pymongo.write_concern import WriteConcern
    c = client[db]["messaggi"].with_options(write_concern=WriteConcern(w="majority"))
    for cli in customers:
        for j in range(k):
            c.insert_one({"cliente": cli, "ts": dt.datetime.now(dt.timezone.utc), "conv": "conv-s9", "ruolo": "user",
                          "modello": "s9", "token": 10, "testo": f"data-ops {tag} {j}", "s9": f"data-ops:{tag}:{j}"})


def op_copy(client, admin, db, customers, cap_s, sleep=time.sleep, now=time.monotonic):
    want = {c: newest(client, db, c) for c in customers}
    target = f"{db}_s9copy"
    t0 = now()
    st, r = admin.call("POST", f"/v1/databases/{db}/fork", {"target": target, "mode": "clone"})
    if st not in (200, 201):
        return {"ok": False, "api": [st, r], "seconds": round(now() - t0, 2)}
    wait_done(admin, target, t0, cap_s, sleep, now)
    secs, right = wait_answers(client, target, want, t0, cap_s, sleep, now)
    out = {"ok": right, "api": [st, r], "seconds": secs, "answers_like_source": right, "customers_checked": len(want),
           "finished": secs < cap_s}
    sd, rd = admin.call("DELETE", f"/v1/databases/{target}")
    out["erase_copy"] = [sd, rd]
    return out


def op_restore(client, admin, db, customers, upload_wait_s, cap_s, sleep=time.sleep, now=time.monotonic, wall=time.time):
    sr, rr = admin.call("PUT", f"/v1/databases/{db}/retention", {"window_secs": 7200, "interval_secs": 30})
    points = None
    t_on = now()
    while now() - t_on < 180:
        sp, rp = admin.call("GET", f"/v1/databases/{db}/restore-points")
        if sp == 200 and rp.get("points"):
            points = rp
            break
        sleep(1)
    write_some(client, db, customers, 2, "before-T")
    sleep(upload_wait_s)
    t_target = int(wall() * 1000)
    at_t = {c: newest(client, db, c) for c in customers}
    sleep(1.5)
    write_some(client, db, customers, 2, "after-T")
    after = {c: newest(client, db, c) for c in customers}
    t0 = now()
    st, r = admin.call("POST", f"/v1/databases/{db}/restore", {"to": t_target})
    done = st == 200 and wait_done(admin, db, t0, cap_s, sleep, now)
    secs, right = wait_answers(client, db, at_t, t0, cap_s, sleep, now) if done else (round(now() - t0, 2), False)
    return {"ok": bool(done and right and at_t != after), "retention": [sr, rr], "restore_point_seen": points is not None,
            "api": [st, r], "seconds": secs, "restored_like_target": right, "changed_since_target": at_t != after,
            "customers_checked": len(at_t), "target_ms": t_target, "finished": bool(done)}


def main(argv=None, sleep=time.sleep, now=time.monotonic):
    ap = argparse.ArgumentParser()
    ap.add_argument("out")
    ap.add_argument("--uri", required=True)
    ap.add_argument("--admin-url", required=True)
    ap.add_argument("--token-file", required=True)
    ap.add_argument("--states", default="")
    ap.add_argument("--db", default="")
    ap.add_argument("--customers", type=int, default=20)
    ap.add_argument("--write-customers", type=int, default=5)
    ap.add_argument("--upload-wait-s", type=float, default=40.0)
    ap.add_argument("--cap-s", type=float, default=1800.0)
    ap.add_argument("--ops", default="copy,restore")
    ap.add_argument("--seed", type=int, default=11)
    a = ap.parse_args(argv)
    import pymongo
    os.makedirs(a.out, exist_ok=True)
    client = pymongo.MongoClient(a.uri, serverSelectionTimeoutMS=30000, socketTimeoutMS=int(a.cap_s * 1000))
    admin = Admin(a.admin_url, open(a.token_file).read().strip(), a.cap_s)
    dbname, st = a.db, None
    if a.states:
        states = fn.find_states(a.states)
        on = set(client.list_database_names())
        cand = sorted((d for d in states if d in on and (not a.db or d == a.db)),
                      key=lambda d: -float((states[d].get("caricamento") or {}).get("byte_bson") or 0))
        if cand:
            dbname, st = cand[0], states[cand[0]]
    s = {"program": "data-ops", "db": dbname, "ops": {}}
    if not dbname or not st:
        s.update(ok=False, text="== data-ops: no database with a state file on the server")
    else:
        customers = fn.customers(st, a.customers, a.seed)
        try:
            s["source_dbstats"] = {k: v for k, v in client[dbname].command("dbStats").items() if k in ("dataSize", "storageSize", "objects")}
        except Exception:  # noqa: BLE001
            pass
        ops = [o for o in a.ops.split(",") if o]
        if "copy" in ops:
            s["ops"]["copy"] = op_copy(client, admin, dbname, customers, a.cap_s, sleep, now)
        if "restore" in ops:
            s["ops"]["restore"] = op_restore(client, admin, dbname, customers[: a.write_customers], a.upload_wait_s, a.cap_s, sleep, now)
        s["ok"] = bool(s["ops"]) and all(o.get("ok") for o in s["ops"].values())
        lines = [f"== data-ops on {dbname} ({s.get('source_dbstats', {}).get('dataSize', '?')} bytes of data)"]
        if "copy" in s["ops"]:
            o = s["ops"]["copy"]
            lines.append(f"copy (fork, clone): {o['seconds']:.2f} s until it answered {o.get('customers_checked')} customers "
                         f"{'like its source' if o.get('answers_like_source') else 'WRONG or not at all'}")
        if "restore" in s["ops"]:
            o = s["ops"]["restore"]
            lines.append(f"restore to a past time: {o['seconds']:.2f} s; {'back exactly as at T' if o.get('restored_like_target') else 'NOT as at T'}"
                         f" on {o.get('customers_checked')} customers that changed after T ({o.get('changed_since_target')}); restore point "
                         f"{'seen' if o.get('restore_point_seen') else 'NOT seen'}")
        s["text"] = "\n".join(lines)
    json.dump(s, open(os.path.join(a.out, "data-ops-summary.json"), "w"), indent=1, default=str)
    open(os.path.join(a.out, "data-ops.txt"), "w").write(s["text"] + "\n")
    print(s["text"])
    print(f"DATA-OPS: {'OK' if s['ok'] else 'FAILED'}")
    return 0 if s["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
