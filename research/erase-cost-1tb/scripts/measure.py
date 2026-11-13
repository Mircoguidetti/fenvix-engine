"""erase-cost-1tb: erase N customers of the ballast database and measure what the physical erasure costs.

Every number comes from counters, not estimates: MinIO's own request and traffic counters (the whole store, so nothing
else may use this MinIO during a run), and the server's serverStatus (fenvix.s3 and, on erase-cost-1tb, the compactor's
bytes written by job kind and the scrub's bytes). Usage:
  measure.py --uri mongodb://127.0.0.1:27017 --admin http://127.0.0.1:8443 --customers 5 --stride 2400 --offset 7 \
             [--run] [--wait-scrubbed SECS] --out results/x.json
--run: POST .../erasures/run after the deletes (forces the physical erasure now, on both binaries).
"""
import argparse
import json
import time
import urllib.request

import pymongo

TOKEN = "test-token-0123456789abcdef"
COLLS = ["messaggi", "eventi", "ordini"]


def minio(url):
    out = {}
    with urllib.request.urlopen(url + "/minio/v2/metrics/cluster") as r:
        for line in r.read().decode().splitlines():
            if line.startswith("minio_s3_requests_total{"):
                api = line.split('api="')[1].split('"')[0]
                out["req." + api] = float(line.rsplit(" ", 1)[1])
            elif line.startswith("minio_s3_traffic_received_bytes"):
                out["received_bytes"] = float(line.rsplit(" ", 1)[1])
            elif line.startswith("minio_s3_traffic_sent_bytes"):
                out["sent_bytes"] = float(line.rsplit(" ", 1)[1])
    return out


def status(c):
    s = c.admin.command("serverStatus")["fenvix"]
    return {"s3": s.get("s3", {}), "compactor": s.get("compactor", {})}


def admin(base, method, path, body=None):
    req = urllib.request.Request(base + path, method=method, data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=7200) as r:
        return json.loads(r.read())


def flat(d, p=""):
    out = {}
    for k, v in d.items():
        if isinstance(v, dict):
            out.update(flat(v, p + k + "."))
        elif isinstance(v, (int, float)):
            out[p + k] = v
    return out


def delta(a, b):
    return {k: b[k] - a.get(k, 0) for k in b if b[k] - a.get(k, 0) != 0}


def snap(c, mu):
    return {"t": time.time(), "minio": minio(mu), "server": flat(status(c))}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--uri", default="mongodb://127.0.0.1:27017/?directConnection=true")
    ap.add_argument("--admin", required=True)
    ap.add_argument("--minio", default="http://127.0.0.1:9000")
    ap.add_argument("--db", default="app_data_b01")
    ap.add_argument("--customers", type=int, default=1)
    ap.add_argument("--stride", type=int, default=1000)
    ap.add_argument("--offset", type=int, default=0)
    ap.add_argument("--run", action="store_true")
    ap.add_argument("--out", required=True)
    ap.add_argument("--idle-s", type=float, default=0, help="wait this long after the deletes before --run (the agent's checks only)")
    a = ap.parse_args()
    c = pymongo.MongoClient(a.uri, serverSelectionTimeoutMS=600000, socketTimeoutMS=3600000)
    db = c[a.db]
    custs = [f"cli-{a.offset + i * a.stride:06d}" for i in range(a.customers)]
    rep = {"customers": custs, "deletes": []}
    s0 = snap(c, a.minio)
    for cu in custs:
        for coll in COLLS:
            t = time.time()
            n = db[coll].delete_many({"cliente": cu}).deleted_count
            rep["deletes"].append({"customer": cu, "coll": coll, "deleted": n, "ms": round((time.time() - t) * 1000, 1)})
    s1 = snap(c, a.minio)
    rep["delete_phase"] = {"s": round(s1["t"] - s0["t"], 2), "minio": delta(s0["minio"], s1["minio"]), "server": delta(s0["server"], s1["server"])}
    if a.idle_s:
        time.sleep(a.idle_s)
        si = snap(c, a.minio)
        rep["idle"] = {"s": round(si["t"] - s1["t"], 2), "minio": delta(s1["minio"], si["minio"]), "server": delta(s1["server"], si["server"]),
                       "states": [e["state"] for e in admin(a.admin, "GET", f"/v1/databases/{a.db}/erasures")["erasures"]][-3 * len(custs):]}
        s1 = si
    if a.run:
        t = time.time()
        r = admin(a.admin, "POST", f"/v1/databases/{a.db}/erasures/run", {})
        s2 = snap(c, a.minio)
        rep["run"] = {"s": round(time.time() - t, 2), "minio": delta(s1["minio"], s2["minio"]), "server": delta(s1["server"], s2["server"]),
                      "states": [e["state"] for e in r["erasures"]], "cleaned_by": [e.get("cleaned_by") for e in r["erasures"]],
                      "rows_erased": [e.get("rows_erased") for e in r["erasures"]], "runs_rewritten": [e.get("runs_rewritten") for e in r["erasures"]]}
    for cu in custs:
        for coll in COLLS:
            assert db[coll].count_documents({"cliente": cu}) == 0, f"{cu} readable in {coll}"
    rep["unreadable_checked"] = True
    json.dump(rep, open(a.out, "w"), indent=1)
    print(json.dumps({k: rep[k] for k in rep if k != "deletes"}, indent=1)[:6000])


if __name__ == "__main__":
    main()
