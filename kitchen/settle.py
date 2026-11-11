"""Wait until our server's compaction is really finished, before the measures (and before the loader is switched off).
History: K2b (reviewer's risk 1 and C6) counted only the `.sst` objects of the pantry; K2c added MIN_SECONDS (our server
closes a database after 60 idle seconds and only then asks the compaction of a closed writer).
aws-fixes (2026-11-11, B5 of the total review): with compaction 2 on, the compactor writes sealed runs and our manifest
under `tenants/<db>/fx/...`, objects with NO `.sst` extension (src/fx/manifest.rs), so the old check was blind to it; and
it gave up after MAX minutes and went on as if nothing happened. Now:
  - every STEP seconds it looks at three things:
      1. the server's own counters (serverStatus, section `fenvix`): `compactor.running` (compaction 2 jobs running now)
         and its totals `publishes`, `absorbs`, `merges`, `reseals`, `errors`; `compaction.running` and `passes` (the
         SlateDB compaction passes, lifecycle.rs); `sealedRuns.publishes` and `publishErrors`; `derived.encoded`;
      2. the `fx/` objects of the pantry (count and bytes): sealed runs, versions of our manifest;
      3. the `.sst` objects of the pantry (count and bytes): SlateDB's L0 and compacted SSTs, the WAL;
  - CALM = for three polls in a row nothing runs (`compactor.running` = 0 and `compaction.running` = 0) and none of the
    totals and none of the counts or bytes moved, and at least MIN_SECONDS passed since the start;
  - the compactor's or the publisher's error counters growing during the wait is NOT calm (a compactor that fails is
    not a finished compaction);
  - no server counters (no URI, the server does not answer, or no `fenvix.compactor` / `fenvix.compaction` section in
    its serverStatus: an engine without compaction 2's counters) is NOT calm: the check cannot see the compactor;
  - no silent give-up: at MAX minutes it stops with "SETTLE: NOT CALM ... STOP" and exit code 3; the caller stops and
    says so instead of measuring (kitchen/client.sh). Exit 0 only when calm.
The record (every poll: time, counters, object counts and bytes; the verdict and its reason) is written to OUT_JSON,
which the caller keeps with the results.
Cost: one LIST of the pantry per poll ($0.005 per 1,000 objects listed, DECLARED); serverStatus is free.
Usage: settle.py PANTRY MAX_MINUTES STEP_SECONDS OUT_JSON [MIN_SECONDS]   (FENVIX_P1_URI in the environment: the
       server whose compaction is awaited; never printed)"""
import json
import os
import sys
import time

COUNTERS = (("compactor", "running"), ("compactor", "publishes"), ("compactor", "absorbs"), ("compactor", "merges"),
            ("compactor", "reseals"), ("compactor", "errors"), ("compaction", "running"), ("compaction", "passes"),
            ("sealedRuns", "publishes"), ("sealedRuns", "publishErrors"), ("derived", "encoded"))
RUNNING = ("compactor.running", "compaction.running")
ERRORS = ("compactor.errors", "sealedRuns.publishErrors")


def objects(s3, bucket):
    """Count and bytes of the `.sst` objects and of the `fx/` objects (sealed runs, manifest versions) of the pantry."""
    out = {"sst": 0, "sst_bytes": 0, "fx": 0, "fx_bytes": 0}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix="tenants/"):
        for o in page.get("Contents", []):
            k = o["Key"]
            if k.endswith(".sst"):
                out["sst"] += 1
                out["sst_bytes"] += o["Size"]
            if "/fx/" in k:
                out["fx"] += 1
                out["fx_bytes"] += o["Size"]
    return out


def counters(client):
    """The server's compaction counters, or (None, reason)."""
    if client is None:
        return None, "no server URI (FENVIX_P1_URI)"
    try:
        f = client.admin.command("serverStatus").get("fenvix") or {}
    except Exception as e:  # noqa: BLE001
        return None, f"serverStatus failed: {type(e).__name__}"
    if not isinstance(f.get("compactor"), dict) or not isinstance(f.get("compaction"), dict):
        return None, "serverStatus has no fenvix.compactor / fenvix.compaction section (engine without compaction 2's counters)"
    c = {}
    for sec, key in COUNTERS:
        v = (f.get(sec) or {}).get(key)
        if v is not None:
            c[f"{sec}.{key}"] = int(v)
    c["compactor.enabled"] = bool((f.get("compactor") or {}).get("enabled"))
    return c, None


def judge(seen, least):
    """(calm, reason) from the polls so far."""
    last = seen[-1]
    if last.get("counters") is None:
        return False, f"no server counters: {last.get('counters_error')}"
    first = next((x for x in seen if x.get("counters") is not None), last)
    grew = [k for k in ERRORS if last["counters"].get(k, 0) > first["counters"].get(k, 0)]
    if grew:
        fc, lc = first["counters"], last["counters"]
        return False, "error counters grew during the wait: " + ", ".join(f"{k} {fc.get(k, 0)} -> {lc[k]}" for k in grew)
    busy = [k for k in RUNNING if last["counters"].get(k, 0) > 0]
    if busy:
        return False, "running: " + ", ".join(f"{k} {last['counters'][k]}" for k in busy)
    if last["s"] < least:
        return False, f"before the minimum wait of {least:.0f} s"
    if len(seen) < 3:
        return False, "fewer than three polls"
    a, b, c = seen[-3:]
    if any(x.get("counters") is None for x in (a, b)):
        return False, "counters missing in one of the last three polls"
    for x, y in ((a, b), (b, c)):
        if x["objects"] != y["objects"]:
            return False, "pantry objects still changing"
        if {k: v for k, v in x["counters"].items() if k != "compactor.enabled"} != \
                {k: v for k, v in y["counters"].items() if k != "compactor.enabled"}:
            return False, "compaction counters still moving"
    return True, "three polls with nothing running and nothing moving"


def main():
    bucket, most, step, out = sys.argv[1], float(sys.argv[2]), float(sys.argv[3]), sys.argv[4]
    least = float(sys.argv[5]) if len(sys.argv) > 5 else 0.0
    import boto3
    s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "eu-west-1"))
    client = None
    uri = os.environ.get("FENVIX_P1_URI")
    if uri:
        from pymongo import MongoClient
        client = MongoClient(uri, appname="app-demo", serverSelectionTimeoutMS=20000)
    t0 = time.time()
    seen = []
    calm, why = False, ""
    while True:
        c, err = counters(client)
        rec = {"s": round(time.time() - t0), "objects": objects(s3, bucket), "counters": c}
        if err:
            rec["counters_error"] = err
        seen.append(rec)
        calm, why = judge(seen, least)
        o = rec["objects"]
        print(f"settle: {rec['s']} s, {o['sst']} SST ({o['sst_bytes'] / 2**30:.3f} GiB), {o['fx']} fx objects "
              f"({o['fx_bytes'] / 2**30:.3f} GiB), counters {c if c is not None else err}: {why}", flush=True)
        if calm or time.time() - t0 >= most * 60:
            break
        time.sleep(step)
    rec = {"calm": calm, "reason": why, "seconds": round(time.time() - t0), "max_minutes": most, "step_s": step,
           "min_s": least, "polls": seen}
    json.dump(rec, open(out, "w"), indent=1)
    last = seen[-1]["objects"]
    if calm:
        print(f"SETTLE: calm after {rec['seconds']} s ({last['sst']} SST, {last['fx']} fx objects): {why}")
        sys.exit(0)
    print(f"SETTLE: NOT CALM after {rec['seconds']} s (cap {most:g} min): {why}: STOP (no measure on an unsettled pantry)")
    sys.exit(3)


if __name__ == "__main__":
    main()
