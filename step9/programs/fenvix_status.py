"""Step 9 · our server's status around a program (total review N4, DECISIONS 2026-11-11 "When the log cannot confirm,
the S3 WAL fallback stays; every result says so").
The engine acknowledges a write the 3-copy log did not confirm within 10 s by flushing it to the object-storage WAL
(tenantStats.quaderno.failed counts those batches). On AWS that is still durable across zones, but a number that mixes
the two paths must say so. Every AWS program therefore saves our server's status before and after, and its result is
labelled with how many batches took the fallback.
  snapshot(client) -> dict      serverStatus().fenvix (s3, compactor, compaction, derived) and tenantStats (quaderno,
                                walGc, sstGc, requests, groupCommit, leases)
  failed(snap) -> int | None    tenantStats.quaderno.failed
  label(before, after) -> dict  {"quaderno_failed_before", "_after", "fallback_batches", "label"}
CLI (machine/extras.sh):
  fenvix_status.py snapshot URI OUT_JSON
  fenvix_status.py stamp RESULT_JSON SHA256   (the mongo-server binary that made a result)
  fenvix_status.py label NAME BEFORE_JSON AFTER_JSON OUT_JSON [RESULT_JSON]
      writes the label to OUT_JSON, adds it as "fenvix_status" to RESULT_JSON when that file exists, and prints
      "FENVIX-STATUS NAME: ..." (a status that could not be read is said so: never silent)."""
import json
import os
import sys

FENVIX_KEYS = ("s3", "compactor", "compaction", "derived", "sealed", "backgroundReads")
TENANT_KEYS = ("quaderno", "walGc", "sstGc", "requests", "groupCommit", "localLog", "agentLane", "leases", "maintenance")


def snapshot(client) -> dict:
    out = {}
    try:
        f = client.admin.command("serverStatus").get("fenvix", {})
        out.update({k: f[k] for k in FENVIX_KEYS if k in f})
    except Exception as e:  # noqa: BLE001 - recorded, never fatal
        out["serverStatus_error"] = f"{type(e).__name__}: {str(e)[:200]}"
    try:
        ts = client.admin.command("tenantStats")
        out.update({k: ts[k] for k in TENANT_KEYS if k in ts})
    except Exception as e:  # noqa: BLE001
        out["tenantStats_error"] = f"{type(e).__name__}: {str(e)[:200]}"
    return json.loads(json.dumps(out, default=str))


def failed(snap) -> int | None:
    q = (snap or {}).get("quaderno")
    return None if not isinstance(q, dict) or q.get("failed") is None else int(q["failed"])


def label(before, after) -> dict:
    b, a = failed(before), failed(after)
    if b is None or a is None:
        return {"quaderno_failed_before": b, "quaderno_failed_after": a, "fallback_batches": None,
                "label": "UNKNOWN: our server's quaderno.failed could not be read before and after"}
    n = a - b if a >= b else a   # a restart starts the counter again at 0
    return {"quaderno_failed_before": b, "quaderno_failed_after": a, "fallback_batches": n,
            "label": ("every write acknowledged by the 3-zone log (2 of 3 copies)" if n == 0 else
                      f"{n} write batches took the S3-WAL fallback (the log did not confirm in 10 s): the result mixes "
                      f"the two paths")}


def main(argv):
    if argv[1] == "snapshot":
        from pymongo import MongoClient
        snap = snapshot(MongoClient(argv[2], serverSelectionTimeoutMS=20000))
        json.dump(snap, open(argv[3], "w"), indent=1)
        print(f"fenvix status saved: quaderno.failed {failed(snap)}")
        return 0
    if argv[1] == "stamp":   # stamp RESULT_JSON SHA256: the binary that made a result, into the result
        res, sha = argv[2], argv[3]
        if os.path.exists(res):
            d = json.load(open(res))
            d["engine_binary_sha256"] = sha or None
            json.dump(d, open(res, "w"), indent=1)
        return 0
    if argv[1] == "label":
        name, bf, af, out = argv[2:6]
        res = argv[6] if len(argv) > 6 else None
        load = lambda p: json.load(open(p)) if os.path.exists(p) else None  # noqa: E731
        lab = label(load(bf), load(af))
        json.dump(lab, open(out, "w"), indent=1)
        if res and os.path.exists(res):
            d = json.load(open(res))
            d["fenvix_status"] = lab
            json.dump(d, open(res, "w"), indent=1)
        print(f"FENVIX-STATUS {name}: {lab['label']}")
        return 0
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
