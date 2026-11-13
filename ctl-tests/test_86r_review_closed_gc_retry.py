"""Review of fix-compactor-start 466d6c28 (2026-11-13): a closed database's GC pass (`gc_closed`) that cannot read a
version now stops, is counted (serverStatus `gcVersionReadErrors`) and is queued again after 60 s.

Checks, with every GET of one retained version's manifest failing (fault file) after the database closed:
- the pass is retried, about once a minute, not in a tight loop;
- no run that a kept version lists is deleted while the error lasts;
- once the fault is cleared, the retries stop, the runs are still there and every acknowledged document reads back.
"""
import json
import os
import tempfile
import time

import pytest
from pymongo import MongoClient

from lib import Server, make_bucket, s3

pytestmark = pytest.mark.skipif(os.environ.get("FENVIX_SEALED_RUNS") == "off", reason="needs sealed runs (FENVIX_SEALED_RUNS=off)")

DB = "gcr"
SEALED = ["--sealed-t-pin-ms", "1000", "--sealed-e1-wait-ms", "0", "--sealed-test-short-e1-wait",
          "--sealed-version-retention-secs", "3600"]


def manifests(bucket):
    c = s3()
    out = {}
    for o in c.list_objects_v2(Bucket=bucket, Prefix=f"tenants/{DB}/fx/manifest/").get("Contents", []):
        v = json.loads(c.get_object(Bucket=bucket, Key=o["Key"])["Body"].read())
        runs = [g["sealed"]["path"] for g in v.get("groups", []) if g.get("sealed")] + [d["path"] for d in v.get("delta", [])]
        out[v["version"]] = (o["Key"], runs)
    return out


def missing_runs(bucket, runs):
    c = s3()
    gone = []
    for r in runs:
        try:
            c.head_object(Bucket=bucket, Key=r)
        except Exception:
            gone.append(r)
    return gone


def find(d, key):
    if isinstance(d, dict):
        if key in d:
            return d[key]
        for v in d.values():
            x = find(v, key)
            if x is not None:
                return x
    return None


def test_closed_gc_retry_neither_loops_nor_deletes():
    ff = tempfile.NamedTemporaryFile("w", delete=False, suffix=".faults")
    ff.close()
    bucket = make_bucket("gcretry")
    srv = Server(bucket, "gcretry", idle=5, extra=SEALED, env={"MONGO_TEST_FAULTS_FILE": ff.name}).start()
    try:
        c = MongoClient(srv.uri, retryWrites=False)
        acked = []
        for r in range(5):
            for k in range(10):
                ids = [f"r{r}-{k}-{i}" for i in range(200)]
                c[DB]["c"].insert_many([{"_id": x, "r": r, "p": "x" * 300} for x in ids], ordered=True)
                acked.extend(ids)
            c[DB]["c"].update_many({"r": r}, {"$set": {"u": r}})
            time.sleep(4)
        c.close()
        t0 = time.time()
        while len(manifests(bucket)) < 3:
            assert time.time() - t0 < 60, f"fewer than 3 versions: {sorted(manifests(bucket))}"
            time.sleep(1)
        m = manifests(bucket)
        oldest = min(m)
        assert oldest < max(m) - 1, f"the failing version must be two below the newest: {sorted(m)}"
        with open(ff.name, "w") as fh:
            fh.write(f"fail_get {m[oldest][0]}\n")
        all_runs = sorted({r for _, (_, runs) in m.items() for r in runs})
        # the database closes (idle 5 s); its closed GC is due 30 s after the last write, then every 60 s while it fails
        time.sleep(200)
        log = srv.log_text()
        skipped = log.count("could not be read")
        faults = log.count("FAULT fail_get")
        st = MongoClient(srv.uri).admin.command("serverStatus")
        errs = find(st, "gcVersionReadErrors")
        gone_during = missing_runs(bucket, all_runs)
        print(f"versions {sorted(m)}, failing {oldest}; during 200 s: passes skipped {skipped}, fault hits {faults}, "
              f"gcVersionReadErrors {errs}; runs listed {len(all_runs)}, gone {len(gone_during)}")
        with open(ff.name, "w") as fh:
            fh.write("")
        time.sleep(100)
        log2 = srv.log_text()
        skipped_after = log2.count("could not be read") - skipped
        m2 = manifests(bucket)
        kept_runs = sorted({r for _, (_, runs) in m2.items() for r in runs})
        gone_after = missing_runs(bucket, kept_runs)
        c = MongoClient(srv.uri)
        got = {d["_id"] for d in c[DB]["c"].find({}, {"_id": 1})}
        lost = [x for x in acked if x not in got]
        print(f"after the fault: new skips {skipped_after}; versions {sorted(m2)}; kept runs {len(kept_runs)}, gone {len(gone_after)}; "
              f"acknowledged {len(acked)}, read {len(got)}, lost {len(lost)}")
        assert 2 <= skipped <= 5, f"the failed pass must be retried about once a minute: {skipped} in 200 s"
        assert errs is not None and errs >= skipped, (errs, skipped)
        assert not gone_during, gone_during[:5]
        assert oldest in m2, "the retained version that failed was deleted"
        assert not gone_after, gone_after[:5]
        assert skipped_after == 0, "retries go on after the fault is cleared"
        assert not lost, lost[:5]
    finally:
        srv.term()
