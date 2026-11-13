"""P-fix-read-amplification: a fetch of the documents an index read wants reads about as many rows as it wants.

The defect (MEASURED by the generator session, sessions/2026-11-12-generator-1tb/REPORT-2.md section 3): a hinted read
of an index over many customers (`stato_ts`) wants a few hundred documents spread over the whole collection. The fetch
(`coll.rs`, `docs_by_ids`) chose one range scan over the span of their ids whenever the tenant looked big enough for
it, and that span is nearly the whole collection: about 310 rows per document examined on a client-written database,
about 4,600 on a 3 GiB generated one after updates (1.6 GB fetched for 800 documents). Two causes, both fixed:
- the scan was not capped (now: at most 2n+32 rows, then point reads for the ids it did not reach);
- the tenant's size counted SlateDB's SSTs only, not our runs, so a database held in runs looked small.

The test: a customer-keyed collection written by a client, published into runs by the compactor, then updated. At
each stage the same hinted index reads; rows read from runs per document examined must stay under 10, and every
answer must equal the expected documents (computed here from what was written) and the answer with the cap off
(`FENVIX_NO_SPAN_BUDGET=1`, the base's uncapped scan). On the base binary the rows per document are in the hundreds.
"""
import datetime as dt
import os
import random
import time

import pytest
from bson import ObjectId

from lib import Server, make_bucket

pytestmark = pytest.mark.skipif(os.environ.get("FENVIX_SEALED_RUNS", "on") in ("off", "0"),
                                reason="needs sealed runs and their compactor (FENVIX_SEALED_RUNS off)")

SEALED = {"FENVIX_SEALED_RUNS": "2", "FENVIX_SEALED_TPIN_MS": "1000", "FENVIX_SEALED_E1_WAIT_MS": "0", "FENVIX_SEALED_COMPACTOR": "small",
          "FENVIX_SEALED_TEST_SHORT_E1_WAIT": "1"}
EXTRA = ["--envelopes", "write", "--customer-key", "o=cliente:ts"]
STATES = ["nuovo", "pagato", "spedito", "consegnato", "reso"]
T0 = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
SPAN = 365 * 86400
CUSTOMERS, PER = 400, 40
MAX_ROWS_PER_DOC = 10


def fx(c):
    f = c.admin.command("serverStatus")["fenvix"]
    return f["sealedRuns"]["rowsFromRuns"], f.get("fetch", {})


def wait_published(c, need_absorbs=1, timeout=180):
    t0 = time.time()
    while time.time() - t0 < timeout:
        s = c.admin.command("serverStatus")["fenvix"]["compactor"]
        if s.get("publishes", 0) >= 1 and s.get("absorbs", 0) >= need_absorbs:
            return s
        time.sleep(0.5)
    raise AssertionError(f"the compactor did not publish and absorb: {s}")


def queries():
    rng = random.Random(5)
    out = []
    for _ in range(4):
        cli = f"cli-{rng.randrange(CUSTOMERS):06d}"
        t = T0 + dt.timedelta(seconds=rng.uniform(0.1, 0.8) * SPAN)
        out.append({"stato": "spedito", "cliente": cli, "ts": {"$gte": t, "$lt": t + dt.timedelta(days=60)}})
        # the same window over every customer: every examined document is returned
        out.append({"stato": "spedito", "ts": {"$gte": t, "$lt": t + dt.timedelta(days=20)}})
    return out


def expected(docs, q):
    lo, hi = q["ts"]["$gte"], q["ts"]["$lt"]
    return sorted(str(d["_id"]) for d in docs.values()
                  if d["stato"] == q["stato"] and lo <= d["ts"] < hi and ("cliente" not in q or d["cliente"] == q["cliente"]))


def measure(c, docs, stage):
    o = c["ra"]["o"]
    worst = 0.0
    answers = []
    for q in queries():
        r0, _ = fx(c)
        ex = o.find(q).hint("stato_ts").explain()["executionStats"]
        r1, f = fx(c)
        got = sorted(str(d["_id"]) for d in o.find(q).hint("stato_ts"))
        assert got == expected(docs, q), f"{stage}: wrong answer for {q}"
        answers.append(got)
        n = max(1, ex["totalDocsExamined"])
        per = (r1 - r0) / n
        print(f"{stage}: examined {ex['totalDocsExamined']}, returned {ex['nReturned']}, rows from runs {r1 - r0}"
              f" ({per:.1f} per document), fetch {f}")
        worst = max(worst, per)
    return worst, answers


def count_rows(c, n, stage):
    """Rows from runs per document of a read of the whole collection (natural order: its entries, then the documents)."""
    r0, _ = fx(c)
    got = sum(1 for _ in c["ra"]["o"].find({}, {"_id": 1}))
    r1, _ = fx(c)
    assert got == n, f"{stage}: {got} documents read, {n} expected"
    print(f"{stage}: whole collection, {got} documents, rows from runs {r1 - r0} ({(r1 - r0) / n:.1f} per document)")
    return (r1 - r0) / n


def test_index_fetch_reads_few_rows_per_document():
    bucket = make_bucket("rdamp")
    srv = Server(bucket, "rdamp", idle=600, extra=EXTRA, env=SEALED).start()
    try:
        c = srv.client()
        o = c["ra"]["o"]
        c["ra"].create_collection("o")
        o.create_index([("cliente", 1), ("ts", -1)], name="cliente_ts")
        o.create_index([("stato", 1), ("ts", -1)], name="stato_ts")
        rng = random.Random(1)
        docs = {}
        batch = []
        for i in range(CUSTOMERS):
            for _ in range(PER):
                d = {"_id": ObjectId(), "cliente": f"cli-{i:06d}", "ts": T0 + dt.timedelta(seconds=int(rng.uniform(0, SPAN))),
                     "stato": rng.choice(STATES), "totale": round(rng.uniform(5, 500), 2), "note": "x" * rng.randint(100, 300)}
                docs[d["_id"]] = d
                batch.append(d)
                if len(batch) == 1000:
                    o.insert_many(batch)
                    batch = []
        if batch:
            o.insert_many(batch)
        wait_published(c)
        time.sleep(3)  # a refresh of the version after the absorption
        w1, a1 = measure(c, docs, "1 after the publish")
        # updates: SlateDB rows above the version's cut, and new L0 SSTs (what flipped the base's choice to the scan)
        ids = list(docs)
        for oid in rng.sample(ids, len(ids) // 10):
            s = rng.choice(STATES)
            o.update_one({"cliente": docs[oid]["cliente"], "_id": oid}, {"$set": {"stato": s}})
            docs[oid]["stato"] = s
        time.sleep(2)
        w2, a2 = measure(c, docs, "2 after the updates")
        whole2 = count_rows(c, len(docs), "2 after the updates")
        srv.kill9()
        time.sleep(8)  # the killed process's lease (6 s) runs out; the next process takes the database over
        # both bounds off (the base's reads: the uncapped fetch scan, the unbounded area reads): the same answers
        srv = Server(bucket, "rdamp-off", idle=600, extra=EXTRA,
                     env=dict(SEALED, FENVIX_NO_SPAN_BUDGET="1", FENVIX_AREA_UNBOUNDED="1")).start()
        c = srv.client()
        w3, a3 = measure(c, docs, "3 bounds off (negative control)")
        assert a3 == a2, "the answers with the bounds off differ"
        whole3 = count_rows(c, len(docs), "3 bounds off (negative control)")
        print(f"worst rows per document: {w1:.1f} after the publish, {w2:.1f} after the updates, {w3:.1f} with the bounds off;"
              f" whole-collection read {whole2:.1f} per document, {whole3:.1f} with the bounds off")
        assert w1 < MAX_ROWS_PER_DOC and w2 < MAX_ROWS_PER_DOC, f"read amplification: {w1:.1f} / {w2:.1f} rows per document"
        assert whole2 < 3, f"a read of the whole collection reads {whole2:.1f} rows per document"
    finally:
        srv.kill9()
