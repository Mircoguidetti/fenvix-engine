"""Step 9 · reads after writes (coordinator, 2026-11-13). A known slowdown, found by the generator's second session
(fenvix-brain sessions/2026-11-12-generator-1tb/REPORT-2.md section 3) and being fixed (P-fix-read-amplification): after a
few thousand updates, a read through the `stato_ts` index of `ordini` read about 4,600 rows from sealed runs per
document examined instead of 2, and took 15 to 40 times longer (3 GiB: 0.4-0.8 s -> 11.5-12.9 s; 20 GiB: about 77 s).
No wrong answer. At 1 TB it decides every such read after writes, so the round measures it, on the big database,
through the measured cook, with the cook's counters around every read.

The read is the probe's own (research/generator-1tb/probe_fetch.py): orders of one state and one customer in a time
window, forced through the index:  ordini.find({stato: "spedito", cliente, ts in [t, t + window)}).hint("stato_ts"),
run as explain (executionStats: documents examined, documents returned), the same K reads at each stage.
The window: the index holds every customer's orders, so the read examines every order of that state in the window to
keep one customer's. The probe's one day examined about 800 documents on 3 GiB; on 1 TB one day holds about 260,000
(ESTIMATED: 15% of the documents are orders, 5 states, a year), which would not end even before any write. So the
window is sized from the state file's own order count to examine about --examine documents (800) at any size; the
record keeps the window and the documents really examined. The stages:
  1 before       the database as the generator (or the load) left it, read only so far
  2 after-writes right after W updates of orders (10% of the orders of sampled customers: a new state, +1.5 on the
                 total: the generator proof's own writes) acknowledged at w: majority; every update is timed
  3 settled      after the cook's compactor went quiet (its counters unchanged for 60 s) or --settle-min passed;
                 the record says which, and whether it absorbed anything
Per read: seconds, documents examined and returned, and on our server the bytes and ranged GETs fetched from S3 and the
rows read from sealed runs (serverStatus fenvix.s3 and fenvix.sealedRuns); on any other server the time and the
explain only. The number that matters: rows read from runs per document examined, and seconds, stage 2 against stage 1.

A read is cut at --max-read-s (the client's socket timeout): recorded as timed out, and its stage stops there (the
server may still be working on it; more reads would only queue behind). A timed-out read is a result, not a failure
of the program. The program fails (exit 1) only when a read or a write errors otherwise, or stage 1 measured nothing.

Usage: reads_after_writes.py OUT_DIR --uri URI (--states DIR | --db NAME --customers-n N) [--reads 6] [--updates 13000]
                             [--write-customers 150] [--max-read-s 180] [--settle-min 20] [--seed 5] [--examine 800]
  -> OUT_DIR/reads-after-writes-summary.json, reads-after-writes.jsonl, reads-after-writes.txt"""
import argparse
import datetime as dt
import json
import os
import random
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import first_next as fn  # noqa: E402
from first_next_ops import T_SPAN_S, T_START  # noqa: E402

STATES = ["nuovo", "pagato", "spedito", "consegnato", "reso"]   # the script's STATI
STAGES = ("before", "after-writes", "settled")


def fx(client):
    """Our server's counters, or None on another server."""
    try:
        f = client.admin.command("serverStatus")["fenvix"]
        return {"get_bytes": int(f["s3"]["getBytes"]), "get_range": int(f["s3"]["getRange"]),
                "rows_from_runs": int(f["sealedRuns"]["rowsFromRuns"]), "compactor": f.get("compactor")}
    except Exception:  # noqa: BLE001 - not our server, or an engine without these counters
        return None


def window_seconds(st, examine):
    """The window that holds about `examine` orders of one state, from the state file's order count (the script's 5
    states, spread over its year). Without a count: the probe's one day."""
    car = (st or {}).get("caricamento") or {}
    orders = (car.get("per_collezione") or {}).get("ordini")
    if isinstance(orders, dict):
        orders = orders.get("documenti") or orders.get("doc")
    if not orders and car.get("documenti"):
        orders = 0.15 * float(car["documenti"])          # the script's mix: 15% of the documents are orders
    if not orders:
        return 86400.0
    per_second = float(orders) / len(STATES) / T_SPAN_S
    return max(1.0, min(86400.0, examine / per_second))


def the_reads(names, k, seed):
    """K (customer, window start) pairs, fixed by the seed: the same reads at every stage."""
    rng = random.Random(f"{seed}:reads")
    out = []
    for _ in range(k):
        cli = names[rng.randrange(len(names))]
        t = T_START + dt.timedelta(seconds=rng.uniform(0.1, 0.9) * T_SPAN_S)
        out.append((cli, t))
    return out


def one_read(coll, cli, t, window_s=86400.0):
    q = {"stato": "spedito", "cliente": cli, "ts": {"$gte": t, "$lt": t + dt.timedelta(seconds=window_s)}}
    e = coll.find(q).hint("stato_ts").explain()
    ex = e.get("executionStats") or {}
    return int(ex.get("totalDocsExamined", 0)), int(ex.get("nReturned", 0))


def stage(client_for_reads, counters_client, dbname, reads, name, rows, window_s=86400.0):
    """Run the K reads of one stage; stop the stage at the first timed-out read."""
    coll = client_for_reads[dbname]["ordini"]
    for i, (cli, t) in enumerate(reads):
        a = fx(counters_client)
        t0 = time.perf_counter()
        err, timed_out, ex, ret = None, False, None, None
        try:
            ex, ret = one_read(coll, cli, t, window_s)
        except Exception as e:  # noqa: BLE001
            msg = f"{type(e).__name__}: {str(e)[:200]}"
            timed_out = "timed out" in msg.lower() or "timeout" in type(e).__name__.lower()
            err = None if timed_out else msg
        secs = time.perf_counter() - t0
        b = fx(counters_client)
        row = {"stage": name, "i": i, "cliente": cli, "t": t.isoformat(), "secs": round(secs, 3), "docs_examined": ex,
               "returned": ret, "timed_out": timed_out, "error": err}
        if a and b:
            row.update(mb_fetched=round((b["get_bytes"] - a["get_bytes"]) / 1e6, 2), ranged_gets=b["get_range"] - a["get_range"],
                       rows_from_runs=b["rows_from_runs"] - a["rows_from_runs"])
            if ex:
                row["rows_per_doc_examined"] = round(row["rows_from_runs"] / ex, 1)
        rows.append(row)
        print(json.dumps(row), flush=True)
        if timed_out:
            break


def writes(client, dbname, names, n_customers, n_updates, seed):
    """The generator proof's writes: for sampled customers, 10% of their orders get a new state and +1.5 on the total."""
    from pymongo.write_concern import WriteConcern
    rng = random.Random(f"{seed}:writes")
    d = client[dbname]["ordini"].with_options(write_concern=WriteConcern(w="majority"))
    ms, errors, customers = [], [], 0
    t0 = time.time()
    for cli in rng.sample(names, min(n_customers, len(names))):
        if len(ms) >= n_updates:
            break
        customers += 1
        try:
            ids = [x["_id"] for x in d.find({"cliente": cli}, {"_id": 1})]
        except Exception as e:  # noqa: BLE001
            errors.append(f"find {cli}: {type(e).__name__}: {str(e)[:120]}")
            continue
        for oid in rng.sample(ids, max(1, len(ids) // 10)) if ids else []:
            if len(ms) >= n_updates:
                break
            t1 = time.perf_counter()
            try:
                d.update_one({"cliente": cli, "_id": oid}, {"$set": {"stato": rng.choice(STATES)}, "$inc": {"totale": 1.5}})
                ms.append((time.perf_counter() - t1) * 1000)
            except Exception as e:  # noqa: BLE001
                errors.append(f"update {cli}: {type(e).__name__}: {str(e)[:120]}")
                if len(errors) > 50:
                    break
    return {"updates": len(ms), "customers": customers, "seconds": round(time.time() - t0, 1), "errors": errors[:50],
            "n_errors": len(errors), "update_p50_ms": fn.pct(ms, 0.5), "update_p99_ms": fn.pct(ms, 0.99),
            "update_max_ms": round(max(ms), 3) if ms else None}


def settle(client, max_min, quiet_s=60, poll_s=5, sleep=time.sleep, now=time.time):
    """Wait until the compactor's counters stay unchanged for quiet_s, at most max_min minutes."""
    first = (fx(client) or {}).get("compactor")
    if first is None:
        return {"waited_s": 0, "quiet": None, "note": "not our server: no compactor counters"}
    last, since, t0 = first, now(), now()
    while now() - t0 < max_min * 60:
        sleep(poll_s)
        s = (fx(client) or {}).get("compactor")
        if s != last:
            last, since = s, now()
        elif now() - since >= quiet_s:
            return {"waited_s": round(now() - t0), "quiet": True, "compactor_before": first, "compactor_after": last,
                    "changed": last != first}
    return {"waited_s": round(now() - t0), "quiet": False, "compactor_before": first, "compactor_after": last,
            "changed": last != first}


def summarize(rows):
    out = {}
    for s in STAGES:
        sel = [r for r in rows if r["stage"] == s]
        done = [r for r in sel if not r["timed_out"] and not r["error"]]
        b = {"reads": len(sel), "answered": len(done), "timed_out": sum(1 for r in sel if r["timed_out"]),
             "errors": sum(1 for r in sel if r["error"])}
        if done:
            b["secs_p50"], b["secs_max"] = fn.pct([r["secs"] for r in done], 0.5), max(r["secs"] for r in done)
            b["docs_examined_p50"] = fn.pct([r["docs_examined"] or 0 for r in done], 0.5)
            for k in ("mb_fetched", "rows_from_runs", "rows_per_doc_examined"):
                v = [r[k] for r in done if k in r]
                if v:
                    b[f"{k}_p50"], b[f"{k}_max"] = fn.pct(v, 0.5), max(v)
        out[s] = b
    base = out["before"]
    for s in STAGES[1:]:
        b = out[s]
        if base.get("secs_p50") and b.get("secs_p50"):
            b["secs_p50_times_before"] = round(b["secs_p50"] / max(1e-9, base["secs_p50"]), 1)
        if base.get("rows_per_doc_examined_p50") and b.get("rows_per_doc_examined_p50"):
            b["rows_per_doc_times_before"] = round(b["rows_per_doc_examined_p50"] / max(1e-9, base["rows_per_doc_examined_p50"]), 1)
    return out


def text(summary):
    lines = [f"== reads after writes on {summary['db']}: {summary['reads_per_stage']} reads per stage, "
             f"{summary['writes'].get('updates')} updates on {summary['writes'].get('customers')} customers "
             f"(update p50 {summary['writes'].get('update_p50_ms')} ms, p99 {summary['writes'].get('update_p99_ms')} ms)"]
    for s in STAGES:
        b = summary["stages"][s]
        x = f"{s:12s}: {b['answered']} of {b['reads']} answered, {b['timed_out']} timed out, {b['errors']} errors"
        if b.get("secs_p50") is not None:
            x += f"; p50 {b['secs_p50']:.2f} s, slowest {b['secs_max']:.2f} s; documents examined p50 {b['docs_examined_p50']}"
        if "rows_per_doc_examined_p50" in b:
            x += f"; rows from runs per document p50 {b['rows_per_doc_examined_p50']}, fetched p50 {b.get('mb_fetched_p50')} MB"
        if "secs_p50_times_before" in b:
            x += f"; time x{b['secs_p50_times_before']} of before"
        if "rows_per_doc_times_before" in b:
            x += f", rows per document x{b['rows_per_doc_times_before']}"
        lines.append(x)
    st = summary.get("settle") or {}
    lines.append(f"settle: waited {st.get('waited_s')} s, quiet {st.get('quiet')}, compactor counters changed {st.get('changed')}")
    return "\n".join(lines)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("out")
    ap.add_argument("--uri", required=True)
    ap.add_argument("--states", default="")
    ap.add_argument("--db", default="")
    ap.add_argument("--customers-n", type=int, default=0)
    ap.add_argument("--reads", type=int, default=6)
    ap.add_argument("--updates", type=int, default=13000)
    ap.add_argument("--write-customers", type=int, default=150)
    ap.add_argument("--max-read-s", type=float, default=180.0)
    ap.add_argument("--settle-min", type=float, default=20.0)
    ap.add_argument("--seed", type=int, default=5)
    ap.add_argument("--examine", type=float, default=800.0, help="documents one read should examine (sizes the window)")
    a = ap.parse_args(argv)
    import pymongo
    os.makedirs(a.out, exist_ok=True)
    client = pymongo.MongoClient(a.uri, serverSelectionTimeoutMS=30000)
    reader = pymongo.MongoClient(a.uri, serverSelectionTimeoutMS=30000, socketTimeoutMS=int(a.max_read_s * 1000))
    dbname, n, st0 = a.db, a.customers_n, None
    if a.states:
        states = fn.find_states(a.states)
        on_server = set(client.list_database_names())
        cand = sorted((d for d in states if d in on_server and (not a.db or d == a.db)),
                      key=lambda d: -float((states[d].get("caricamento") or {}).get("byte_bson") or 0))
        if cand:
            dbname, n, st0 = cand[0], int(states[cand[0]]["clienti"]["n"]), states[cand[0]]
    rows, w, st = [], {}, {}
    win = window_seconds(st0, a.examine)
    if dbname and n:
        names = [f"cli-{i:06d}" for i in range(n)]
        reads = the_reads(names, a.reads, a.seed)
        stage(reader, client, dbname, reads, "before", rows, win)
        w = writes(client, dbname, names, a.write_customers, a.updates, a.seed)
        print(json.dumps({"writes": {k: v for k, v in w.items() if k != "errors"}}), flush=True)
        stage(reader, client, dbname, reads, "after-writes", rows, win)
        st = settle(client, a.settle_min)
        stage(reader, client, dbname, reads, "settled", rows, win)
    summary = {"program": "reads-after-writes", "db": dbname, "reads_per_stage": a.reads, "max_read_s": a.max_read_s,
               "seed": a.seed, "window_s": round(win, 1), "examine_target": a.examine, "our_counters": fx(client) is not None, "writes": w, "settle": st, "stages": summarize(rows)}
    with open(os.path.join(a.out, "reads-after-writes.jsonl"), "w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    ok = (summary["stages"]["before"]["answered"] > 0 and not any(b["errors"] for b in summary["stages"].values())
          and w.get("updates", 0) > 0 and not w.get("n_errors"))
    summary["ok"] = ok
    txt = text(summary) if rows else "no database or no customers: nothing measured"
    summary["text"] = txt
    json.dump(summary, open(os.path.join(a.out, "reads-after-writes-summary.json"), "w"), indent=1, default=str)
    open(os.path.join(a.out, "reads-after-writes.txt"), "w").write(txt + "\n")
    print(txt)
    print(f"READS-AFTER-WRITES: {'OK' if ok else 'FAILED'} (database {dbname or 'none'})")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
