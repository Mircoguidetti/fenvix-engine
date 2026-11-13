"""Step 9 · first and next (total review R1 point 2, 2026-11-12): the founder's goal is that a customer's FIRST request may
pay S3 and the NEXT requests of that customer are as fast as the others. Nothing in the round measured the next ones:
the founder's script repeats the same query (any cache serves it) and never sends a different request of the same
customer. This program does, on the databases of the round (the Atlas-shaped ones in job M2, the big ballast ones in
job B), through the measured cook, with the cook's own counters around every request:

  for N cold customers of a database (drawn with a fixed seed from the state file's customers, never one of the
  founder's script's reserved customers, its own cold pool):
    FIRST   messaggi {cliente} sort ts -1 limit 20          (Q1: the customer's latest messages)
    NEXT 1  eventi   {cliente} sort ts -1 limit 100         (Q3's shape: a different collection)
    NEXT 2  ordini   group by stato, count and sum totale   (an aggregate: a third collection)
    NEXT 3  messaggi {cliente, ts < its middle} limit 20     (Q1d's shape: an older page of the first collection)
    HOT     FIRST again                                     (the warm baseline: "as fast as the others")
  per request: the time (the call and every result read), the GETs and the bytes the cook read from S3 (tenantStats
  requests.get and getBytes, read just before and just after, outside the timed region), and the maintenance GETs in
  the same window (background work, given apart: the request does not own them).

A customer whose FIRST made no GET was not cold (its blocks were already cached): counted and reported, kept out of the
FIRST line. Verdict per NEXT line, as bench-big-speed's firstnext.py: NEXT / HOT at p50 and p99, EQUAL within 10%,
FASTER under 0.9, SLOWER above 1.1; and the GETs: NEXT must make 0 GETs for "as fast as the others" to hold on S3.
The founder's script stays frozen; this is a separate program, like write-peak.

Usage: first_next.py OUT_DIR --uri URI --states DIR [--customers 30] [--max-dbs 2] [--seed 9] [--db NAME]
  DIR: folders holding <db>/risultati_p1.json (the founder's script's state files: the Atlas ones, the ballast ones);
  the largest databases present on the server are taken first (their state's caricamento.byte_bson).
  -> OUT_DIR/first-next-summary.json, OUT_DIR/first-next-requests.jsonl, a FIRST/NEXT text; exit 1 when a request
     failed or no database or customer could be measured."""
import argparse
import json
import os
import random
import sys
import time

KINDS = ("first", "next1", "next2", "next3", "hot")
NAMES = {"first": "FIRST  messaggi latest 20", "next1": "NEXT 1 eventi latest 100", "next2": "NEXT 2 ordini by stato",
         "next3": "NEXT 3 messaggi older page", "hot": "HOT    the first again"}


def verdict(r):
    """bench-big-speed's firstnext.py: within 10% is EQUAL."""
    return "FASTER" if r < 0.9 else ("EQUAL" if r <= 1.1 else "SLOWER")


def pct(xs, q):
    xs = sorted(xs)
    return round(xs[min(len(xs) - 1, int(q * len(xs)))], 3) if xs else None


def find_states(root):
    out = {}
    for dirpath, _, files in os.walk(root):
        if "risultati_p1.json" in files:
            try:
                st = json.load(open(os.path.join(dirpath, "risultati_p1.json")))
            except Exception:  # noqa: BLE001 - an unreadable state file is not a database to measure
                continue
            if st.get("db") and (st.get("clienti") or {}).get("n"):
                out[st["db"]] = st
    return out


def customers(st, n, seed):
    """n customer names of the state, never a reserved one (the founder's script's cold pool), fixed seed."""
    cl = st["clienti"]
    reserved = set(cl.get("riservati") or [])
    pool = [i for i in range(int(cl["n"])) if i not in reserved]
    pick = random.Random(f"{seed}:{st['db']}").sample(pool, min(n, len(pool)))
    return [cl.get("nomi_da", "cli-%06d") % i for i in pick]


def counters(client):
    t = client.admin.command("tenantStats")
    r, m = t.get("requests") or {}, t.get("maintenance") or {}
    return {"get": int(r.get("get", 0)), "get_bytes": int(r.get("getBytes", 0)),
            "maint_get": int(m.get("compactGets", 0)) + int(m.get("gcGets", 0))}


def requests_of(db, cli):
    """The five requests of one customer, as (kind, callable returning the number of documents read)."""
    s = [("ts", -1)]

    def first():
        return len(list(db["messaggi"].find({"cliente": cli}).sort(s).limit(20)))

    def next1():
        return len(list(db["eventi"].find({"cliente": cli}).sort(s).limit(100)))

    def next2():
        p = [{"$match": {"cliente": cli}}, {"$group": {"_id": "$stato", "n": {"$sum": 1}, "t": {"$sum": "$totale"}}}, {"$sort": {"_id": 1}}]
        return len(list(db["ordini"].aggregate(p)))

    def next3():
        # an older page: the messages before the middle of what the FIRST request saw (no extra read to find it)
        mid = state.get("mid")
        q = {"cliente": cli} if mid is None else {"cliente": cli, "ts": {"$lt": mid}}
        return len(list(db["messaggi"].find(q).sort(s).limit(20)))

    state = {}

    def first_and_mid():
        docs = list(db["messaggi"].find({"cliente": cli}).sort(s).limit(20))
        if docs:
            state["mid"] = docs[len(docs) // 2].get("ts")
        return len(docs)

    return [("first", first_and_mid), ("next1", next1), ("next2", next2), ("next3", next3), ("hot", first)]


def measure(client, dbname, cli, out_rows):
    db = client[dbname]
    rows = []
    for kind, fn in requests_of(db, cli):
        c0 = counters(client)
        t0 = time.perf_counter()
        err, n = None, None
        try:
            n = fn()
        except Exception as e:  # noqa: BLE001 - a failed request is recorded and makes the program fail
            err = f"{type(e).__name__}: {str(e)[:200]}"
        ms = (time.perf_counter() - t0) * 1000
        c1 = counters(client)
        row = {"db": dbname, "cliente": cli, "kind": kind, "ms": round(ms, 3), "docs": n, "error": err,
               "gets": c1["get"] - c0["get"], "get_bytes": c1["get_bytes"] - c0["get_bytes"],
               "maintenance_gets": c1["maint_get"] - c0["maint_get"]}
        rows.append(row)
        out_rows.append(row)
    return rows


def summarize(rows):
    """Per database and over all: per kind p50/p99 of ms, GETs and bytes; the NEXT verdicts against HOT."""
    def block(rs):
        cold = [r for r in rs if r["kind"] == "first" and r["gets"] > 0 and not r["error"]]
        cold_customers = {(r["db"], r["cliente"]) for r in cold}
        out = {"customers": len({(r["db"], r["cliente"]) for r in rs}), "cold_customers": len(cold_customers),
               "not_cold_at_first": len({(r["db"], r["cliente"]) for r in rs if r["kind"] == "first"}) - len(cold_customers),
               "errors": sum(1 for r in rs if r["error"])}
        for k in KINDS:
            sel = [r for r in rs if r["kind"] == k and (r["db"], r["cliente"]) in cold_customers and not r["error"]]
            if not sel:
                out[k] = None
                continue
            out[k] = {"n": len(sel), "p50_ms": pct([r["ms"] for r in sel], 0.5), "p99_ms": pct([r["ms"] for r in sel], 0.99),
                      "gets_p50": pct([r["gets"] for r in sel], 0.5), "gets_max": max(r["gets"] for r in sel),
                      "get_bytes_p50": pct([r["get_bytes"] for r in sel], 0.5), "get_bytes_max": max(r["get_bytes"] for r in sel),
                      "maintenance_gets_total": sum(r["maintenance_gets"] for r in sel)}
        hot = out.get("hot")
        for k in ("next1", "next2", "next3"):
            if out.get(k) and hot:
                v = {}
                for q in ("p50_ms", "p99_ms"):
                    r = out[k][q] / max(1e-9, hot[q])
                    v[q] = {"ratio_to_hot": round(r, 2), "verdict": verdict(r)}
                v["gets_zero"] = out[k]["gets_max"] == 0
                out[k]["against_hot"] = v
        return out

    by_db = {}
    for r in rows:
        by_db.setdefault(r["db"], []).append(r)
    return {"all": block(rows), "by_db": {d: block(rs) for d, rs in sorted(by_db.items())}}


def text(summary):
    lines = []
    for name, b in [("all databases", summary["all"])] + list(summary["by_db"].items()):
        lines.append(f"== {name}: {b['customers']} customers, {b['cold_customers']} cold at their first request, "
                     f"{b['not_cold_at_first']} not cold (left out), {b['errors']} errors")
        for k in KINDS:
            x = b.get(k)
            if not x:
                lines.append(f"{NAMES[k]}: no measure")
                continue
            vs = ""
            if x.get("against_hot"):
                a = x["against_hot"]
                vs = (f" | vs HOT p50 x{a['p50_ms']['ratio_to_hot']} {a['p50_ms']['verdict']}, p99 x{a['p99_ms']['ratio_to_hot']} "
                      f"{a['p99_ms']['verdict']}, GETs {'0' if a['gets_zero'] else 'NOT 0'}")
            lines.append(f"{NAMES[k]}: n={x['n']} p50 {x['p50_ms']:.2f} ms p99 {x['p99_ms']:.2f} ms; GETs p50 {x['gets_p50']} "
                         f"max {x['gets_max']}; bytes p50 {x['get_bytes_p50']} max {x['get_bytes_max']}; "
                         f"background GETs in the window {x['maintenance_gets_total']}{vs}")
    return "\n".join(lines)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("out")
    ap.add_argument("--uri", required=True)
    ap.add_argument("--states", required=True)
    ap.add_argument("--customers", type=int, default=30)
    ap.add_argument("--max-dbs", type=int, default=2)
    ap.add_argument("--seed", type=int, default=9)
    ap.add_argument("--db", default="", help="only this database (review of the AWS harness: the command's ballast "
                    "database, when the pantry holds more than one)")
    a = ap.parse_args(argv)
    import pymongo
    os.makedirs(a.out, exist_ok=True)
    client = pymongo.MongoClient(a.uri, serverSelectionTimeoutMS=30000)
    states = find_states(a.states)
    on_server = set(client.list_database_names())
    dbs = sorted((d for d in states if d in on_server and (not a.db or d == a.db)),
                 key=lambda d: -float((states[d].get("caricamento") or {}).get("byte_bson") or 0))[: a.max_dbs]
    rows = []
    for d in dbs:
        for cli in customers(states[d], a.customers, a.seed):
            measure(client, d, cli, rows)
    summary = {"program": "first-next", "databases": dbs, "customers_per_db": a.customers, "seed": a.seed,
               "states_found": sorted(states), "requests": len(rows), **summarize(rows)}
    with open(os.path.join(a.out, "first-next-requests.jsonl"), "w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    txt = text(summary)
    summary["text"] = txt
    ok = bool(dbs) and summary["all"]["cold_customers"] > 0 and summary["all"]["errors"] == 0
    summary["ok"] = ok
    json.dump(summary, open(os.path.join(a.out, "first-next-summary.json"), "w"), indent=1)
    open(os.path.join(a.out, "first-next.txt"), "w").write(txt + "\n")
    print(txt)
    print(f"FIRST-NEXT: {'OK' if ok else 'FAILED'} ({len(dbs)} databases {dbs}, {summary['all']['cold_customers']} cold customers, "
          f"{summary['all']['errors']} errors)")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
