"""Step 9 · first and next, per request of the agent benchmark (coordinator, 2026-11-13; total review R1): the founder's
goal is a database of 1 TB and up where a customer's FIRST request may pay S3 and every NEXT request of that customer
is as fast as on MongoDB. This program times, on one database of the round (the 1 TB generated one, an Atlas-shaped
one, or the same data on a mongod), each of the five requests of the agent benchmark both ways:

  the customer is the thread, its `messaggi` (customer-keyed, newest first) are its checkpoints:
    resume       the newest message of the customer                    find {cliente} sort ts -1 limit 1
    last10       its 10 newest                                         find {cliente} sort ts -1 limit 10
    history      its whole history, newest first                       find {cliente} sort ts -1
    time_travel  the newest message at a past time T                   find {cliente, ts <= T} sort ts -1 limit 1
    save         one new message at the end of the customer's history  insert_one, w: majority

  FIRST of an operation: the operation sent as the very first request to a customer nobody read since the server
  started (cold). So each operation has its own group of N cold customers (5 groups, drawn with a fixed seed, never one
  of the founder's script's reserved customers; `--label` changes the draw, so a second run takes other customers).
  NEXT of an operation: the same operation sent to a customer whose first request was ANOTHER operation (the other
  four groups): 4 N samples. In a read group the save comes last, so the reads see the generated history only.

Per request: the time (the call and every document read); the server's counters just before and just after, outside the
timed region: on our server the GETs and bytes read from S3 (tenantStats requests.get, getBytes) and the rows read from
sealed runs (serverStatus fenvix.sealedRuns.rowsFromRuns); on a mongod the pages and bytes WiredTiger read into its cache
(serverStatus wiredTiger.cache), its equivalent of "it had to go outside memory"; `--counters none` for any other
server. A FIRST request that read nothing from outside (0 GETs, or 0 bytes into the cache) was not cold: counted,
reported, kept out of the FIRST line.

No verdict against a competitor here: first_next_compare.py puts two summaries side by side (ours and mongod's, the
same data, the same customers: same seed and label). What this program says alone: FIRST and NEXT p50/p99 per
operation, the GETs and bytes of each, and whether every NEXT read made 0 GETs.
The founder's script stays frozen; the time constants below are checked against it by the test.

Usage: first_next_ops.py OUT_DIR --uri URI (--states DIR | --db NAME --customers-n N) [--per-op 30] [--seed 9]
                         [--label before-writes] [--counters auto|fenvix|mongod|none] [--max-s 120]
  -> OUT_DIR/first-next-ops-summary.json, first-next-ops-requests.jsonl, first-next-ops.txt; exit 1 when a request
     failed, or no customer could be measured."""
import argparse
import datetime as dt
import json
import os
import random
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import first_next as fn  # noqa: E402  (find_states, pct: the same rules as the first program)

OPS = ("resume", "last10", "history", "time_travel", "save")
READS = OPS[:4]
NAMES = {"resume": "resume (newest 1)", "last10": "last 10", "history": "whole history", "time_travel": "time travel",
         "save": "save (insert 1, majority)"}
# the founder's script's timeline (prova_infinite.py T_INIZIO, T_SPAN_S), checked by the test against the frozen file
T_START = dt.datetime(2025, 11, 1, tzinfo=dt.timezone.utc)
T_SPAN_S = 365 * 86400


def groups(st, per_op, seed, label, db=None, customers_n=None, ops=None, spare=0):
    """{op: [customer names]}: one disjoint group of per_op customers per operation (5, or 4 with --no-save), never a
    reserved one, fixed by seed and label. `spare` more customers, outside every group, come back under "_spare"."""
    ops = ops or OPS
    if st:
        cl = st["clienti"]
        reserved, n, name = set(cl.get("riservati") or []), int(cl["n"]), cl.get("nomi_da", "cli-%06d")
        db = st["db"]
    else:
        reserved, n, name = set(), int(customers_n), "cli-%06d"
    pool = [i for i in range(n) if i not in reserved]
    pick = random.Random(f"{seed}:{label}:{db}").sample(pool, min(len(pool), per_op * len(ops) + spare))
    extra = pick[-spare:] if spare and len(pick) > len(ops) + spare else []
    if extra:
        pick = pick[:-spare]
    k = len(pick) // len(ops)
    out = {op: [name % i for i in pick[j * k:(j + 1) * k]] for j, op in enumerate(ops)}
    if spare:
        out["_spare"] = [name % i for i in extra]
    return out


def past_time(cli, seed):
    """T of the time travel: a time between 25% and 90% of the script's year, fixed by the customer and the seed."""
    return T_START + dt.timedelta(seconds=random.Random(f"{seed}:tt:{cli}").uniform(0.25, 0.90) * T_SPAN_S)


def new_message(cli, k):
    """One message in the script's shape (doc_messaggio), marked so it can be told from the generated ones."""
    return {"cliente": cli, "ts": dt.datetime.now(dt.timezone.utc), "conv": "conv-s9", "ruolo": "user", "modello": "s9",
            "token": 200, "testo": ("first next ops " * 20)[:200], "s9": f"first-next-ops:{k}"}


def run_op(db, op, cli, seed, max_ms, k=0):
    """Send one request; returns the number of documents read (1 for a save)."""
    c = db["messaggi"]
    s = [("ts", -1)]
    if op == "resume":
        return len(list(c.find({"cliente": cli}).sort(s).limit(1).max_time_ms(max_ms)))
    if op == "last10":
        return len(list(c.find({"cliente": cli}).sort(s).limit(10).max_time_ms(max_ms)))
    if op == "history":
        return sum(1 for _ in c.find({"cliente": cli}).sort(s).max_time_ms(max_ms))
    if op == "time_travel":
        return len(list(c.find({"cliente": cli, "ts": {"$lte": past_time(cli, seed)}}).sort(s).limit(1).max_time_ms(max_ms)))
    if op == "save":
        from pymongo.write_concern import WriteConcern
        c.with_options(write_concern=WriteConcern(w="majority")).insert_one(new_message(cli, k))
        return 1
    raise ValueError(op)


def order_for(first_op, save=True, rot=0):
    """The requests of one customer: its FIRST, then the other reads as NEXT, then (in a read group) the save, last.
    save=False (--no-save): the four reads only, nothing is written.
    rot (review of the AWS plan, point 4): the customer's index in its group picks a row of a balanced Latin square
    (Williams) over the NEXT reads: within every n rows (3 reads: 6 rows, 4 reads: 4) each read takes each position
    equally and follows each other read equally (a plain rotation would keep time travel right after a whole-history read
    in 2 orders of 3: its most favourable place); the summary also reports NEXT by position."""
    reads = [o for o in READS if o != first_op]
    rows = williams(len(reads))
    rest = [reads[i] for i in rows[rot % len(rows)]] + (["save"] if save and first_op != "save" else [])
    return [first_op] + rest


def williams(n):
    """A balanced Latin square of order n (Williams, 1949): n rows when n is even, 2n when odd (with the reversed rows)."""
    if n <= 1:
        return [list(range(n))]
    first, lo, hi = [0], 1, n - 1
    while len(first) < n:
        first.append(lo)
        lo += 1
        if len(first) < n:
            first.append(hi)
            hi -= 1
    rows = [[(x + i) % n for x in first] for i in range(n)]
    return rows + [r[::-1] for r in rows] if n % 2 else rows


# ---------- the server's counters ----------
def counters_fenvix(client):
    """The process-wide GETs and bytes (tenantStats), the rows read from sealed runs, and `background`: a snapshot of
    every counter of the server's own background work (review of the AWS plan, point 2: the GET counters are the whole
    process's, so they also count our compactor's and the GC's reads). A request during which `background` changed is
    left out of FIRST and NEXT (summarize) and counted; no foreground-only counter is added to the engine."""
    t = client.admin.command("tenantStats")
    r, m = t.get("requests") or {}, t.get("maintenance") or {}
    out = {"gets": int(r.get("get", 0)), "bytes": int(r.get("getBytes", 0)),
           "maintenance_gets": int(m.get("compactGets", 0)) + int(m.get("gcGets", 0))}
    bg = {"maintenance": out["maintenance_gets"]}
    try:
        f = client.admin.command("serverStatus")["fenvix"]
        out["rows_from_runs"] = int(f["sealedRuns"]["rowsFromRuns"])
        for k in ("compactor", "compaction", "backgroundReads"):
            if k in f:
                bg[k] = f[k]
    except Exception:  # noqa: BLE001 - an engine without the counters: the row count is left out, nothing else changes
        pass
    out["background"] = bg
    return out


def background_moved(c0, c1):
    """Did the server's background work move between the two snapshots: a counter changed, or a pass or compactor job
    was running at either end (`compaction.running` > 0)."""
    a, b = c0.get("background"), c1.get("background")
    if a is None or b is None:
        return False
    if a != b:
        return True
    return any(int(((x.get("compaction") or {}).get("running") or 0)) > 0 for x in (a, b))


def counters_mongod(client):
    c = client.admin.command("serverStatus")["wiredTiger"]["cache"]
    return {"gets": int(c["pages read into cache"]), "bytes": int(c["bytes read into cache"])}


def pick_counters(client, kind):
    """(name, function or None). auto: our server if it answers tenantStats, else mongod if it has WiredTiger, else none."""
    if kind in ("auto", "fenvix"):
        try:
            counters_fenvix(client)
            return "fenvix", counters_fenvix
        except Exception:  # noqa: BLE001
            if kind == "fenvix":
                raise
    if kind in ("auto", "mongod"):
        try:
            counters_mongod(client)
            return "mongod", counters_mongod
        except Exception:  # noqa: BLE001
            if kind == "mongod":
                raise
    return "none", None


def open_database(client, dbname, cli, max_ms):
    """One read of a customer outside every group, before anything is measured: on a cook that has not served the
    database yet it pays the opening (the manifest, the maps), which is not any customer's first request. Its time is
    kept apart ("how long until a new server answers on this database")."""
    t0 = time.perf_counter()
    err = None
    try:
        client[dbname]["messaggi"].find_one({"cliente": cli}, max_time_ms=max_ms)
    except Exception as e:  # noqa: BLE001 - recorded; the measured requests that follow say whether the server answers
        err = f"{type(e).__name__}: {str(e)[:200]}"
    return {"cliente": cli, "ms": round((time.perf_counter() - t0) * 1000, 3), "error": err}


def measure_customer(client, dbname, cli, first_op, seed, max_ms, cnt, rows, k, save=True):
    db = client[dbname]
    for pos, op in enumerate(order_for(first_op, save, rot=k)):
        c0 = cnt(client) if cnt else {}
        t0 = time.perf_counter()
        err, n = None, None
        try:
            n = run_op(db, op, cli, seed, max_ms, k)
        except Exception as e:  # noqa: BLE001 - a failed request is recorded and makes the program fail
            err = f"{type(e).__name__}: {str(e)[:200]}"
        ms = (time.perf_counter() - t0) * 1000
        c1 = cnt(client) if cnt else {}
        row = {"db": dbname, "cliente": cli, "group": first_op, "op": op, "role": "first" if pos == 0 else "next",
               "pos": pos, "ms": round(ms, 3), "docs": n, "error": err}
        for key in ("gets", "bytes", "maintenance_gets", "rows_from_runs"):
            if key in c0 and key in c1:
                row[key] = c1[key] - c0[key]
        if "background" in c0 and "background" in c1:
            row["background_moved"] = background_moved(c0, c1)
        rows.append(row)


def line_of(sel):
    if not sel:
        return None
    out = {"n": len(sel), "p50_ms": fn.pct([r["ms"] for r in sel], 0.5), "p99_ms": fn.pct([r["ms"] for r in sel], 0.99),
           "max_ms": round(max(r["ms"] for r in sel), 3), "docs_p50": fn.pct([r["docs"] or 0 for r in sel], 0.5)}
    for key in ("gets", "bytes", "rows_from_runs"):
        v = [r[key] for r in sel if key in r]
        if v:
            out[f"{key}_p50"], out[f"{key}_max"] = fn.pct(v, 0.5), max(v)
    m = [r["maintenance_gets"] for r in sel if "maintenance_gets" in r]
    if m:
        out["maintenance_gets_total"] = sum(m)
    return out


def summarize(rows, has_counters):
    """Per operation: FIRST (cold customers only) and NEXT, and NEXT by position; whether every NEXT read made 0 GETs.
    A request during which the server's background work moved (`background_moved`) is left out of both lines, since
    its GETs may be the compactor's: counted per operation and in all (`dropped_background`). Its customer's cold test
    then cannot be read either, so a FIRST dropped this way drops that customer's NEXT requests too."""
    ok_rows = [r for r in rows if not r["error"]]
    dropped = [r for r in ok_rows if r.get("background_moved")]
    clean = [r for r in ok_rows if not r.get("background_moved")]
    firsts = [r for r in clean if r["role"] == "first"]
    cold = [r for r in firsts if not has_counters or r.get("bytes", 0) > 0 or r.get("gets", 0) > 0 or r["op"] == "save"]
    cold_customers = {r["cliente"] for r in cold}
    out = {"customers": len({r["cliente"] for r in rows}), "cold_customers": len(cold_customers),
           "not_cold_at_first": len(firsts) - len(cold), "errors": len(rows) - len(ok_rows),
           "dropped_background": len(dropped), "dropped_background_first": sum(1 for r in dropped if r["role"] == "first"),
           "ops": {}}
    for op in OPS:
        if not any(r["op"] == op for r in rows):
            continue   # --no-save: no save line at all
        f = [r for r in cold if r["op"] == op]
        n = [r for r in clean if r["role"] == "next" and r["op"] == op and r["cliente"] in cold_customers]
        o = {"first": line_of(f), "next": line_of(n), "dropped_background": sum(1 for r in dropped if r["op"] == op)}
        if op != "save":
            o["next_by_position"] = {str(p): line_of([r for r in n if r["pos"] == p]) for p in sorted({r["pos"] for r in n})}
        if o["first"] and o["next"]:
            o["first_minus_next_p50_ms"] = round(o["first"]["p50_ms"] - o["next"]["p50_ms"], 3)
        if o["next"] and "gets_max" in o["next"]:
            o["next_gets_zero"] = o["next"]["gets_max"] == 0
        out["ops"][op] = o
    return out


def text(summary):
    b = summary
    lines = [f"== {b['db']} ({b['label']}; counters: {b['counters']}): {b['customers']} customers, {b['cold_customers']} cold at "
             f"their first request, {b['not_cold_at_first']} not cold (left out of FIRST), {b['errors']} errors, "
             f"{b.get('dropped_background', 0)} requests left out because the server's background work moved during them "
             f"({b.get('dropped_background_first', 0)} of them FIRST)"]
    if b.get("opening"):
        lines.append(f"opening the database (one read of a customer outside the groups, before the measure): "
                     f"{b['opening']['ms']:.0f} ms" + (f", ERROR {b['opening']['error']}" if b['opening'].get('error') else ""))
    for op in OPS:
        o = b["ops"].get(op)
        if not o:
            continue
        for role in ("first", "next"):
            x = o[role]
            if not x:
                lines.append(f"{role.upper():5s} {NAMES[op]}: no measure")
                continue
            extra = ""
            if "gets_p50" in x:
                extra = f"; GETs p50 {x['gets_p50']} max {x['gets_max']}; bytes p50 {x['bytes_p50']} max {x['bytes_max']}"
            if "rows_from_runs_p50" in x:
                extra += f"; rows from runs p50 {x['rows_from_runs_p50']}"
            lines.append(f"{role.upper():5s} {NAMES[op]}: n={x['n']} p50 {x['p50_ms']:.2f} ms p99 {x['p99_ms']:.2f} ms max "
                         f"{x['max_ms']:.0f} ms; documents p50 {x['docs_p50']}{extra}")
        bypos = o.get("next_by_position") or {}
        if bypos:
            lines.append(f"      NEXT {NAMES[op]} by position: " + "; ".join(
                f"{p}: n={x['n']} p50 {x['p50_ms']:.2f} p99 {x['p99_ms']:.2f}" for p, x in bypos.items() if x))
        if "next_gets_zero" in o and op != "save":
            lines.append(f"      NEXT {NAMES[op]} without S3: {'yes, 0 GETs' if o['next_gets_zero'] else 'NO, it still reads from outside'}")
    return "\n".join(lines)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("out")
    ap.add_argument("--uri", required=True)
    ap.add_argument("--states", default="", help="folder with <db>/risultati_p1.json: the largest database on the server is taken")
    ap.add_argument("--db", default="", help="the database, when there is no state file (with --customers-n)")
    ap.add_argument("--customers-n", type=int, default=0, help="number of customers cli-000000.. of --db")
    ap.add_argument("--per-op", type=int, default=30, help="cold customers per operation (5 groups)")
    ap.add_argument("--seed", type=int, default=9)
    ap.add_argument("--label", default="before-writes")
    ap.add_argument("--counters", default="auto", choices=("auto", "fenvix", "mongod", "none"))
    ap.add_argument("--max-s", type=float, default=120.0, help="the most one read may take (maxTimeMS)")
    ap.add_argument("--no-save", action="store_true", help="the four reads only: nothing is written into the database "
                    "(for a pantry that later jobs must read as loaded, e.g. the pilot before job M1)")
    a = ap.parse_args(argv)
    import pymongo
    os.makedirs(a.out, exist_ok=True)
    client = pymongo.MongoClient(a.uri, serverSelectionTimeoutMS=30000)
    st, dbname = None, a.db
    if a.states:
        states = fn.find_states(a.states)
        on_server = set(client.list_database_names())
        cand = sorted((d for d in states if d in on_server and (not a.db or d == a.db)),
                      key=lambda d: -float((states[d].get("caricamento") or {}).get("byte_bson") or 0))
        if cand:
            dbname, st = cand[0], states[cand[0]]
    cname, cnt = pick_counters(client, a.counters) if a.counters != "none" else ("none", None)
    rows, opening = [], None
    the_ops = READS if a.no_save else OPS
    if dbname and (st or a.customers_n):
        g = groups(st, a.per_op, a.seed, a.label, dbname, a.customers_n, the_ops, spare=1)
        if g.get("_spare"):
            opening = open_database(client, dbname, g["_spare"][0], int(a.max_s * 1000))
        # one customer of each group in turn, so every operation's FIRST is spread over the whole run
        for i in range(max(len(g[op]) for op in the_ops)):
            for op in the_ops:
                if i < len(g[op]):
                    measure_customer(client, dbname, g[op][i], op, a.seed, int(a.max_s * 1000), cnt, rows, i, save=not a.no_save)
    # the customers measured, as a fingerprint: first_next_compare.py pairs two runs only when they measured the same
    # customers (aws-local-round: ours and mongod's looked like one plan with other customers behind it)
    import hashlib
    fp = hashlib.sha256(json.dumps([[r["cliente"], r["op"]] for r in rows]).encode()).hexdigest()[:16] if rows else None
    summary = {"program": "first-next-ops", "db": dbname, "label": a.label, "seed": a.seed, "per_op": a.per_op,
               "customers_fingerprint": fp,
               "saves": not a.no_save, "counters": cname, "opening": opening, "requests": len(rows),
               **summarize(rows, cnt is not None)}
    with open(os.path.join(a.out, "first-next-ops-requests.jsonl"), "w") as f:
        for r in rows:
            f.write(json.dumps(r, default=str) + "\n")
    txt = text(summary)
    summary["text"] = txt
    ok = bool(rows) and summary["cold_customers"] > 0 and summary["errors"] == 0
    summary["ok"] = ok
    json.dump(summary, open(os.path.join(a.out, "first-next-ops-summary.json"), "w"), indent=1)
    open(os.path.join(a.out, "first-next-ops.txt"), "w").write(txt + "\n")
    print(txt)
    print(f"FIRST-NEXT-OPS: {'OK' if ok else 'FAILED'} (database {dbname or 'none'}, {summary['cold_customers']} cold customers, "
          f"{summary['errors']} errors)")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
