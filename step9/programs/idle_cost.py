"""Step 9 · idle cost (founder's decision, 2026-11-13, for the AWS round): what an OPEN database with NO traffic costs on
our line, at each size: the S3 requests per second the servers make while nobody uses it, and the dollars per month
they come to at S3's prices.

Our servers count every request they send to S3 (tenantStats `requests`: get (whole and ranged), head, list, put
(single and multipart), copy, delete). The program:
  1. opens the database: one read of one customer (the first request of the job, if nothing else touched it);
  2. waits --settle-s (the opening's own work and any compaction it starts are left out; the counters are read every
     10 s and the wait ends early when they stay flat for 60 s);
  3. reads the counters of every server given (the cook, and the warm spare when there is one: it is part of the
     service, RULES "fair comparison"), waits --idle-s with no request of ours but those reads, reads them again.
Requests per second = the difference / the seconds; dollars per month = requests per second x 2,592,000 x the price of
each kind (S3 Standard eu-west-1, DECLARED as in ci/estimate.py: GET and HEAD $0.0004 per 1,000; PUT, COPY and LIST
$0.005 per 1,000; DELETE free). The counters are the whole process's: every database the server has open counts, and
the report lists them (listDatabases). The 3-zone log nodes are not read: with no write they upload nothing.

On a server that is not ours (no tenantStats) it says so and measures nothing: Atlas and mongod charge by the hour,
whatever the traffic; their idle cost is their price list, not a counter.

Usage: idle_cost.py OUT_DIR --uri URI [--spare-uri URI] (--states DIR | --db NAME) [--settle-s 300] [--idle-s 600]
  -> OUT_DIR/idle-cost-summary.json, idle-cost.txt; exit 1 only when the database cannot be opened or no counter reads."""
import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import first_next as fn  # noqa: E402

PRICE_PER_1000 = {"get": 0.0004, "head": 0.0004, "list": 0.005, "put": 0.005, "copy": 0.005, "delete": 0.0}
MONTH_S = 30 * 86400
KINDS = tuple(PRICE_PER_1000)


def counters(client):
    """The server's S3 request counters, or None when it has none (not our server)."""
    try:
        r = client.admin.command("tenantStats").get("requests") or {}
    except Exception:  # noqa: BLE001
        return None
    return {k: int(r.get(k, 0)) for k in KINDS}


def cost(per_s):
    """Dollars per month of a rate per second by kind."""
    return sum(per_s.get(k, 0.0) * MONTH_S * PRICE_PER_1000[k] / 1000 for k in KINDS)


def settle(clients, settle_s, flat_s=60, poll_s=10, sleep=time.sleep, now=time.monotonic):
    """Wait until every counter is flat for flat_s, at most settle_s. Returns (seconds waited, flat or not)."""
    t0 = now()
    last = [counters(c) for c in clients]
    since = now()
    while now() - t0 < settle_s:
        sleep(poll_s)
        cur = [counters(c) for c in clients]
        if cur != last:
            last, since = cur, now()
        elif now() - since >= flat_s:
            return round(now() - t0, 1), True
    return round(now() - t0, 1), False


def measure(clients, idle_s, sleep=time.sleep, now=time.monotonic):
    a = [counters(c) for c in clients]
    t0 = now()
    sleep(idle_s)
    b = [counters(c) for c in clients]
    secs = now() - t0
    per = {k: 0.0 for k in KINDS}
    by_server = []
    for x, y in zip(a, b):
        if x is None or y is None:
            by_server.append(None)
            continue
        d = {k: y[k] - x[k] for k in KINDS}
        by_server.append(d)
        for k in KINDS:
            per[k] += d[k] / secs
    return round(secs, 1), per, by_server


def main(argv=None, sleep=time.sleep, now=time.monotonic):
    ap = argparse.ArgumentParser()
    ap.add_argument("out")
    ap.add_argument("--uri", required=True)
    ap.add_argument("--spare-uri", default="")
    ap.add_argument("--states", default="")
    ap.add_argument("--db", default="")
    ap.add_argument("--settle-s", type=float, default=300.0)
    ap.add_argument("--idle-s", type=float, default=600.0)
    ap.add_argument("--label", default="")
    a = ap.parse_args(argv)
    import pymongo
    os.makedirs(a.out, exist_ok=True)
    cook = pymongo.MongoClient(a.uri, serverSelectionTimeoutMS=30000)
    clients = [cook] + ([pymongo.MongoClient(a.spare_uri, serverSelectionTimeoutMS=30000)] if a.spare_uri else [])
    dbname, st = a.db, None
    if a.states:
        states = fn.find_states(a.states)
        on = set(cook.list_database_names())
        cand = sorted((d for d in states if d in on and (not a.db or d == a.db)),
                      key=lambda d: -float((states[d].get("caricamento") or {}).get("byte_bson") or 0))
        if cand:
            dbname, st = cand[0], states[cand[0]]
    s = {"program": "idle-cost", "label": a.label, "db": dbname, "servers": len(clients), "prices_per_1000": PRICE_PER_1000}
    if counters(cook) is None:
        s.update(ok=True, ours=False, text=f"== idle cost: not our server (no tenantStats): nothing to count; its idle "
                                          f"cost is its price per hour")
    elif not dbname:
        s.update(ok=False, ours=True, text="== idle cost: no database to open")
    else:
        t0 = time.perf_counter()
        err = None
        try:
            cli = (st or {}).get("clienti", {}).get("nomi_da", "cli-%06d") % 1
            cook[dbname]["messaggi"].find_one({"cliente": cli})
        except Exception as e:  # noqa: BLE001
            err = f"{type(e).__name__}: {str(e)[:200]}"
        s["open_ms"] = round((time.perf_counter() - t0) * 1000, 1)
        s["open_error"] = err
        waited, flat = settle(clients, a.settle_s, sleep=sleep, now=now)
        secs, per, by_server = measure(clients, a.idle_s, sleep=sleep, now=now)
        try:
            s["databases_on_server"] = sorted(d for d in cook.list_database_names() if d not in ("admin", "local", "config"))
        except Exception:  # noqa: BLE001
            s["databases_on_server"] = None
        tot = sum(per.values())
        dollars = cost(per)
        per = {k: round(v, 6) for k, v in per.items()}
        s.update(ours=True, settle_s=waited, settled=flat, idle_s=secs, requests_per_s=per, requests_per_s_total=round(tot, 6),
                 by_server=by_server, dollars_per_month=round(dollars, 4), ok=err is None)
        s["text"] = (f"== idle cost of {dbname} ({a.label or 'no label'}), {len(clients)} server(s) ({'cook and spare' if len(clients) > 1 else 'cook'}), "
                     f"databases open on the cook: {s['databases_on_server']}\n"
                     f"opened in {s['open_ms']:.0f} ms{', ERROR ' + err if err else ''}; settled {'yes' if flat else 'NO (counters still moving)'} after {waited:.0f} s\n"
                     f"idle {secs:.0f} s: {tot:.3f} S3 requests per second (" + ", ".join(f"{k} {v:g}" for k, v in per.items() if v) +
                     f") = {dollars:.2f} $ per month at S3 prices (DECLARED)")
    json.dump(s, open(os.path.join(a.out, "idle-cost-summary.json"), "w"), indent=1)
    open(os.path.join(a.out, "idle-cost.txt"), "w").write(s["text"] + "\n")
    print(s["text"])
    print(f"IDLE-COST: {'OK' if s['ok'] else 'FAILED'}")
    return 0 if s["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
