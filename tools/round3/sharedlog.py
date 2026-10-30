"""Calculation only (nothing built): S3 PUTs of a write log SHARED by the databases of one server.

Model: every write of every database of the server is acknowledged after the next shared PUT; the server issues one
PUT per window of W ms that contains at least one write (a window with no write sends nothing). Acknowledged after S3,
so a crash loses nothing. The databases still need their own objects (the shared log is not a database): each database
written in a minute later gets its data into its own WAL/SST, here at most one PUT per database per minute it was written
(the "materialization" of the shared log, an assumption, stated in the report).

Traces (simulated, 1 hour, seed fixed):
  fleet100   100 databases, 1 write/s each (Poisson)
  mixed      50 databases in bursts (1 write/s for 30 s every 300 s, phases spread), 20 reading only, 30 idle
  20x4-5     20 databases, 4 or 5 active at any time (the active set changes every 60 s), 10 writes/s each when active
Compared with the measured modes (results-round3/minio): immediate ~1 PUT per write, relaxed 1 PUT per written database
per 5 s, local log 1 PUT per written database per 30 s.
usage: sharedlog.py > results-round3/sharedlog.json"""
import json
import math
import random

PUT = 0.005 / 1000
MONTH = 30 * 86400
H = 3600.0
rng = random.Random(7)


def poisson_times(rate, t0, t1):
    t, out = t0, []
    while True:
        t += rng.expovariate(rate)
        if t >= t1:
            return out
        out.append(t)


def trace(kind):
    """list of (time, db) of the writes in one hour"""
    w = []
    if kind == "fleet100":
        for d in range(100):
            w += [(t, d) for t in poisson_times(1.0, 0, H)]
        n = 100
    elif kind == "mixed":
        for d in range(50):
            phase = rng.uniform(0, 300)
            k = 0
            while True:
                s = phase + 300 * k - 300
                k += 1
                if s >= H:
                    break
                a, b = max(0, s), min(H, s + 30)
                if b > a:
                    w += [(t, d) for t in poisson_times(1.0, a, b)]
        n = 100
    else:
        n = 20
        for m in range(int(H // 60)):
            active = rng.sample(range(20), rng.choice([4, 5]))
            for d in active:
                w += [(t, d) for t in poisson_times(10.0, m * 60, (m + 1) * 60)]
    w.sort()
    return w, n


def shared(writes, window):
    wins = {int(t / window) for t, _ in writes}
    return len(wins)


def per_db_windows(writes, window):
    return len({(d, int(t / window)) for t, d in writes})


out = {"assumptions": {"put_usd": PUT, "month_s": MONTH, "materialization": "1 PUT per database per minute in which it was written"}, "traces": {}}
for kind in ("fleet100", "mixed", "20x4-5"):
    w, n = trace(kind)
    mat = per_db_windows(w, 60.0)
    r = {"databases": n, "writes_per_s": round(len(w) / H, 2), "rows": []}

    def row(name, puts):
        ps = puts / H
        r["rows"].append({"mode": name, "put_per_s": round(ps, 2), "usd_server_month": round(ps * MONTH * PUT, 2), "usd_db_month": round(ps * MONTH * PUT / n, 3)})
    row("immediate (1 PUT per write, measured ~0.95-1.0)", len(w))
    row("relaxed 5 s (per database)", per_db_windows(w, 5.0))
    row("local log 30 s (per database)", per_db_windows(w, 30.0))
    for ms in (50, 100, 250, 1000):
        s = shared(w, ms / 1000)
        row(f"shared log {ms} ms (log only)", s)
        row(f"shared log {ms} ms + materialization", s + mat)
    out["traces"][kind] = r
print(json.dumps(out, indent=1))
