#!/usr/bin/env python3
"""review r2 of log-on-pair: are there write stops after the first one (the keeper's hand-back) shorter than
pair_stops.py's 500 ms threshold? For each cycle: sum of gaps between consecutive acknowledgements longer than GAP ms
in [first ack after the kill, kill + 5 s], minus the same sum in a quiet window of equal length before the kill
(the writer acknowledges in bursts, so short gaps exist without any failover). Excess > 0 = extra stop after the first.

Usage: stops_vs_baseline.py KILLDIR [GAP_MS ...]
"""
import bisect, csv, gzip, json, os, statistics as st, sys

d = sys.argv[1]
gaps_ms = [float(x) for x in sys.argv[2:]] or [100, 150, 250]
p = os.path.join(d, 'acked.csv')
f = open(p) if os.path.exists(p) else gzip.open(p + '.gz', 'rt')
t = sorted(float(r[2]) for r in csv.reader(f) if len(r) >= 7 and r[5] in ('0', '1'))
ev = [json.loads(l) for l in open(os.path.join(d, 'cycles.jsonl')) if 'kill_unix_ms' in l]


def gapsum(a, b, g):
    i, j, s = bisect.bisect_left(t, a), bisect.bisect_right(t, b), 0.0
    for k in range(i, j - 1):
        if t[k + 1] - t[k] > g:
            s += t[k + 1] - t[k]
    return s / 1000


for g in gaps_ms:
    ex, kk = [], []
    for e in ev:
        k = e['kill_unix_ms']
        i = bisect.bisect_right(t, k)
        if i >= len(t):
            continue
        a = t[i]
        L = k + 5000 - a
        if L <= 0:
            continue
        x = gapsum(a, k + 5000, g) - gapsum(k - 1000 - L, k - 1000, g)
        ex.append(x)
        if e.get('new_leader') == 2:
            kk.append(x)
    print(f'{d} gap > {g:.0f} ms: excess later stop per cycle median {st.median(ex):+.3f} s, max {max(ex):+.3f} s'
          + (f'; cycles the poll saw the keeper lead ({len(kk)}): median {st.median(kk):+.3f} s' if kk else ''))
