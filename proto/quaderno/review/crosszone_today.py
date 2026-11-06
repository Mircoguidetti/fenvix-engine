#!/usr/bin/env python3
"""review of log-on-pair, point 4: where the "about 3.9 crossings per write today" comes from.

From the committed phases (pair_bench.py output) only. Today's shape: the log leader is in the waiter's zone for 1
customer in 3 (base-near) and in another zone for 2 in 3 (base-far); both are MEASURED. The pair: pair-2000. Nothing
in the branch measures a feed of the warm spare in today's shape, so any term for it is an ASSUMPTION: +1.0 copy if
the spare reads every record from another zone, 0 if it tails the log replica of its own zone (the cell has one per zone).

Usage: crosszone_today.py phases.jsonl [...]
"""
import json, statistics, sys
from collections import defaultdict

rows = [json.loads(l) for p in sys.argv[1:] for l in open(p) if l.strip()]
CROSS = lambda r, far: (r.startswith('r') or r in ('c->r1', 'c->r2') or (far and r == 'c->r0'))
g = defaultdict(list)
for r in rows:
    rb, n = r['route_bytes'], r['writer']['ok_per_s'] * r['secs']
    x = sum(a + b for k, (a, b) in rb.items() if CROSS(k, r['case'] == 'base-far')) / n
    g[(r['writes'], r['case'])].append(x / (rb['r0->r1'][0] / n))
for w in sorted({k[0] for k in g}):
    c = {k: statistics.median(v) for (ww, k), v in g.items() if ww == w}
    today = c['base-near'] / 3 + 2 * c['base-far'] / 3
    pair = c.get('pair-2000', c.get('pair-0'))
    print(f'{w} writes/s: MEASURED crossings per write: base-near {c["base-near"]:.2f}, base-far {c["base-far"]:.2f}, pair {pair:.2f}')
    print(f'  today, weighted 1/3 near + 2/3 far (MEASURED parts): {today:.2f} -> pair {100 * (pair / today - 1):+.0f}%')
    print(f'  today + 1.0 for a cross-zone spare feed (ESTIMATED, not measured): {today + 1:.2f} -> pair {100 * (pair / (today + 1) - 1):+.0f}%')
    print(f'  leader-near customers only (MEASURED): pair vs today {100 * (pair / c["base-near"] - 1):+.0f}%')
