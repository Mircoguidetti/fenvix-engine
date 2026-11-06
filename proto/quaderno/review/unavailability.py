#!/usr/bin/env python3
"""review of log-on-pair, point 3: write unavailability per failover AS THE WRITER SAW IT (time from the kill/stop of
the waiter to the next acknowledged write in acked.csv[.gz]), and who the status poll saw leading next (2 = the keeper; a short keeper turn can be missed: count its "guida" lines
in r2's log for the exact number).

Usage: unavailability.py KILLDIR [...]   (a pair_kill9.py output directory)
"""
import bisect, csv, gzip, json, os, statistics as st, sys

for d in sys.argv[1:]:
    p = os.path.join(d, 'acked.csv')
    f = open(p) if os.path.exists(p) else gzip.open(p + '.gz', 'rt')
    t = sorted(float(r[2]) for r in csv.reader(f) if len(r) >= 7 and r[5] in ('0', '1'))
    ev = [json.loads(l) for l in open(os.path.join(d, 'cycles.jsonl'))]
    g = []
    for e in ev:
        i = bisect.bisect_right(t, e['kill_unix_ms'])
        if i < len(t):
            g.append(((t[i] - e['kill_unix_ms']) / 1000, e['mode'], e['new_leader']))
    k = [x for x, m, _ in g if m == 'kill9']
    kk = [x for x, m, n in g if m == 'kill9' and n == 2]
    print(f'{d}: kill -9 cycles {len(k)}: median {st.median(k):.2f} s, max {max(k):.2f} s; keeper seen leading next in {len(kk)}'
          + (f' (median {st.median(kk):.2f} s)' if kk else '') + f'; all cycles median {st.median(x for x, _, _ in g):.2f} s')
