#!/usr/bin/env python3
"""log-on-pair fix round: every write stop around each failover, as the writer saw it (acked.csv[.gz] of pair_kill9.py).

The reviewer's `review/unavailability.py` counts the time from the kill/stop of the waiter to the FIRST acknowledged
write after it. When the keeper leads, writes resume on the keeper and then stop a second time while it hands the lead
back to a data copy. This script also counts that second stop: for each cycle, every gap between two consecutive
acknowledgements longer than GAP_MS (default 500; the writer acknowledges about 1,000 writes/s but in bursts, with normal gaps up to about 0.25 s)
in the window [kill, kill + 5 s] (a cycle is at least 6 s long), and reports the first stop (= the reviewer's number), the hand-over stop, and the
total.

Usage: pair_stops.py KILLDIR [GAP_MS]
"""
import bisect, csv, gzip, json, os, statistics as st, sys

d = sys.argv[1]
GAP = float(sys.argv[2]) if len(sys.argv) > 2 else 500.0
p = os.path.join(d, 'acked.csv')
f = open(p) if os.path.exists(p) else gzip.open(p + '.gz', 'rt')
t = sorted(float(r[2]) for r in csv.reader(f) if len(r) >= 7 and r[5] in ('0', '1'))
ev = [json.loads(l) for l in open(os.path.join(d, 'cycles.jsonl'))]
first, second, total, rows = [], [], [], []
for e in ev:
    k = e.get('kill_unix_ms')
    if k is None:
        continue
    i = bisect.bisect_right(t, k)
    if i >= len(t):
        continue
    f0 = (t[i] - k) / 1000
    gaps = []
    j = i
    while j + 1 < len(t) and t[j + 1] < k + 5000:
        g = t[j + 1] - t[j]
        if g > GAP:
            gaps.append(g / 1000)
        j += 1
    later = sum(gaps)
    first.append(f0)
    second.append(later)
    total.append(f0 + later)
    rows.append(f"cycle {e['cycle']} {e['mode']} waiter r{e['waiter']} new leader r{e['new_leader']}: first stop {f0:.3f} s, later stops "
                + (', '.join(f'{g:.3f}' for g in gaps) or 'none') + f", total {f0 + later:.3f} s")
print('\n'.join(rows))
print(f'{d}: {len(first)} cycles; first stop (kill to first ack) median {st.median(first):.3f} s, max {max(first):.3f} s; '
      f'later stops (> {GAP:.0f} ms) median {st.median(second):.3f} s, max {max(second):.3f} s; '
      f'total median {st.median(total):.3f} s, max {max(total):.3f} s')
