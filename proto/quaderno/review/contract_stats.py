#!/usr/bin/env python3
"""review of log-on-pair, point 1 of the contract: how sure is the KEEP?

Pools every pair-2000 and base-near phase at 1,000 writes/s from the given phases.jsonl files (pair_bench.py output),
and prints, per file and pooled: the median p99 of each case (from the send, as pair_analyze.py), the difference, the
per-repetition differences (same run, same rep number), and a bootstrap 90% interval of the difference of medians with
the probability that it is below 1 ms. KEEP needs < 1 ms, DROP > 3 ms (the contract).

Usage: contract_stats.py phases.jsonl [...]
"""
import json, random, statistics as st, sys


def load(p):
    return [json.loads(l) for l in open(p) if l.strip()]


def p99(rs, case, key='lat_send'):
    return {r['rep']: r['writer'][key]['p99_us'] / 1000 for r in rs if r['writes'] == 1000 and r['case'] == case}


def verdict(d):
    return 'KEEP' if d < 1 else ('DROP' if d > 3 else 'NEITHER')


B, P = [], []
for f in sys.argv[1:]:
    rs = load(f)
    b, p = p99(rs, 'base-near'), p99(rs, 'pair-2000')
    if not b or not p:
        continue
    d = st.median(p.values()) - st.median(b.values())
    print(f'{f}: base-near {sorted(b.values())} pair-2000 {sorted(p.values())} -> {d:+.2f} ms {verdict(d)}; '
          f'per rep {[round(p[k] - b[k], 2) for k in sorted(set(b) & set(p))]}')
    B += list(b.values())
    P += list(p.values())
d = st.median(P) - st.median(B)
random.seed(1)
bs = sorted(st.median(random.choices(P, k=len(P))) - st.median(random.choices(B, k=len(B))) for _ in range(20000))
print(f'POOLED ({len(B)} + {len(P)} phases): base-near {st.median(B):.2f} ms, pair-2000 {st.median(P):.2f} ms -> {d:+.2f} ms {verdict(d)}; '
      f'bootstrap 90% interval [{bs[1000]:+.2f}, {bs[19000]:+.2f}] ms, P(< 1 ms) = {sum(x < 1 for x in bs) / len(bs):.2f}')
