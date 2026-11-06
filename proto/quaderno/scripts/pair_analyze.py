#!/usr/bin/env python3
"""log-on-pair: summary of pair_bench.py results (phases.jsonl) against the contract of the prompt.

Per case and write rate: median over the repetitions of the ack p50/p99 (from the send), validity of every phase
(core mean <= 90%, writer >= 98% of the asked rate, reads >= 98% of the asked rate), reads p99, disk traffic, cross-zone
bytes per acknowledged write, CPU of r0 per acknowledged record. Then the verdict: pair-2000 minus base-near at 1,000
writes/s (median p99): KEEP < 1 ms, DROP > 3 ms.

Usage: pair_analyze.py phases.jsonl [more.jsonl ...] > analysis.txt
"""
import json, statistics, sys
from collections import defaultdict

rows = []
for p in sys.argv[1:]:
    rows += [json.loads(l) for l in open(p) if l.strip()]

CROSS = lambda r, far: (r.startswith('r') or r in ('c->r1', 'c->r2') or (far and r == 'c->r0'))


def valid(r):
    why = []
    if r['core_mean'] and max(r['core_mean'].values()) > 90:
        why.append('core mean %.0f%%' % max(r['core_mean'].values()))
    w = r['writer']
    if w.get('sent_per_s', 0) < 0.98 * r['writes']:
        why.append('writer sent %.0f/s' % w.get('sent_per_s', 0))
    if r['reads'] and r['reader'].get('ops_per_s', 0) < 0.98 * r['reads']:
        why.append('reads done %.0f/s' % r['reader'].get('ops_per_s', 0))
    return why


def med(v):
    v = [x for x in v if x is not None]
    return statistics.median(v) if v else None


g = defaultdict(list)
for r in rows:
    g[(r['writes'], r['case'])].append(r)

ORDER = ['base-near', 'base-far', 'pair-0', 'pair-2000', 'pair-8000', 'cold-2000', 'cold-8000']
summary = {}
for w in sorted({k[0] for k in g}):
    print(f'=== {w} writes/s')
    for case in ORDER:
        rs = g.get((w, case))
        if not rs:
            continue
        p50 = [r['writer']['lat_send']['p50_us'] / 1000 for r in rs]
        p99 = [r['writer']['lat_send']['p99_us'] / 1000 for r in rs]
        p999 = [r['writer']['lat_send']['p999_us'] / 1000 for r in rs]
        inval = [(r['rep'], valid(r)) for r in rs if valid(r)]
        far = case == 'base-far'
        xb, recb, cpu_rec, dr, drb, dw, dfl = [], [], [], [], [], [], []
        for r in rs:
            ok = r['writer']['ok']
            secs = r['secs']
            # acknowledged writes in the counter window: rate x seconds (the writer's own count covers its 60 s window)
            n = r['writer']['ok_per_s'] * secs
            rb = r['route_bytes']
            xb.append(sum(a + b for k, (a, b) in rb.items() if CROSS(k, far)) / n)
            # one copy of a write: what the leader sends to ONE follower per record (r0->r1 when r0 leads)
            recb.append(rb.get('r0->r1', [0, 0])[0] / n)
            if r['cpu_s'].get('r0') is not None:
                cpu_rec.append(r['cpu_s']['r0'] / n * 1e6)
            dr.append(r['disk']['reads'] / secs)
            drb.append(r['disk']['read_bytes'] / secs / 1e6)
            dw.append(r['disk']['writes'] / secs)
            dfl.append(r['disk'].get('flushes', 0) / secs)
        rd = [r['reader'].get('p99_us', 0) / 1000 for r in rs if r['reads']]
        rdn = [r['reader'].get('ops_per_s', 0) for r in rs if r['reads']]
        ms = [r['cpu_s'].get('mongo-server') / r['secs'] for r in rs if r['cpu_s'].get('mongo-server') is not None]
        cores = {c: round(med([r['core_mean'][c] for r in rs]), 0) for c in rs[0]['core_mean']}
        s = {'p50': med(p50), 'p99': med(p99), 'p999': med(p999), 'p99_all': p99, 'p50_all': p50, 'invalid': inval,
             'xzone_bytes_per_write': med(xb), 'copy_bytes_per_write': med(recb), 'r0_cpu_us_per_rec': med(cpu_rec),
             'disk_reads_s': med(dr), 'disk_read_MBs': med(drb), 'disk_writes_s': med(dw), 'disk_flushes_s': med(dfl),
             'reads_p99_ms': med(rd), 'reads_done_s': med(rdn), 'ms_cores': med(ms), 'cores_mean': cores}
        summary[(w, case)] = s
        print(f'{case:10s} ack p50 {s["p50"]:.2f} ms  p99 {s["p99"]:.2f} ms (reps {", ".join("%.2f" % x for x in p99)})  p99.9 {s["p999"]:.2f} ms'
              f'  | invalid: {inval or "none"}')
        print(f'{"":10s} cross-zone bytes/write {s["xzone_bytes_per_write"]:.0f}  one copy (r0->r1) {s["copy_bytes_per_write"]:.0f}'
              f'  crossings {s["xzone_bytes_per_write"] / s["copy_bytes_per_write"]:.2f}  | r0 CPU {s["r0_cpu_us_per_rec"]:.0f} us/record'
              + (f'  mongo-server {s["ms_cores"]:.2f} cores' if s['ms_cores'] else ''))
        print(f'{"":10s} disk: reads {s["disk_reads_s"]:.0f}/s {s["disk_read_MBs"]:.1f} MB/s, writes {s["disk_writes_s"]:.0f}/s, flushes {s["disk_flushes_s"]:.0f}/s'
              + (f'  | reads done {s["reads_done_s"]:.0f}/s p99 {s["reads_p99_ms"]:.1f} ms' if s['reads_done_s'] else '') + f'  | cores {cores}')
    print()

for w in sorted({k[0] for k in g}):
    b = summary.get((w, 'base-near'))
    if not b:
        continue
    for case in ORDER[1:]:
        s = summary.get((w, case))
        if s:
            print(f'{w} writes/s: {case} - base-near: p99 {s["p99"] - b["p99"]:+.2f} ms, p50 {s["p50"] - b["p50"]:+.2f} ms')
    print()

b, p = summary.get((1000, 'base-near')), summary.get((1000, 'pair-2000'))
if b and p:
    d = p['p99'] - b['p99']
    v = 'KEEP' if d < 1 else ('DROP' if d > 3 else 'NEITHER (between 1 and 3 ms)')
    print(f'VERDICT: {v}: p99 {d:+.2f} ms at 2,000 reads/s and 1,000 writes/s (pair-2000 {p["p99"]:.2f} ms vs base-near {b["p99"]:.2f} ms, medians of {len(p["p99_all"])} and {len(b["p99_all"])})')
