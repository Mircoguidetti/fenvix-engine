#!/usr/bin/env python3
"""Q8c · analysis of q8c.jsonl against the thresholds written before the runs.

LIKE ATLAS (locally) for a phase: the normal databases have p50 (from send, minus the ping p50) <= 2.5 ms and p99
(from send, minus the ping p50) <= 5 ms, and zero rejections. Atlas M30 w:majority is 2.2-2.5 ms p50 "minus the hello"
(MISURATO 2026-11-05); its p99 was not valid, so "p99 close" is fixed here at twice Atlas' p50.
A phase counts only if VALID (no core above 90% on average, normals' generator sent >= 98% of what was asked).
On AWS (`--replicas DIR`, the r0/r1/r2 result folders with campioni.jsonl) the per-core check also covers the three
replica machines, over the phase's [t_start, t_end].

Usage: q8c_analyze.py q8c.jsonl [--replicas DIR]
"""
import json, sys, statistics


def opt(a, k, d=None):
    return a[a.index(k) + 1] if k in a else d


args = sys.argv[1:]
rows = [json.loads(l) for l in open(args[0]) if l.strip()]
rdir = opt(args, '--replicas')
samples = {}
if rdir:
    import os
    for r in ('r0', 'r1', 'r2'):
        p = os.path.join(rdir, r, 'campioni.jsonl')
        if os.path.exists(p):
            samples[r] = [json.loads(l) for l in open(p) if l.strip()]


def replica_cpu(t0, t1):
    out = {}
    for r, ss in samples.items():
        w = [s['percore'] for s in ss if t0 <= s['t'] <= t1 and 'percore' in s]
        if w:
            out[r] = max(sum(x[k] for x in w) / len(w) for k in w[0])
    return out


ref = [r for r in rows if r['level'] == 1]
ref_p99 = statistics.median(r['normal']['lat_send']['p99_us'] for r in ref) / 1000 if ref else None
ref_p50 = statistics.median(r['normal']['lat_send']['p50_us'] for r in ref) / 1000 if ref else None
print(f"reference (1 database alone, median of {len(ref)}): p50 from send {ref_p50} ms, p99 from send {ref_p99} ms")
verdict = {}
for r in rows:
    n = r['normal']
    ping = n.get('ping', {}).get('p50_us', 0) / 1000
    p50 = n['lat_send']['p50_us'] / 1000 - ping
    p99 = n['lat_send']['p99_us'] / 1000 - ping
    p999 = n['lat_send']['p999_us'] / 1000 - ping
    valid = r['valid']
    rc = replica_cpu(r['t_start'], r.get('t_end', r['t_start'] + 60)) if samples else {}
    if rc and max(rc.values()) > 90:
        valid = False
    like = p50 <= 2.5 and p99 <= 5.0 and n['rejected'] == 0
    per_log = n.get('per_log', [])
    nl = r.get('noisy_logs', [])
    same = [f"{x['p99_us']/1000:.1f}" for i, x in enumerate(per_log) if i in nl and x['n']]
    other = [f"{x['p99_us']/1000:.1f}" for i, x in enumerate(per_log) if i not in nl and x['n']]
    key = (r['level'], r['phase'])
    print(f"level {r['level']:>3} {r['phase']:<9} {'VALID    ' if valid else 'NOT VALID'} {'LIKE ATLAS' if like else 'NOT YET   '} "
          f"p50 {p50:.2f} p99 {p99:.2f} p99.9 {p999:.2f} ms (send - ping {ping:.2f}); vs alone p99 {p99 + ping - (ref_p99 or 0):+.2f} | "
          f"p99 on the noisy log(s) {same} other logs {other} | max core {r['percore'].get('max_core_mean_pct', 0):.0f}% replicas {rc} | "
          f"sent {n.get('sent_per_s', 0):.0f}/{r['asked_per_s']} rej {n['rejected']} busy {n['busy_retries']} total {r['recs_per_s_total']:.0f} rec/s")
    if valid:
        verdict.setdefault(r['level'], []).append(like)
print()
for lv in sorted({r['level'] for r in rows}):
    v = verdict.get(lv, [])
    nv = sum(1 for r in rows if r['level'] == lv) - len(v)
    print(f"level {lv}: valid phases {len(v)}, like Atlas {sum(v)}, not yet {len(v) - sum(v)}, NOT VALID {nv}")
