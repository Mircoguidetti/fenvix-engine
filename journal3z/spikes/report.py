#!/usr/bin/env python3
"""Markdown tables for docs/journal3z-libraries.md from journal3z/results/spikes/*.json."""
import json, glob, os, statistics
R = os.path.join(os.path.dirname(__file__), '..', 'results', 'spikes')
def load(n):
    p = os.path.join(R, n + '.json')
    return json.load(open(p)) if os.path.exists(p) else None
cands = [('proto', 'proto-latency', 'proto'), ('custom', 'custom-latency', 'custom'), ('openraft (1 entry per write)', 'openraft-latency', 'openraft'),
         ('openraft (batched entries)', 'openraft-batched', 'openraft-batched'), ('raft-rs', 'raftrs-latency', 'raftrs'),
         ('omnipaxos (BLE on)', 'omnipaxos-latency', 'omnipaxos'), ('omnipaxos (BLE off)', 'omnipaxos-bleoff-latency', 'omnipaxos-bleoff')]
print('### Latency and throughput, conc 8, 1 KB, one-way delay 0.5–1.0 ms (3 runs; median run by p99; all 3 p99 shown)\n')
print('| candidate | p50 ms | p95 ms | p99 ms (median run) | p99 of runs 1/2/3 | max ms | writes/s | CPU µs/write (excl. net) | CPU µs/write (all) | peak RSS MB | msgs/write | errors |')
print('|---|---|---|---|---|---|---|---|---|---|---|---|')
med = {}
for name, pre, key in cands:
    runs = [load(f'{pre}-r{i}') for i in (1, 2, 3)]
    runs = [r['result'] for r in runs if r]
    if not runs:
        continue
    s = sorted(runs, key=lambda r: r['p99_ms'])
    m = s[len(s) // 2]
    med[key] = m
    print(f"| {name} | {m['p50_ms']:.2f} | {m['p95_ms']:.2f} | **{m['p99_ms']:.2f}** | {' / '.join('%.2f' % r['p99_ms'] for r in runs)} | {m['max_ms']:.1f} | {m['writes_per_s']:.0f} | {m['cpu_us_per_write_excl_net']:.0f} | {m['cpu_us_per_write']:.0f} | {m['hwm_mb']:.0f} | {m['messages_per_write']:.2f} | {sum(r['errors'] for r in runs)} |")
print('\n### Concurrency 64 and secondary network (one-way 1–2 ms), one run each\n')
print('| candidate | conc 64: p50 / p99 ms | conc 64: writes/s | conc 64: CPU µs/write excl. net | delay 1–2 ms, conc 8: p50 / p99 ms | writes/s |')
print('|---|---|---|---|---|---|')
for name, c64, d12 in [('proto', 'proto-conc64', 'proto-delay1-2'), ('custom', 'custom-conc64', 'custom-delay1-2'), ('openraft (1 entry per write)', 'openraft-conc64', 'openraft-delay1-2'),
                       ('openraft (batched)', 'openraft-batched-conc64', 'openraft-batched-delay1-2'), ('raft-rs', 'raftrs-conc64', 'raftrs-delay1-2'),
                       ('omnipaxos (BLE on)', 'omnipaxos-conc64', 'omnipaxos-delay1-2'), ('omnipaxos (BLE off)', 'omnipaxos-bleoff-conc64', None)]:
    a, b = load(c64), load(d12) if d12 else None
    fa = lambda r: f"{r['result']['p50_ms']:.2f} / {r['result']['p99_ms']:.2f} | {r['result']['writes_per_s']:.0f} | {r['result']['cpu_us_per_write_excl_net']:.0f}" if r else '– | – | –'
    fb = lambda r: f"{r['result']['p50_ms']:.2f} / {r['result']['p99_ms']:.2f} | {r['result']['writes_per_s']:.0f}" if r else '– | –'
    print(f'| {name} | {fa(a)} | {fb(b)} |')
print('\n### Idle groups (10 s after start-up; messages through the delay line; CPU of the whole process and without the delay-line thread)\n')
print('| candidate | groups | start-up s | messages/s | CPU % of one core (excl. net) | RSS MB |')
print('|---|---|---|---|---|---|')
for c, label in [('proto', 'proto'), ('custom', 'custom'), ('openraft', 'openraft'), ('raftrs', 'raft-rs'), ('omnipaxos', 'omnipaxos (BLE on)'), ('omnipaxos-bleoff', 'omnipaxos (BLE off)')]:
    for g in (1, 100, 1000):
        r = load(f'{c}-idle-{g}')
        if not r:
            print(f'| {label} | {g} | failed / not run | | | |')
            continue
        x = r['result']
        print(f"| {label} | {g} | {x.get('startup_s', 0):.2f} | {x['messages_per_s']:.0f} | {x['cpu_pct_of_one_core']:.1f} ({x['cpu_pct_excl_net']:.1f}) | {x['rss_mb']:.0f} |")
