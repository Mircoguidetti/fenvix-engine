"""Client-slow operations vs the server's lines. Usage: correlate.py OPS.jsonl COOK.log [min_ms]"""
import json, sys, collections, gzip
ops_p, log_p = sys.argv[1], sys.argv[2]; thr = float(sys.argv[3]) if len(sys.argv) > 3 else 30
op = lambda p: gzip.open(p, 'rt') if p.endswith('.gz') else open(p)
cmds, stalls, msgs = [], [], []
for l in op(log_p):
    if l.startswith(('WAITS ', 'WAITS_STALL', 'WAITS_MSG')):
        d = dict(kv.split('=', 1) for kv in l.split()[1:])
        if 'u' not in d: continue
        u0 = float(d['u']) / 1000; u1 = u0 + float(d['ms']) / 1000
        (cmds if l.startswith('WAITS ') else stalls if l.startswith('WAITS_STALL') else msgs).append((u0, u1, d))
cats = ["admit","lock_w","lock_r","refresh","stage","stage_snap","log_ack","visible","fallback","wal_flush","io_wait","comp_start","read_get","read_scan"]
def ov(a0, a1, b0, b1): return max(0.0, min(a1, b1) - max(a0, b0))
res = collections.Counter(); n = 0
for l in op(ops_p):
    o = json.loads(l)
    if o.get('phase') != 'write' or o['op'] not in ('put', 'put_writes', 'resume') or o['ms'] < thr: continue
    n += 1; t1 = o['ts']; t0 = t1 - o['ms'] / 1000
    srv = [c for c in cmds if ov(t0, t1, c[0], c[1]) > 0 and c[2]['cmd'] not in ('hello',)]
    st = sum(ov(t0, t1, s[0], s[1]) for s in stalls) * 1000
    best = max(srv, key=lambda c: c[1] - c[0], default=None)
    if best and (best[1] - best[0]) * 1000 >= 0.5 * o['ms']:
        d = best[2]; k = max(cats, key=lambda k: float(d.get(k, '0/0').split('/')[0]))
        res['server command: ' + k] += 1
    elif st >= 0.3 * o['ms']:
        res['runtime stalled (outside the command)'] += 1
    else:
        res['not seen in the server (client, network or host)'] += 1
print(f'client ops over {thr} ms in the write phase: {n}')
for k, v in res.most_common(): print(f'  {v:5d} {k}')
