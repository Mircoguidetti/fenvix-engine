"""perf script (with -g) of the runtime worker thread -> busy streaks (consecutive non-idle samples) and what ran in
them. Usage: perf script -i X | perf_streaks.py MIN_MS"""
import sys, re, collections
min_ms = float(sys.argv[1]) if len(sys.argv) > 1 else 10
samples = []; cur = None
for line in sys.stdin:
    if not line.strip():
        if cur: samples.append(cur); cur = None
        continue
    if not line.startswith((' ', '\t')):
        m = re.search(r'\s(\d+\.\d+):', line)
        cur = [float(m.group(1)), []] if m else None
    elif cur is not None:
        parts = line.strip().split(None, 1)
        cur[1].append(parts[1] if len(parts) > 1 else parts[0])
if cur: samples.append(cur)
IDLE = ('do_epoll_wait', 'epoll_wait', 'futex_wait', 'schedule_hrtimeout')
KEYS = [('memtable_flusher', 'slatedb memtable flush'), ('wal_buffer', 'slatedb WAL flush'), ('WalBuffer', 'slatedb WAL flush'),
        ('compaction_worker', 'slatedb compaction worker'), ('compactor::', 'slatedb compactor'), ('garbage_collector', 'slatedb gc'),
        ('hyper::proto', 'http client io (hyper)'), ('hyper::client', 'http client io (hyper)'), ('reqwest', 'http client (reqwest)'),
        ('handle_connection', 'command'), ('tenant_bench::fx::', 'fx'), ('locallog', 'locallog'), ('qlog', 'log client'), ('quaderno', 'log client'),
        ('lifecycle::Maintenance', 'maintenance'), ('lifecycle::', 'lifecycle'), ('slatedb::', 'slatedb other'), ('mongo_server::', 'server other')]
def cls(fr):
    s = ' '.join(fr)
    for k, v in KEYS:
        if k in s: return v
    return 'other: ' + (fr[0][:60] if fr else '?')
def idle(fr): return any(any(i in f for i in IDLE) for f in fr[:8])
streaks = []; st = None
for t, fr in samples:
    if idle(fr):
        if st: streaks.append(st); st = None
        continue
    if st and t - st[1] <= 0.0025:
        st[1] = t; st[2].append(cls(fr)); st[3].append(fr)
    else:
        if st: streaks.append(st)
        st = [t, t, [cls(fr)], [fr]]
if st: streaks.append(st)
busy = collections.Counter(c for s in streaks for c in s[2])
print('samples', len(samples), 'busy', sum(busy.values()))
print('busy by class:', busy.most_common(12))
long = [s for s in streaks if (s[1] - s[0]) * 1000 >= min_ms]
print(f'streaks >= {min_ms} ms:', len(long))
agg = collections.Counter(); ms = collections.Counter()
for s in long:
    c = collections.Counter(s[2]).most_common(1)[0][0]; agg[c] += 1; ms[c] += (s[1] - s[0]) * 1000
print('dominant class of long streaks:', [(k, v, round(ms[k])) for k, v in agg.most_common()])
for s in sorted(long, key=lambda s: s[0] - s[1])[:6]:
    print(f"\n-- {round((s[1]-s[0])*1000)} ms", collections.Counter(s[2]).most_common(3))
    fr = collections.Counter(f for frs in s[3] for f in set(frs[:25]) if 'slatedb' in f or 'tenant_bench' in f or 'mongo_server' in f or 'hyper' in f)
    for f, n in fr.most_common(8): print('   ', n, f[:160])
