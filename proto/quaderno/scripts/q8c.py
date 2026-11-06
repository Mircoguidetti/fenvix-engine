#!/usr/bin/env python3
"""Q8c · The shared log "like Atlas": several logs per cell, the per-database door, per-core CPU check.

For each level (number of databases in the cell) this starts LOGS groups of 3 replicas (one log each, its own leader
and threads, preferred leaders spread over the replicas), one fake flusher per log (no S3), then runs the phases.
Databases go to log (database number % LOGS). Normal databases write 100 records/s each (1.9 KB documents, open
loop); the confirm latency is measured from the scheduled instant. Noisy neighbours are separate generator processes.

Validity (RULES + Q8c prompt): a phase is VALID only if
  - no core of the machine is above 90% busy on average over the phase (here the whole machine is the system under
    test: replicas, generators and flushers share the same cores; per-core samples every second), and
  - the normals' generator sent at least 98% of what was asked (the load sender is not the limit).
Invalid phases are run and written anyway, marked NOT VALID, never dropped.

Usage: q8c.py OUT [--logs 4] [--levels 1,2,20,50,100] [--phases nessuno,mille,ventimila,raffiche,mega,muore,dieci]
              [--secs 60] [--secs-small 120] [--warm 10] [--tag name] [--ref-repeats 3]
Env: PQ_DOOR=1 PQ_DOOR_RECS=... (door), PQ_CORSIE=1 (lanes), PQ_RICICLA=1, Q8_REMOTE=1 + Q8_LOGS_REPLICAS (AWS mode:
     the replicas are already running on other machines; "a,b,c;d,e,f" one group per log).
"""
import json, os, signal, subprocess, sys, threading, time

HERE = os.path.dirname(os.path.abspath(__file__))
PQ = os.environ.get('PQ_BIN') or (os.path.join(HERE, 'pq') if os.path.exists(os.path.join(HERE, 'pq')) else os.path.join(HERE, '..', 'target', 'release', 'pq'))
WORK = os.environ.get('PQ_WORK', '/root/pq-work')
REMOTE = os.environ.get('Q8_REMOTE') == '1'
HZ = os.sysconf('SC_CLK_TCK')


def opt(a, k, d):
    return a[a.index(k) + 1] if k in a else d


args = sys.argv[1:]
OUT = args[0]
LOGS = int(opt(args, '--logs', '4'))
LEVELS = [int(x) for x in opt(args, '--levels', '1,2,20,50,100').split(',')]
PHASES = opt(args, '--phases', 'nessuno,mille,ventimila,raffiche,mega,muore,dieci').split(',')
SECS = int(opt(args, '--secs', '60'))
SECS_SMALL = int(opt(args, '--secs-small', '120'))
WARM = int(opt(args, '--warm', '10'))
TAG = opt(args, '--tag', f'q8c-logs{LOGS}')
REF_REPEATS = int(opt(args, '--ref-repeats', '3'))
# first normal database (Q8c diagnosis: --normal-from 1 puts a level-2 normal on another log than the neighbour 1000)
NORMAL_FROM = int(opt(args, '--normal-from', '0'))
WINDOW = opt(args, '--window-ms', '10000')
os.makedirs(OUT, exist_ok=True)
ENV = dict(os.environ, PQ_BIN=PQ)
ENV.setdefault('PQ_CORSIE', '1')
ENV.setdefault('PQ_RICICLA', '1')
ENV.setdefault('PQ_PREALLOC_MBS', '64')
LOGF = open(os.path.join(OUT, f'gen-{TAG}.log'), 'ab')

NOISY = {
    'mille': (['--mode', 'steady', '--rate', '1000', '--deadline-ms', '5000'], False),
    'ventimila': (['--mode', 'steady', '--rate', '20000', '--deadline-ms', '2000', '--inflight', '20000'], False),
    'raffiche': (['--mode', 'burst', '--burst', '50000', '--batch', '1000', '--burst-every-ms', '10000', '--deadline-ms', '10000', '--inflight', '64'], False),
    'mega': (['--mode', 'big', '--rate', '20', '--big-kb', '1024', '--deadline-ms', '5000', '--inflight', '256'], False),
    'muore': (['--mode', 'steady', '--rate', '5000', '--deadline-ms', '3000', '--inflight', '20000'], True),
}
DIECI_ORDER = ['ventimila', 'raffiche', 'mega', 'muore', 'mille']


def sh(cmd, **kw):
    return subprocess.run(cmd, capture_output=True, text=True, **kw)


def genv(g):
    return dict(ENV, PQ_WORK=os.path.join(WORK, f'g{g}'), PQ_PORT_OFF=str(1000 * g), PQ_PREF=str(g % 3),
                # Q8d (reviewer C4): each log refuses the databases of the other logs
                PQ_LOG_ID=str(g), PQ_LOGS=str(LOGS))


def group_addrs(g):
    if REMOTE:
        return os.environ['Q8_LOGS_REPLICAS'].split(';')[g]
    return ','.join(f'127.0.0.1:{1000 * g + 7200 + i}' for i in range(3))


def all_addrs():
    return ';'.join(group_addrs(g) for g in range(LOGS))


def cluster(g, *a):
    return sh(['python3', os.path.join(HERE, 'cluster.py')] + list(a), env=genv(g), timeout=120)


def status(g):
    o = sh([PQ, 'status', '--replicas', group_addrs(g)], timeout=20).stdout
    return [json.loads(l) for l in o.splitlines() if l.strip()]


def pids(g):
    try:
        return json.load(open(os.path.join(WORK, f'g{g}', 'pids.json')))
    except Exception:
        return {}


def proc_cpu(pid):
    try:
        f = open(f'/proc/{pid}/stat').read().rsplit(')', 1)[1].split()
        return (int(f[11]) + int(f[12])) / HZ
    except Exception:
        return None


def rss_mb(pid):
    try:
        for l in open(f'/proc/{pid}/status'):
            if l.startswith('VmRSS'):
                return int(l.split()[1]) / 1024
    except Exception:
        return None


def cores():
    out = {}
    for l in open('/proc/stat'):
        if l.startswith('cpu') and l[3].isdigit():
            f = l.split()
            v = [int(x) for x in f[1:]]
            out[f[0]] = (v[3] + v[4], sum(v))
    return out


def diskstats():
    best = None
    for l in open('/proc/diskstats'):
        f = l.split()
        if f[2].startswith('loop') or f[2].startswith('ram'):
            continue
        d = {'dev': f[2], 'writes': int(f[7]), 'sectors_w': int(f[9]), 'flush': int(f[18]) if len(f) > 18 else 0}
        if best is None or d['writes'] > best['writes']:
            best = d
    return best or {}


class PerCore(threading.Thread):
    """Per-core busy % every second."""

    def __init__(self):
        super().__init__(daemon=True)
        self.samples = []
        self.stop = False

    def run(self):
        a = cores()
        while not self.stop:
            time.sleep(1)
            b = cores()
            self.samples.append({k: 100.0 * (1 - (b[k][0] - a[k][0]) / max(1, b[k][1] - a[k][1])) for k in b})
            a = b

    def summary(self):
        if not self.samples:
            return {}
        ks = sorted(self.samples[0])
        mean = {k: sum(s[k] for s in self.samples) / len(self.samples) for k in ks}
        over95 = {k: sum(1 for s in self.samples if s[k] > 95) / len(self.samples) for k in ks}
        return {'mean_pct': mean, 'frac_s_over_95': over95, 'max_core_mean_pct': max(mean.values()), 'n': len(self.samples)}


class Gen:
    def __init__(self, name, gargs, kill, db_from):
        self.name, self.gargs, self.kill, self.db_from = name, gargs, kill, db_from
        self.p = None
        self.stop = False
        self.kills = 0
        self.cpu = 0.0

    def cmd(self):
        return ['nice', '-n', '5', PQ, 'corsie', '--replicas', all_addrs(), '--db-from', str(self.db_from), '--dbs', '1', '--secs', '100000', '--warm', '0',
                '--label', f'{self.name}-{self.db_from}', '--threads', '2', '--conns', '16'] + self.gargs

    def start(self):
        self.p = subprocess.Popen(self.cmd(), stdout=subprocess.DEVNULL, stderr=LOGF)
        if self.kill:
            threading.Thread(target=self.killer, daemon=True).start()

    def killer(self):
        while not self.stop:
            time.sleep(8)
            if self.stop:
                break
            self.cpu += proc_cpu(self.p.pid) or 0
            self.p.send_signal(signal.SIGKILL)
            self.p.wait()
            self.kills += 1
            time.sleep(2)
            if self.stop:
                break
            self.p = subprocess.Popen(self.cmd(), stdout=subprocess.DEVNULL, stderr=LOGF)

    def end(self):
        self.stop = True
        self.cpu += proc_cpu(self.p.pid) or 0
        self.p.send_signal(signal.SIGKILL)
        self.p.wait()


def noisy_for(phase, level):
    if phase == 'nessuno':
        return []
    if phase == 'dieci':
        k = max(1, level // 10)
        return [DIECI_ORDER[i % len(DIECI_ORDER)] for i in range(k)]
    return [phase]


def up():
    for g in range(LOGS):
        cluster(g, 'up', '--fresh', '--no-proxy', '--mem-cap-mb', os.environ.get('PQ_MEMCAP', str(max(600, 3000 // LOGS))), '--window-ms', WINDOW)
        cluster(g, 'flusher-up', '--replicas', group_addrs(g), '--null', '1', '--age-ms', os.environ.get('Q8_FLUSH_AGE_MS', '2000'),
                '--interval-ms', os.environ.get('Q8_FLUSH_INTERVAL_MS', '500'), '--hot-db', '0')
    # wait for the preferred leaders (the others wait PQ_PREF_DELAY_MS before their first vote)
    end = time.time() + 20
    while time.time() < end:
        ls = [[s['id'] for s in status(g) if s.get('role') == 'Leader'] for g in range(LOGS)]
        if all(len(x) == 1 for x in ls):
            return [x[0] for x in ls]
        time.sleep(0.3)
    raise SystemExit('no leader in every log')


def down():
    for g in range(LOGS):
        cluster(g, 'flusher-down')
        cluster(g, 'down')


def leaders():
    out = []
    for g in range(LOGS):
        st = status(g)
        out.append({'leader': [s['id'] for s in st if s.get('role') == 'Leader'], 'st': {s['id']: s for s in st if 'id' in s}})
    return out


def run_phase(level, ph):
    kinds = noisy_for(ph, level)
    n_normal = level - len(kinds)
    secs = SECS_SMALL if n_normal <= 2 else SECS
    gens = [Gen(kd, NOISY[kd][0], NOISY[kd][1], 1000 + i) for i, kd in enumerate(kinds)]
    for g in gens:
        g.start()
    time.sleep(3)
    normal = subprocess.Popen(['chrt', '-f', '20', PQ, 'corsie', '--replicas', all_addrs(), '--db-from', str(NORMAL_FROM), '--dbs', str(n_normal), '--rate', '100', '--doc', '1900',
                               '--secs', str(secs), '--warm', str(WARM), '--label', 'normali', '--threads', '2', '--conns', '16', '--deadline-ms', '10000'],
                              stdout=subprocess.PIPE, stderr=LOGF, text=True)
    time.sleep(WARM)
    for g in range(LOGS):
        sh([PQ, 'reset', '--replicas', group_addrs(g)], timeout=20)
    t0 = time.time()
    pc = PerCore()
    pc.start()
    reps = {f'g{g}r{i}': pids(g).get(f'r{i}') for g in range(LOGS) for i in range(3)} if not REMOTE else {}
    c0 = {k: proc_cpu(v) for k, v in reps.items()}
    gc0 = proc_cpu(normal.pid)
    d0 = diskstats()
    st0 = leaders()
    time.sleep(secs)
    el = time.time() - t0
    pc.stop = True
    c1 = {k: proc_cpu(v) for k, v in reps.items()}
    gc1 = proc_cpu(normal.pid)
    d1 = diskstats()
    st1 = leaders()
    out = normal.communicate(timeout=180)[0]
    res = json.loads(out.strip().splitlines()[-1])
    for g in gens:
        g.end()
    cpu = pc.summary()
    rep_cores = {k: ((c1[k] - c0[k]) / el if None not in (c0[k], c1[k]) else None) for k in reps}
    gen_cores = (gc1 - gc0) / el if None not in (gc0, gc1) else None
    asked = n_normal * 100
    sent_ok = res.get('sent_per_s', 0) >= 0.98 * asked
    cpu_ok = (cpu.get('max_core_mean_pct') or 100) <= 90.0
    noisy_logs = sorted({(1000 + i) % LOGS for i in range(len(kinds))})
    per_log = res.get('per_log', [])
    per_log_commits = [max((s.get('commit_index', 0) for s in st1[g]['st'].values()), default=0) - max((s.get('commit_index', 0) for s in st0[g]['st'].values()), default=0)
                       for g in range(LOGS)]
    commits = sum(per_log_commits)
    diags = []
    for g in range(LOGS):
        try:
            diags.append(json.loads(st1[g]['st'][st1[g]['leader'][0]]['diag']))
        except Exception:
            diags.append({})
    r = {'t_start': t0, 't_end': t0 + el, 'secs': el, 'tag': TAG, 'logs': LOGS, 'level': level, 'phase': ph, 'noisy': kinds, 'noisy_logs': noisy_logs, 'n_normal': n_normal,
         'env': {k: v for k, v in ENV.items() if k.startswith('PQ_') or k.startswith('Q8_')}, 'normal': res, 'percore': cpu,
         'replica_cores': rep_cores, 'gen_normal_cores': gen_cores, 'asked_per_s': asked, 'sent_ok': sent_ok, 'cpu_ok': cpu_ok,
         'valid': sent_ok and cpu_ok, 'recs_per_s_total': commits / el, 'recs_per_s_per_log': [c / el for c in per_log_commits],
         'disk': {k: (d1.get(k, 0) - d0.get(k, 0)) / el for k in ('writes', 'flush', 'sectors_w')},
         'leaders_before': [x['leader'] for x in st0], 'leaders_after': [x['leader'] for x in st1],
         'leader_diag': diags, 'noisy_kills': sum(g.kills for g in gens), 'noisy_gen_cpu_s': sum(g.cpu for g in gens),
         'rss_mb': {k: rss_mb(v) for k, v in reps.items()}}
    with open(os.path.join(OUT, 'q8c.jsonl'), 'a') as f:
        f.write(json.dumps(r) + '\n')
    L = res['lat']
    same = [per_log[g] for g in noisy_logs if g < len(per_log)]
    other = [per_log[g] for g in range(len(per_log)) if g not in noisy_logs]
    fmt = lambda v: ' '.join(f"log{per_log.index(x)} p50 {x['p50_us']/1000:.2f} p99 {x['p99_us']/1000:.2f}" for x in v if x['n'])
    door = [d.get('porta') for d in diags]
    print(f"{TAG} level {level} phase {ph}: {'VALID' if r['valid'] else 'NOT VALID'} normals {n_normal} p50 {L['p50_us']/1000:.2f} p99 {L['p99_us']/1000:.2f} p99.9 {L['p999_us']/1000:.2f} "
          f"max {L['max_us']/1000:.1f} ms n {L['n']} rej {res['rejected']} unk {res['unknown']} busy {res['busy_retries']} | noisy logs {noisy_logs}: [{fmt(same)}] others: [{fmt(other)}] | "
          f"cores {', '.join(f'{k} {v:.0f}%' for k, v in sorted(cpu.get('mean_pct', {}).items()))} | gen {gen_cores or 0:.2f} core asked {asked} sent {res.get('sent_per_s', 0):.0f}/s | "
          f"total {r['recs_per_s_total']:.0f} rec/s | door {door}", flush=True)
    time.sleep(5)


def run_level(level):
    if not REMOTE:
        up()
    pw = int(os.environ.get('Q8_PREWARM', '30'))
    if pw > 0:
        sh(['chrt', '-f', '20', PQ, 'corsie', '--replicas', all_addrs(), '--db-from', '0', '--dbs', str(level), '--rate', '100', '--secs', str(pw), '--warm', '0',
            '--label', 'warmup', '--threads', '2', '--conns', '16'], timeout=pw + 60)
    if level == 1:
        for _ in range(REF_REPEATS):
            run_phase(1, 'nessuno')
    else:
        for ph in PHASES:
            run_phase(level, ph)
    if not REMOTE:
        down()


for lv in LEVELS:
    run_level(lv)
print('end', TAG, flush=True)
