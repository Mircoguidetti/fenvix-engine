#!/usr/bin/env python3
"""Q8 · Prova delle corsie sul quaderno condiviso (Fable 2.5, prova 8, allargata a molti clienti).

Per ogni livello (numero di database sullo stesso quaderno) si accende un gruppo nuovo (3 repliche, cartelle
separate, fdatasync vero), lo scaricatore finto (senza S3, pota la finestra) e, fase dopo fase, i vicini rumorosi.
I database "normali" fanno 100 scritture al secondo ciascuno (documenti da 1,9 KB, ciclo aperto); la latenza di
conferma si misura dall'istante pianificato. Il riferimento è un database da solo sullo stesso quaderno (livello 1).

Disposizione sui 4 core: capo (leader) sui core 0-1, le due copie di riserva sul core 2, generatori, scaricatore e
proxy sul core 3; il generatore dei normali con priorità in tempo reale (chrt -f), così i generatori rumorosi sullo
stesso core non sporcano la misura dei normali.

Uso: corsie.py OUT [--lanes 1] [--levels 1,2,20,50,100] [--phases nessuno,mille,...] [--secs 60] [--secs-small 120]
     [--warm 10] [--window-ms 10000] [--tag nome]
Fasi: nessuno, mille (1.000/s), ventimila (20.000/s, sopra la quota), raffiche (50.000 record in lotti da 1.000 ogni
10 s), mega (record da 1 MB, 20 al secondo), muore (5.000/s, ucciso ogni 8 s e riavviato dopo 2 s), dieci (il 10% dei
database rumorosi tutti insieme, di tipi diversi).
"""
import json, os, signal, subprocess, sys, threading, time

HERE = os.path.dirname(os.path.abspath(__file__))
PQ = os.environ.get('PQ_BIN') or os.path.join(HERE, '..', 'target', 'release', 'pq')
WORK = os.environ.get('PQ_WORK', '/root/pq-work')
HZ = os.sysconf('SC_CLK_TCK')


def opt(a, k, d):
    return a[a.index(k) + 1] if k in a else d


args = sys.argv[1:]
OUT = args[0]
LANES = opt(args, '--lanes', '1')
LEVELS = [int(x) for x in opt(args, '--levels', '1,2,20,50,100').split(',')]
PHASES = opt(args, '--phases', 'nessuno,mille,ventimila,raffiche,mega,muore,dieci').split(',')
SECS = int(opt(args, '--secs', '60'))
SECS_SMALL = int(opt(args, '--secs-small', '120'))
WARM = int(opt(args, '--warm', '10'))
WINDOW = opt(args, '--window-ms', '10000')
TAG = opt(args, '--tag', f'corsie{LANES}')
os.makedirs(OUT, exist_ok=True)
ENV = dict(os.environ, PQ_CORSIE=LANES)
ENV.setdefault('PQ_PREALLOC_MBS', '64')

# i tipi di vicino rumoroso: (nome, argomenti del generatore, uccisioni periodiche)
NOISY = {
    'mille': (['--mode', 'steady', '--rate', '1000', '--deadline-ms', '5000'], False),
    'ventimila': (['--mode', 'steady', '--rate', '20000', '--deadline-ms', '2000', '--inflight', '20000'], False),
    'raffiche': (['--mode', 'burst', '--burst', '50000', '--batch', '1000', '--burst-every-ms', '10000', '--deadline-ms', '10000', '--inflight', '64'], False),
    'mega': (['--mode', 'big', '--rate', '20', '--big-kb', '1024', '--deadline-ms', '5000', '--inflight', '256'], False),
    'muore': (['--mode', 'steady', '--rate', '5000', '--deadline-ms', '3000', '--inflight', '20000'], True),
}
# nella fase "dieci" i tipi si alternano partendo dai più pesanti
DIECI_ORDER = ['ventimila', 'raffiche', 'mega', 'muore', 'mille']


def sh(cmd, **kw):
    return subprocess.run(cmd, capture_output=True, text=True, **kw)


def cluster(*a):
    return sh(['python3', os.path.join(HERE, 'cluster.py')] + list(a), env=ENV, timeout=120)


def status():
    o = sh([PQ, 'status'], timeout=20).stdout
    return [json.loads(l) for l in o.splitlines() if l.strip()]


def pids():
    return json.load(open(os.path.join(WORK, 'pids.json')))


def pin(pid, cores):
    sh(['taskset', '-a', '-p', '-c', cores, str(pid)])


def proc_cpu(pid, tid=None):
    p = f'/proc/{pid}/task/{tid}/stat' if tid else f'/proc/{pid}/stat'
    try:
        f = open(p).read().rsplit(')', 1)[1].split()
        return (int(f[11]) + int(f[12])) / HZ
    except Exception:
        return None


def proc_kv(pid, file, key):
    try:
        for l in open(f'/proc/{pid}/{file}'):
            if l.startswith(key):
                return int(l.split()[1])
    except Exception:
        return None


def diskstats():
    for l in open('/proc/diskstats'):
        f = l.split()
        if f[2] == 'vda':
            return {'writes': int(f[7]), 'sectors_w': int(f[9]), 'ms_w': int(f[10]), 'io_ms': int(f[12]), 'flush': int(f[18]) if len(f) > 18 else 0}
    return {}


def snap(reps):
    s = {'t': time.time(), 'disk': diskstats()}
    for k, pid in reps.items():
        s[k] = {'cpu': proc_cpu(pid), 'main': proc_cpu(pid, pid), 'rss_kb': proc_kv(pid, 'status', 'VmRSS'), 'wbytes': proc_kv(pid, 'io', 'write_bytes'), 'wchar': proc_kv(pid, 'io', 'wchar')}
    return s


def delta(a, b, reps):
    el = b['t'] - a['t']
    r = {'secs': el, 'disk': {k: b['disk'][k] - a['disk'][k] for k in b['disk']}}
    r['disk']['MB_s'] = r['disk']['sectors_w'] * 512 / el / 1e6
    r['disk']['util_pct'] = r['disk']['io_ms'] / el / 10
    for k in reps:
        x, y = a[k], b[k]
        r[k] = {'cores': (y['cpu'] - x['cpu']) / el if None not in (x['cpu'], y['cpu']) else None,
                'main_cores': (y['main'] - x['main']) / el if None not in (x['main'], y['main']) else None,
                'rss_mb': (y['rss_kb'] or 0) / 1024, 'write_bytes_MB_s': ((y['wbytes'] or 0) - (x['wbytes'] or 0)) / el / 1e6,
                'wchar_MB_s': ((y['wchar'] or 0) - (x['wchar'] or 0)) / el / 1e6}
    return r


class Gen:
    """Un generatore rumoroso (processo a parte), con morte e riavvio periodici se richiesto."""

    def __init__(self, name, gargs, kill, db_from, log):
        self.name, self.gargs, self.kill, self.db_from, self.log = name, gargs, kill, db_from, log
        self.p = None
        self.stop = False
        self.kills = 0
        self.cpu = 0.0

    def cmd(self):
        return ['nice', '-n', '5', 'taskset', '-c', '3', PQ, 'corsie', '--db-from', str(self.db_from), '--dbs', '1', '--secs', '100000', '--warm', '0',
                '--label', f'{self.name}-{self.db_from}', '--threads', '2', '--conns', '16'] + self.gargs

    def start(self):
        self.p = subprocess.Popen(self.cmd(), stdout=subprocess.DEVNULL, stderr=self.log)
        if self.kill:
            threading.Thread(target=self.killer, daemon=True).start()

    def killer(self):
        while not self.stop:
            time.sleep(8)
            if self.stop:
                break
            c = proc_cpu(self.p.pid)
            self.cpu += c or 0
            self.p.send_signal(signal.SIGKILL)
            self.p.wait()
            self.kills += 1
            time.sleep(2)
            if self.stop:
                break
            self.p = subprocess.Popen(self.cmd(), stdout=subprocess.DEVNULL, stderr=self.log)

    def end(self):
        self.stop = True
        c = proc_cpu(self.p.pid)
        self.cpu += c or 0
        self.p.send_signal(signal.SIGKILL)
        self.p.wait()


def noisy_for(phase, level):
    if phase == 'nessuno':
        return []
    if phase == 'dieci':
        k = max(1, level // 10)
        return [DIECI_ORDER[i % len(DIECI_ORDER)] for i in range(k)]
    return [phase]


def run_level(level):
    cluster('up', '--fresh', '--no-proxy', '--mem-cap-mb', os.environ.get('PQ_MEMCAP', '3000'), '--window-ms', WINDOW)
    cluster('flusher-up', '--null', '1', '--age-ms', '2000', '--interval-ms', '500', '--hot-db', '0')
    time.sleep(2)
    p = pids()
    st = status()
    lid = [s['id'] for s in st if s.get('role') == 'Leader'][0]
    reps = {f'r{i}': p[f'r{i}'] for i in range(3)}
    for k, v in p.items():
        if k == 'cfg':
            continue
        if k == f'r{lid}':
            pin(v, '0,1')
        elif k.startswith('r'):
            pin(v, '2')
        else:
            pin(v, '3')
    phases = ['nessuno'] if level == 1 else PHASES
    for ph in phases:
        kinds = noisy_for(ph, level)
        n_normal = level - len(kinds)
        secs = SECS_SMALL if n_normal <= 2 else SECS
        log = open(os.path.join(OUT, f'gen-{TAG}-{level}-{ph}.log'), 'ab')
        gens = [Gen(kd, NOISY[kd][0], NOISY[kd][1], 1000 + i, log) for i, kd in enumerate(kinds)]
        for g in gens:
            g.start()
        time.sleep(3)
        normal = subprocess.Popen(['chrt', '-f', '20', 'taskset', '-c', '3', PQ, 'corsie', '--db-from', '0', '--dbs', str(n_normal), '--rate', '100', '--doc', '1900',
                                   '--secs', str(secs), '--warm', str(WARM), '--label', 'normali', '--threads', '2', '--conns', '16', '--deadline-ms', '10000'],
                                  stdout=subprocess.PIPE, stderr=log, text=True)
        time.sleep(WARM)
        s0 = snap(reps)
        st0 = {s['id']: s for s in status() if 'id' in s}
        time.sleep(secs)
        s1 = snap(reps)
        st1 = {s['id']: s for s in status() if 'id' in s}
        out = normal.communicate(timeout=120)[0]
        res = json.loads(out.strip().splitlines()[-1])
        for g in gens:
            g.end()
        lead1 = [i for i, s in st1.items() if s.get('role') == 'Leader']
        try:
            diag = json.loads(st1[lid]['diag'])
        except Exception:
            diag = {}
        r = {'tag': TAG, 'lanes': LANES, 'prealloc_mbs': ENV.get('PQ_PREALLOC_MBS'), 'env': {k: v for k, v in ENV.items() if k.startswith('PQ_')}, 'level': level, 'phase': ph, 'noisy': kinds, 'n_normal': n_normal, 'window_ms': WINDOW,
             'normal': res, 'sys': delta(s0, s1, reps), 'leader': f'r{lid}', 'leader_after': [f'r{i}' for i in lead1],
             'term_change': st1.get(lid, {}).get('term') != st0.get(lid, {}).get('term'),
             'leader_diag': diag, 'mem_used_mb': {f'r{i}': s.get('mem_used', 0) >> 20 for i, s in st1.items()},
             'commit_index_delta': st1.get(lid, {}).get('commit_index', 0) - st0.get(lid, {}).get('commit_index', 0),
             'noisy_kills': sum(g.kills for g in gens), 'noisy_gen_cpu_s': sum(g.cpu for g in gens)}
        r['recs_per_s_total'] = r['commit_index_delta'] / r['sys']['secs']
        with open(os.path.join(OUT, 'corsie.jsonl'), 'a') as f:
            f.write(json.dumps(r) + '\n')
        L = res['lat']
        print(f"{TAG} livello {level} fase {ph}: normali {n_normal} p50 {L['p50_us']/1000:.2f} p99 {L['p99_us']/1000:.2f} p99.9 {L['p999_us']/1000:.2f} max {L['max_us']/1000:.1f} ms "
              f"n {L['n']} rifiutati {res['rejected']} ignoti {res['unknown']} | totale {r['recs_per_s_total']:.0f} rec/s, capo {r['sys'][r['leader']]['cores']:.2f} core, disco {r['sys']['disk']['MB_s']:.1f} MB/s",
              flush=True)
        # quiete tra le fasi: corsie vuote
        time.sleep(6)
    cluster('down')


for lv in LEVELS:
    run_level(lv)
print('fine', TAG, flush=True)
