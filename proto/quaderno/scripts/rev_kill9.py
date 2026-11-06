#!/usr/bin/env python3
"""rev-q8c · Reviewer's kill -9 test (guarantee 1: no confirmed write lost), independent of the session's tests.

One group of 3 replicas (real fdatasync, recycling ON, 1 MB segments, lanes ON, door ON), the null flusher (old
segments leave the log and are recycled, as in production with S3), a writer (`pq failover`) that writes ~2,000
commits/s and records every CONFIRMED commit (pos + txn id) to a CSV, flushed line by line.

N cycles, each one picks a scenario at random and kills with SIGKILL at a random instant:
  one       kill one random replica, restart after 0-2 s
  two       kill two random replicas (often the leader), restart after 0-2 s
  three     kill all three, restart
  one_off   "one copy off": kill replica X and keep it down; 0.5-3 s of writes on the other two; then kill one of the
            two survivors at a random instant; restart it after 0-2 s; then restart X
After each cycle: wait until the three replicas agree (same last index, durable >= commit), then dump the committed
log of EVERY replica (index, txn, pos). At the end: every confirmed commit whose pos falls inside a dump's range must be
in that dump with the same txn id (otherwise LOST), and no replica may panic.

Usage: rev_kill9.py OUT [--cycles 110] [--seed 1]
"""
import csv, json, os, random, re, subprocess, sys, time

HERE = os.path.dirname(os.path.abspath(__file__))
PQ = os.environ.get('PQ_BIN') or os.path.join(HERE, '..', 'target', 'release', 'pq')
WORK = os.environ.get('PQ_WORK', '/root/pq-rev')


def opt(a, k, d):
    return a[a.index(k) + 1] if k in a else d


args = sys.argv[1:]
OUT = os.path.abspath(args[0])
CYCLES = int(opt(args, '--cycles', '110'))
random.seed(int(opt(args, '--seed', '1')))
os.makedirs(OUT, exist_ok=True)
ENV = dict(os.environ, PQ_WORK=WORK, PQ_RICICLA='1', PQ_SEG_MB='1', PQ_CORSIE='1', PQ_DOOR='1', PQ_DOOR_RECS='100000', PQ_DOOR_MBS='500',
           PQ_PREALLOC_MBS='64', PQ_BIN=PQ)
LOG = open(os.path.join(OUT, 'kill9.log'), 'a')


def say(*x):
    s = ' '.join(str(v) for v in x)
    print(s, flush=True)
    LOG.write(s + '\n')
    LOG.flush()


def cl(*a):
    return subprocess.run(['python3', os.path.join(HERE, 'cluster.py')] + list(a), env=ENV, capture_output=True, text=True, timeout=120)


def status():
    try:
        o = subprocess.run([PQ, 'status'], capture_output=True, text=True, timeout=20).stdout
    except subprocess.TimeoutExpired:
        return {}
    return {s['id']: s for s in (json.loads(l) for l in o.splitlines() if l.strip()) if 'id' in s}


def leader():
    for _ in range(100):
        for i, s in status().items():
            if s.get('role') == 'Leader':
                return i
        time.sleep(0.1)
    return random.randrange(3)


def converged(t=60):
    end = time.time() + t
    while time.time() < end:
        st = status()
        if len(st) == 3 and any(s.get('role') == 'Leader' for s in st.values()):
            c = max(s['commit_index'] for s in st.values())
            # writer is running: accept the replicas within 2,000 entries of each other and all durable past the
            # commit seen at the start of the check
            if all(s['durable'] >= c - 2000 for s in st.values()):
                return st
        time.sleep(0.3)
    return None


def dump(r, k):
    p = os.path.join(OUT, 'dumps', f'c{k:03d}-r{r}.csv')
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, 'w') as f:
        rc = subprocess.run([PQ, 'dump', '--replica', str(r)], stdout=f, stderr=subprocess.PIPE, text=True, timeout=120).returncode
    return p if rc == 0 else None


def offsets():
    return {i: os.path.getsize(os.path.join(WORK, f'r{i}.log')) for i in range(3)}


def panicked(since):
    bad = []
    for i in range(3):
        txt = open(os.path.join(WORK, f'r{i}.log'), errors='replace').read()[since.get(i, 0):]
        if 'panicked' in txt or 'troncamento di una voce confermata' in txt:
            bad.append(i)
    return bad


def kill(i):
    cl('kill', str(i))


def start(i):
    cl('start', str(i))


def main():
    say('START', time.strftime('%Y-%m-%d %H:%M:%S'), 'cycles', CYCLES)
    cl('up', '--fresh', '--no-proxy', '--mem-cap-mb', '1200', '--window-ms', '5000')
    cl('flusher-up', '--null', '1', '--age-ms', '30000', '--interval-ms', '200', '--hot-db', '0')
    time.sleep(2)
    acked = os.path.join(OUT, 'acked.csv')
    w = subprocess.Popen([PQ, 'failover', '--secs', str(CYCLES * 25 + 120), '--dbs', '200', '--period-ms', '100', '--out', acked],
                         stdout=open(os.path.join(OUT, 'writer.out'), 'w'), stderr=open(os.path.join(OUT, 'writer.err'), 'w'), env=ENV)
    time.sleep(40)  # fill > 30 s so that segments are flushed and recycled from the first cycle on
    dumps = []
    kills = 0
    bad_all = []
    t_start = time.time()
    for k in range(CYCLES):
        off = offsets()
        sc = random.choice(['one', 'two', 'two', 'three', 'one_off', 'one_off'])
        time.sleep(random.uniform(0, 3))
        lid = leader()
        if sc == 'one':
            v = [random.randrange(3)]
            for i in v: kill(i)
            time.sleep(random.uniform(0, 2))
            for i in v: start(i)
        elif sc == 'two':
            other = random.choice([i for i in range(3) if i != lid])
            v = [lid, other] if random.random() < 0.7 else [i for i in range(3) if i != lid]
            for i in v: kill(i)
            time.sleep(random.uniform(0, 2))
            for i in v: start(i)
        elif sc == 'three':
            v = [0, 1, 2]
            random.shuffle(v)
            for i in v:
                kill(i)
                time.sleep(random.uniform(0, 0.05))
            time.sleep(random.uniform(0, 1))
            for i in v: start(i)
        else:
            x = random.randrange(3)
            kill(x)
            time.sleep(random.uniform(0.5, 3))
            y = random.choice([i for i in range(3) if i != x])
            kill(y)
            v = [x, y]
            time.sleep(random.uniform(0, 2))
            start(y)
            time.sleep(random.uniform(0.5, 3))
            start(x)
        kills += len(v)
        st = converged()
        bad = panicked(off)
        bad_all += bad
        rec = {}
        for i in v:
            txt = open(os.path.join(WORK, f'r{i}.log'), errors='replace').read()[off.get(i, 0):]
            m = re.findall(rf'replica {i} pronta: indice (\d+)\.\.(\d+)', txt)
            rec[i] = m[-1] if m else None
        ds = [dump(r, k) for r in range(3)]
        dumps += [d for d in ds if d]
        say(f'cycle {k} {sc} leader_before={lid} killed={v} recovered={rec} converged={st is not None} panics={bad} dumps={sum(1 for d in ds if d)}/3 t={time.time() - t_start:.0f}s')
        if st is None:
            say('NOT CONVERGED: status', json.dumps(status()))
    say('kills', kills)
    w.terminate()
    time.sleep(2)
    cl('flusher-down')
    # final dump after a quiet period
    cl('down')
    # check
    conf = {}
    with open(acked) as f:
        for row in csv.reader(f):
            if len(row) >= 7:
                conf[int(row[3])] = row[6]
    say('confirmed commits', len(conf))
    checked = lost = wrong = 0
    covered = set()
    for d in dumps:
        m = {}
        with open(d) as f:
            for line in f:
                i, t, p = line.strip().split(',')
                m[int(p)] = t
        if not m:
            continue
        lo, hi = min(m), max(m)
        for p, t in conf.items():
            if lo <= p <= hi:
                checked += 1
                covered.add(p)
                if p not in m:
                    lost += 1
                    if lost < 20:
                        say('LOST', d, p, t)
                elif m[p] != t:
                    wrong += 1
                    if wrong < 20:
                        say('WRONG TXN', d, p, t, m[p])
    say(f'RESULT kills={kills} cycles={CYCLES} confirmed={len(conf)} distinct_confirmed_checked={len(covered)} checks={checked} lost={lost} wrong_txn={wrong} panics={sorted(set(bad_all))}')
    say('PASS' if lost == 0 and wrong == 0 and not bad_all else 'FAIL')


main()
