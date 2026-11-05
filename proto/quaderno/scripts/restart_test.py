#!/usr/bin/env python3
"""Q8c · Restart test with recycled segments (the recycling bug found by the Q8b reviewer).

A local group (3 replicas, fsync for real) with PQ_RICICLA=1 and small segments (PQ_SEG_MB, default 1 MB), the fake
flusher (no S3) so old segments leave the log and are recycled as spares. A writer fills many segments, then:

  case A "quiet": writer stopped, every replica caught up (last == commit == C); SIGKILL two replicas (the leader and
          one follower), restart them. PASS if each restarted replica recovers exactly C entries (its log line
          "pronta: indice a..b" has b == C) and the group commits again with commit >= C.
  case B "under load": SIGKILL two replicas while the writer runs, restart them after 1 s. PASS if no replica panics,
          the commit index after the restart is >= the commit seen before the kill, and all three converge.
  case C "all three": writer stopped, SIGKILL all three replicas, restart. PASS as in A.

Usage: restart_test.py OUT_DIR [--rounds 3] [--rate 3000] [--fill-s 15]
Exit code 0 only if every case of every round passes.
"""
import json, os, re, signal, subprocess, sys, time

HERE = os.path.dirname(os.path.abspath(__file__))
PQ = os.environ.get('PQ_BIN') or os.path.join(HERE, '..', 'target', 'release', 'pq')
WORK = os.environ.get('PQ_WORK', '/root/pq-work')


def opt(a, k, d):
    return a[a.index(k) + 1] if k in a else d


args = sys.argv[1:]
OUT = args[0]
ROUNDS = int(opt(args, '--rounds', '3'))
RATE = opt(args, '--rate', '3000')
FILL = int(opt(args, '--fill-s', '15'))
os.makedirs(OUT, exist_ok=True)
ENV = dict(os.environ, PQ_RICICLA='1', PQ_SEG_MB=os.environ.get('PQ_SEG_MB', '1'), PQ_CORSIE=os.environ.get('PQ_CORSIE', '1'), PQ_BIN=PQ)
LOG = open(os.path.join(OUT, 'restart_test.log'), 'a')


def say(*x):
    s = ' '.join(str(v) for v in x)
    print(s, flush=True)
    LOG.write(s + '\n')
    LOG.flush()


def cl(*a):
    return subprocess.run(['python3', os.path.join(HERE, 'cluster.py')] + list(a), env=ENV, capture_output=True, text=True, timeout=120)


def status():
    o = subprocess.run([PQ, 'status'], capture_output=True, text=True, timeout=20).stdout
    return {s['id']: s for s in (json.loads(l) for l in o.splitlines() if l.strip()) if 'id' in s}


def leader():
    for _ in range(200):
        for i, s in status().items():
            if s.get('role') == 'Leader':
                return i
        time.sleep(0.1)
    return -1


def writer(secs):
    return subprocess.Popen([PQ, 'corsie', '--db-from', '0', '--dbs', '20', '--rate', str(int(RATE) // 20), '--doc', '1900', '--secs', str(secs), '--warm', '0',
                             '--label', 'restart', '--threads', '2', '--conns', '8', '--deadline-ms', '5000'], stdout=subprocess.PIPE, stderr=LOG, text=True, env=ENV)


def recovered(i, since):
    """Last index recovered by replica i at its latest start (log line 'replica i pronta: indice a..b')."""
    txt = open(os.path.join(WORK, f'r{i}.log'), errors='replace').read()[since.get(i, 0):]
    m = re.findall(rf'replica {i} pronta: indice (\d+)\.\.(\d+)', txt)
    return int(m[-1][1]) if m else None


def panicked(since):
    bad = []
    for i in range(3):
        txt = open(os.path.join(WORK, f'r{i}.log'), errors='replace').read()[since.get(i, 0):]
        if 'panicked' in txt or 'troncamento di una voce confermata' in txt:
            bad.append(i)
    return bad


def offsets():
    return {i: os.path.getsize(os.path.join(WORK, f'r{i}.log')) for i in range(3)}


def wait_caught_up(t=20):
    end = time.time() + t
    while time.time() < end:
        st = status()
        if len(st) == 3:
            c = max(s['commit_index'] for s in st.values())
            if all(s['last_index'] == c and s['durable'] >= c for s in st.values()):
                return c, st
        time.sleep(0.2)
    return None, status()


def segs_info():
    out = {}
    for i in range(3):
        d = os.path.join(WORK, f'r{i}')
        out[i] = sorted(f for f in os.listdir(d) if f.startswith('seg-') or f == 'spare.log')
    return out


def fill():
    w = writer(FILL)
    w.communicate(timeout=FILL + 60)


def case_quiet(name, kill):
    fill()
    c, st = wait_caught_up()
    if c is None:
        say(name, 'NOT CAUGHT UP', json.dumps({i: (s['last_index'], s['commit_index'], s['durable']) for i, s in st.items()}))
        return False
    segs = segs_info()
    recycled = os.environ.get('PQ_RICICLA') != '0'
    off = offsets()
    for i in kill:
        cl('kill', str(i))
    time.sleep(0.5)
    for i in kill:
        cl('start', str(i))
    lid = leader()
    time.sleep(1.5)
    got = {i: recovered(i, off) for i in kill}
    st2 = status()
    commit2 = max((s['commit_index'] for s in st2.values()), default=0)
    bad = panicked(off)
    ok = all(got[i] == c for i in kill) and commit2 >= c and not bad and lid >= 0
    say(name, 'PASS' if ok else 'FAIL', f'C={c} recovered={got} commit_after={commit2} leader={lid} panics={bad} segments_before={ {i: len(v) for i, v in segs.items()} }')
    return ok


def case_load(name, kill):
    w = writer(FILL + 6)
    time.sleep(FILL / 2)
    st = status()
    c0 = max(s['commit_index'] for s in st.values())
    off = offsets()
    for i in kill:
        cl('kill', str(i))
    time.sleep(1)
    for i in kill:
        cl('start', str(i))
    w.communicate(timeout=FILL + 90)
    c, st2 = wait_caught_up(30)
    bad = panicked(off)
    ok = c is not None and c >= c0 and not bad
    say(name, 'PASS' if ok else 'FAIL', f'commit_before_kill={c0} converged_commit={c} panics={bad} last={ {i: s["last_index"] for i, s in st2.items()} }')
    return ok


def main():
    allok = True
    cl('up', '--fresh', '--no-proxy', '--mem-cap-mb', '1500', '--window-ms', '5000')
    cl('flusher-up', '--null', '1', '--age-ms', '500', '--interval-ms', '200', '--hot-db', '0')
    time.sleep(2)
    for r in range(ROUNDS):
        lid = leader()
        fol = [i for i in range(3) if i != lid]
        allok &= case_quiet(f'round {r} A quiet: kill leader {lid} + follower {fol[0]}', [lid, fol[0]])
        lid = leader()
        fol = [i for i in range(3) if i != lid]
        allok &= case_quiet(f'round {r} A2 quiet: kill both followers {fol}', fol)
        lid = leader()
        fol = [i for i in range(3) if i != lid]
        allok &= case_load(f'round {r} B under load: kill leader {lid} + follower {fol[1]}', [lid, fol[1]])
        allok &= case_quiet(f'round {r} C quiet: kill all three', [0, 1, 2])
    cl('flusher-down')
    cl('down')
    say('RESULT', 'ALL PASS' if allok else 'SOME FAIL')
    sys.exit(0 if allok else 1)


main()
