#!/usr/bin/env python3
"""Q8d tests for reviewer C1, C3, C4 on a real local group of 3 replicas (scripts/cluster.py, real fdatasync).

  C1: a resend of an already committed VALIDATED commit (same txn id) gets Committed(same pos) as long as the commit
      is in the log; "Conflict" only after it has left the window (pq retryprobe, window 2000 ms, null flusher).
  C3: with database A over its door cap, a CommitBatch [A, B] gives B "Committed" (pq doormix, PQ_DOOR=1).
  C4: a log of a cell with 2 logs (log 1 of 2) commits only its own databases and refuses the others (pq foreigndb).
Exit 0 = all PASS. Run from anywhere; needs target/release/pq.
"""
import os, re, subprocess, sys, time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.join(HERE, '..')
PQ = os.path.join(ROOT, 'target', 'release', 'pq')
CL = os.path.join(ROOT, 'scripts', 'cluster.py')
WORK = os.environ.get('PQ_WORK_BASE', '/tmp/q8d-probes')
REPL = '127.0.0.1:7200,127.0.0.1:7201,127.0.0.1:7202'
fails = []


def cluster(env, *a):
    return subprocess.run(['python3', CL] + list(a), env=env, capture_output=True, text=True, timeout=120).stdout


def pq(*a):
    return subprocess.run([PQ] + list(a), capture_output=True, text=True, timeout=120).stdout


def up(name, extra_env, window_ms=60000):
    env = dict(os.environ, PQ_WORK=os.path.join(WORK, name), PQ_CORSIE='1', **extra_env)
    cluster(env, 'up', '--fresh', '--no-proxy', '--window-ms', str(window_ms), '--mem-cap-mb', '512')
    return env


def check(label, ok, detail):
    print(('PASS ' if ok else 'FAIL ') + label + ('' if ok else ' :: ' + detail.replace('\n', ' | ')))
    if not ok:
        fails.append(label)


# ---- C1
for door in ('0', '1'):
    env = up(f'c1-door{door}', {'PQ_DOOR': door}, window_ms=2000)
    cluster(env, 'flusher-up', '--replicas', REPL, '--null', '1', '--age-ms', '100', '--interval-ms', '100')
    time.sleep(1)
    out = pq('retryprobe', '--replicas', REPL, '--window-ms', '2000')
    cluster(env, 'down')
    first_conflict = None
    for m in re.finditer(r't\+\s*(\d+) ms resend of T -> (\S+)', out):
        if m.group(2).startswith('Conflict') and first_conflict is None:
            first_conflict = int(m.group(1))
    bad = re.search(r'OTHER pos', out)
    # T leaves the window about 2000 ms after its commit; before that every resend must be Committed(same pos)
    check(f'C1 retry of a committed validated commit (door {door}): no Conflict while it is in the log',
          'Committed(same pos)' in out and not bad and (first_conflict is None or first_conflict >= 1800), out)
    print(out.strip())

# ---- C3
env = up('c3', {'PQ_DOOR': '1', 'PQ_DOOR_RECS': '1000'})
out = pq('doormix', '--replicas', REPL, '--flood', '1000')
cluster(env, 'down')
ab = re.search(r'batch \[A, B\]: .* -> B gets (\S+)', out)
check('C3 batch [A over cap, B quiet]: B gets Committed', bool(ab) and ab.group(1) == 'Committed', out)
a_only = re.search(r'batch \[A\]: .* -> B gets (\S+)', out)
check('C3 control: A over its cap is still refused', bool(a_only) and a_only.group(1).startswith('Busy'), out)
print(out.strip())

# ---- C4
env = up('c4', {'PQ_LOG_ID': '1', 'PQ_LOGS': '2'})
out = pq('foreigndb', '--replicas', REPL, '--n', '8')
cluster(env, 'down')
rows = dict(re.findall(r'db (\d+): (\S+)', out))
ok = len(rows) == 8 and all((rows[str(d)] == 'Committed') == (d % 2 == 1) for d in range(8)) and all(rows[str(d)].startswith('Refused') for d in range(0, 8, 2))
check('C4 log 1 of 2 commits only odd databases and refuses the others', ok, out)
print(out.strip())
env = up('c4-single', {})
out = pq('foreigndb', '--replicas', REPL, '--n', '4')
cluster(env, 'down')
check('C4 control: a single log owns every database', out.count('Committed') == 4, out)

print('RESULT', 'PASS' if not fails else 'FAIL ' + ', '.join(fails))
sys.exit(1 if fails else 0)
