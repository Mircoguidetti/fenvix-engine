#!/usr/bin/env python3
"""log-on-pair · point 3 of prompts/P-log-on-pair-local.md: guarantees 1 and 8 with the log on the pair.

The pair shape of `pair_common.py` (ack = the leader's own durable copy AND the first remote copy; copy 3 is the keeper,
which leads only as a last resort and hands back), cross-zone delay on every link, real fdatasync, recycling on. A writer
(`pq failover`, ~1,000 commits/s over 1,000 databases) records every ACKNOWLEDGED commit (txn id, position) to a CSV.

The waiter is whichever data copy (r0 or r1) leads; the other data copy is the spare. Each cycle:
  kill9  (most cycles): writes for 1-4 s, kill -9 of the waiter; at once (before any new leader can exist) the status of
         the spare and of the keeper (durable index); wait for the new leader (time and who); restart the old waiter after
         0-2 s and at once send it commits directly (`pq zombieprobe`): it must not acknowledge any (it comes back with
         the old turn number, the others have a newer one)
  stop   (every 4th cycle): SIGSTOP of the waiter (frozen, still believes it leads), wait for the new leader, SIGCONT and
         at once the same zombie probe: a frozen-then-woken old waiter must not acknowledge any write either
Then wait until the three copies agree. At the end: the writer stops; the committed log of every copy is dumped and every
acknowledged commit must be in each of them at its acknowledged position (no acknowledged write lost, the spare has every
one), every zombie "Committed" must be in the log, and for each kill the number of acknowledged commits the spare did
NOT have durable at the instant of the kill is counted (they reached it later from the keeper).

The spare's and keeper's indexes are sampled 250 ms after the kill (Status is a 100 ms snapshot).

Usage: pair_kill9.py OUT [--cycles 32] [--seed 1] [--delay-us 500]
"""
import csv, json, os, random, signal, subprocess, sys, time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pair_common import *  # noqa


def opt(a, k, d):
    return a[a.index(k) + 1] if k in a else d


A = sys.argv[1:]
OUT = os.path.abspath(A[0])
CYCLES = int(opt(A, '--cycles', '32'))
random.seed(int(opt(A, '--seed', '1')))
DELAY = int(opt(A, '--delay-us', '500'))
os.makedirs(OUT, exist_ok=True)
LOG = open(os.path.join(OUT, 'kill9.log'), 'a')
EV = open(os.path.join(OUT, 'cycles.jsonl'), 'a')


def say(*x):
    s = time.strftime('%H:%M:%S ') + ' '.join(str(v) for v in x)
    print(s, flush=True)
    LOG.write(s + '\n')
    LOG.flush()


cell = Cell(work=os.path.join(OUT, 'work'), delay_us=DELAY, leader_ack=True, keeper=True, mem_cap_mb=3000, seg_mb=8)


def fast_status(i):
    """status of ONE replica, straight to its client port, as fast as possible"""
    try:
        o = subprocess.run([PQ, 'status', '--replicas', f'127.0.0.1:{7200 + i}'], capture_output=True, text=True, timeout=5).stdout
        return json.loads(o.strip().splitlines()[0])
    except Exception:
        return {}


def leader_not(old, t=30):
    t0 = time.time()
    while time.time() - t0 < t:
        for i, s in cell.status(timeout=3).items():
            if s.get('role') == 'Leader' and i != old:
                return i, time.time() - t0
        time.sleep(0.05)
    return -1, None


def converged(t=60):
    end = time.time() + t
    while time.time() < end:
        st = cell.status(timeout=3)
        if len(st) == 3 and any(s.get('role') == 'Leader' for s in st.values()):
            c = max(s['commit_index'] for s in st.values())
            if all(s['durable'] >= c - 3000 for s in st.values()):
                return st
        time.sleep(0.3)
    return None


def zombie(i, ms=3000):
    o = subprocess.run([PQ, 'zombieprobe', '--replicas', cell.direct_addrs(), '--replica', str(i), '--ms', str(ms), '--every-ms', '2'],
                       capture_output=True, text=True, timeout=60).stdout
    try:
        return json.loads(o.strip().splitlines()[-1])
    except Exception:
        return {'error': o[-300:]}


def panicked():
    bad = []
    for i in range(3):
        txt = open(os.path.join(cell.work, f'r{i}.log'), errors='replace').read()
        if 'panicked' in txt:
            bad.append(i)
    return bad


def main():
    say('START cycles', CYCLES, 'delay_us', DELAY)
    ld = cell.up(flusher=False)
    say('first leader', ld)
    acked = os.path.join(OUT, 'acked.csv')
    secs = CYCLES * 30 + 60
    w = subprocess.Popen(['taskset', '-c', CPU_TOOLS, PQ, 'failover', '--replicas', cell.client_addrs(), '--secs', str(secs), '--dbs', '1000',
                          '--period-ms', '1000', '--out', acked], stdout=open(os.path.join(OUT, 'writer.out'), 'w'),
                         stderr=open(os.path.join(OUT, 'writer.err'), 'w'), env=BASE_ENV)
    time.sleep(8)
    t_start = time.time()
    events = []
    for k in range(CYCLES):
        st = cell.status(timeout=3)
        lid = next((i for i, s in st.items() if s.get('role') == 'Leader'), -1)
        if lid == 2:
            # the keeper leads: wait for its hand-over to a data copy
            t0 = time.time()
            while lid == 2 and time.time() - t0 < 20:
                time.sleep(0.2)
                st = cell.status(timeout=3)
                lid = next((i for i, s in st.items() if s.get('role') == 'Leader'), -1)
        if lid not in (0, 1):
            say('cycle', k, 'no data-copy leader, leader =', lid)
            ev = {'cycle': k, 'error': 'no data-copy leader', 'leader': lid}
            EV.write(json.dumps(ev) + '\n')
            events.append(ev)
            time.sleep(3)
            continue
        spare = 1 - lid
        mode = 'stop' if k % 4 == 3 else 'kill9'
        time.sleep(random.uniform(1, 4))
        old_term = st[lid]['term']
        tk = time.time()
        if mode == 'kill9':
            cell.kill(f'r{lid}')
        else:
            cell.kill(f'r{lid}', signal.SIGSTOP)
        # Status is a snapshot the replica refreshes every 100 ms: sample 250 ms after the kill, when the snapshot shows
        # everything the spare and the keeper received from the waiter before it died (no new leader can exist yet: the
        # election timeouts are 1-2 s)
        time.sleep(0.25)
        sp, kp = fast_status(spare), fast_status(2)
        t_snap = time.time() - tk
        new, t_fail = leader_not(lid)
        new_term = (cell.status(timeout=3).get(new) or {}).get('term')
        if mode == 'kill9':
            time.sleep(random.uniform(0, 2))
            cell.start_replica(lid)
        else:
            cell.kill(f'r{lid}', signal.SIGCONT)
        z = zombie(lid)
        conv = converged()
        ev = {'cycle': k, 'mode': mode, 'kill_unix_ms': tk * 1000, 'waiter': lid, 'spare': spare, 'old_term': old_term, 'new_leader': new, 'new_term': new_term,
              'failover_s': round(t_fail, 3) if t_fail else None, 'snap_after_s': round(t_snap, 3),
              'spare_durable_at_kill': sp.get('durable'), 'spare_last_at_kill': sp.get('last_index'), 'keeper_durable_at_kill': kp.get('durable'),
              'keeper_last_at_kill': kp.get('last_index'), 'zombie': z, 'converged': conv is not None, 't': round(time.time() - t_start, 1)}
        EV.write(json.dumps(ev) + '\n')
        EV.flush()
        events.append(ev)
        say('cycle', k, mode, 'waiter r%d' % lid, '-> new leader r%s in %s s' % (new, ev['failover_s']), 'terms', old_term, '->', new_term,
            '| spare durable', sp.get('durable'), 'keeper durable', kp.get('durable'), '| zombie committed', len(z.get('committed', [])),
            'not_leader', z.get('not_leader'), 'ambiguous', z.get('ambiguous'), 'no_answer', z.get('no_answer'), '| converged', conv is not None)
    say('cycles done; stopping the writer')
    # the writer stops by itself at --secs; shorten: kill it and keep its CSV (flushed line by line)
    w.kill()
    time.sleep(2)
    converged()
    time.sleep(3)
    # dump every replica
    dumps = {}
    for i in range(3):
        p = os.path.join(OUT, f'dump-r{i}.csv')
        with open(p, 'w') as f:
            rc = subprocess.run([PQ, 'dump', '--replicas', cell.direct_addrs(), '--replica', str(i)], stdout=f, stderr=subprocess.PIPE, timeout=300).returncode
        m = {}
        for row in csv.reader(open(p)):
            if len(row) >= 3:
                m[row[1]] = (int(row[0]), int(row[2]))
        dumps[i] = m
        say('dump r%d rc %d entries %d' % (i, rc, len(m)))
    ack = []
    for row in csv.reader(open(acked)):
        if len(row) >= 7 and row[5] in ('0', '1'):
            ack.append((row[6], int(row[3]), float(row[2])))
    res = {'acked': len(ack), 'cycles': len(events), 'kill9': sum(1 for e in events if e.get('mode') == 'kill9'),
           'stop': sum(1 for e in events if e.get('mode') == 'stop'), 'panics': panicked()}
    for i in range(3):
        lost = [t for t, p, _ in ack if t not in dumps[i]]
        wrong = [t for t, p, _ in ack if t in dumps[i] and dumps[i][t][1] != p]
        res[f'r{i}_missing'] = len(lost)
        res[f'r{i}_wrong_pos'] = len(wrong)
    # zombie acks must be in the log
    zc = [c.split(':')[0] for e in events for c in (e.get('zombie') or {}).get('committed', [])]
    res['zombie_committed'] = len(zc)
    res['zombie_committed_missing_from_log'] = sum(1 for t in zc if t not in dumps[0])
    # per kill: acknowledged commits the spare did not have durable at the instant of the kill
    idx = {t: dumps[0][t][0] for t, _, _ in ack if t in dumps[0]}
    gaps = []
    for e in events:
        if e.get('spare_durable_at_kill') is None:
            continue
        # acknowledged BEFORE the kill instant with a log index above the spare's durable index at that instant: those
        # were on the waiter and the keeper only (ack = own copy + first remote)
        n = sum(1 for t, _, te in ack if te < e['kill_unix_ms'] and idx.get(t, 0) > e['spare_durable_at_kill'])
        gaps.append(n)
        e['acked_not_on_spare_at_kill'] = n
    res['acked_not_on_spare_at_kill_per_cycle'] = gaps
    res['failover_s'] = [e.get('failover_s') for e in events]
    res['new_leader'] = [e.get('new_leader') for e in events]
    res['zombie_outcomes'] = [{k: (len(v) if k == 'committed' else v) for k, v in (e.get('zombie') or {}).items()} for e in events]
    json.dump(res, open(os.path.join(OUT, 'result.json'), 'w'), indent=1)
    say('RESULT', json.dumps({k: v for k, v in res.items() if not isinstance(v, list)}))
    cell.down()


if __name__ == '__main__':
    main()
