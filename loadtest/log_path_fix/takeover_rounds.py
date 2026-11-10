#!/usr/bin/env python3
"""Log-path fix (P-log-path-fix, 2026-11-08): the warm spare after a live takeover serves every acknowledged write.

Two engines A and B on the same object storage and the same replicated log (3 copies, `common.Log`), lease 4 s, upload
every 30 s (so nearly every acknowledged default-path write is only in the log when the primary dies), agent lane on
`agent*`, both following the database's WAL (`--agent-lane-follow`). No sleep before the kill: writer threads are
running when the primary gets kill -9 at a random moment.

Each round: the primary writes with w:majority from 6 threads (2 default-path inserts into `fast`, 2 agent-lane inserts
into `agent`, 1 mixed transaction over `fast` and `agent`, 1 `$inc` on a hot document on the default path) for a random
0.3-2.5 s, then is killed while they run. The spare takes the database over (first read after the lease lapses) and
must serve every write acknowledged in this round and in all earlier rounds, and the hot counter at least at the number
of acknowledged increments. Then the killed engine is restarted as the new spare (a fresh local directory every second
time) and the roles swap.
  takeover_rounds.py OUT_DIR [--rounds 50] [--upload-secs 30]
"""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'agent_lane'))
from pymongo import WriteConcern
from pymongo.errors import PyMongoError
from common import *

args = sys.argv[1:]
OUT = Out(args[0], 'takeover_rounds')
ROUNDS = int(opt(args, '--rounds', '50'))
UPLOAD = int(opt(args, '--upload-secs', '30'))
SEED = int(opt(args, '--seed', str(int(time.time()))))
rng = random.Random(SEED)
WORK = os.path.join(OUT.dir, 'work')
DB = 'tk'
kill_leftovers((28121, 28122), 8100)
shutil.rmtree(WORK, ignore_errors=True)
os.makedirs(WORK)
log = Log(os.path.join(WORK, 'log'), 8100)
log.up()
st = ['--store', 'local', '--local-dir', os.path.join(WORK, 'store')]
follow = ['--agent-lane-follow', DB, '--agent-lane-window-ms', '50']
E = [Engine(OUT.dir, 'A', 28121, st, log=log, upload_secs=UPLOAD, extra=follow), Engine(OUT.dir, 'B', 28122, st, log=log, upload_secs=UPLOAD, extra=follow)]
assert E[0].start(True) and E[1].start(True), 'engines did not start'
OUT.say(f'seed {SEED}, {ROUNDS} rounds, upload every {UPLOAD} s, lease 4 s')

acked = {'fast': set(), 'agent': set()}
hot_acked = 0
total_missing = 0
first_read = []


def writers(P, rnd, stop):
    lock = threading.Lock()
    got = {'fast': [], 'agent': [], 'hot': 0}

    def ins(coll, t):
        c = P.client(socketTimeoutMS=20000)
        col = c[DB].get_collection(coll, write_concern=WriteConcern(w='majority'))
        i = 0
        while not stop.is_set():
            _id = f'{coll[0]}{rnd}-{t}-{i}'
            i += 1
            try:
                col.insert_one({'_id': _id, 'r': rnd, 'pad': 'x' * 100})
            except PyMongoError:
                return
            with lock:
                got[coll].append(_id)

    def txn():
        c = P.client(socketTimeoutMS=20000)
        i = 0
        while not stop.is_set():
            a, b = f'tf{rnd}-{i}', f'ta{rnd}-{i}'
            i += 1
            try:
                with c.start_session() as s:
                    with s.start_transaction(write_concern=WriteConcern(w='majority')):
                        c[DB]['fast'].insert_one({'_id': a, 'r': rnd}, session=s)
                        c[DB]['agent'].insert_one({'_id': b, 'r': rnd}, session=s)
            except PyMongoError:
                return
            with lock:
                got['fast'].append(a)
                got['agent'].append(b)

    def hot():
        c = P.client(socketTimeoutMS=20000)
        col = c[DB].get_collection('fast', write_concern=WriteConcern(w='majority'))
        while not stop.is_set():
            try:
                col.update_one({'_id': 'hot'}, {'$inc': {'n': 1}}, upsert=True)
            except PyMongoError:
                return
            with lock:
                got['hot'] += 1

    th = [threading.Thread(target=ins, args=('fast', 0)), threading.Thread(target=ins, args=('fast', 1)),
          threading.Thread(target=ins, args=('agent', 0)), threading.Thread(target=ins, args=('agent', 1)),
          threading.Thread(target=txn), threading.Thread(target=hot)]
    for t in th:
        t.daemon = True
        t.start()
    return th, got, lock


p = 0
for rnd in range(ROUNDS):
    P, S = E[p], E[1 - p]
    stop = threading.Event()
    th, got, lock = writers(P, rnd, stop)
    time.sleep(rng.uniform(0.3, 2.5))
    P.kill9()                       # writers still running: no sleep before the kill
    t_kill = time.time()
    stop.set()
    for t in th:
        t.join(30)
    with lock:
        for k in ('fast', 'agent'):
            acked[k].update(got[k])
        hot_acked += got['hot']
        n_round = len(got['fast']) + len(got['agent']) + got['hot']
    sc = S.client(socketTimeoutMS=120000)
    retry(lambda: sc[DB]['fast'].find_one({'_id': 'hot'}), 120)
    first_read.append(time.time() - t_kill)
    missing = 0
    for k in ('fast', 'agent'):
        missing += len(acked[k]) - len(find_ids(sc, DB, k, acked[k]))
    h = sc[DB]['fast'].find_one({'_id': 'hot'})
    hot_now = (h or {}).get('n', 0)
    hot_short = max(0, hot_acked - hot_now)
    sc.close()
    total_missing += missing + hot_short
    OUT.say(f'round {rnd}: primary {P.name} killed after {n_round} acknowledged writes this round; spare {S.name} answered {first_read[-1]:.2f} s after the kill; '
            f'acknowledged so far fast={len(acked["fast"])} agent={len(acked["agent"])} hot={hot_acked} (spare has {hot_now}); missing={missing} hot_short={hot_short}')
    assert P.start(rnd % 2 == 1), f'{P.name} did not restart'
    p = 1 - p
for e in E:
    e.kill9()
log.down()
fr = sorted(first_read)
ok = total_missing == 0
OUT.say(f'first read after the kill: p50 {fr[len(fr) // 2]:.2f} s, max {fr[-1]:.2f} s')
OUT.say(f'RESULT takeover_rounds rounds={ROUNDS} acked_fast={len(acked["fast"])} acked_agent={len(acked["agent"])} acked_hot_incs={hot_acked} missing={total_missing} ->', 'PASS' if ok else 'FAIL')
sys.exit(0 if ok else 1)
