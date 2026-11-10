#!/usr/bin/env python3
"""Log-path fix: what the confirmed-state rule costs in latency. MEASURED on this machine (the 3 log copies share it).

Default-path writes (insert, w:majority) on the replicated log, p50 and p99 of the acknowledgement, three ways:
  alone       only default-path writers in the database
  lane-same   an agent-lane writer is busy in the SAME database (its batch waits for the window and the PUT; on the
              fixed engine a later default-path write is acknowledged only once that earlier batch is confirmed)
  lane-other  the agent-lane writer is busy in ANOTHER database
And reads: a point read by _id right after each acknowledged write (read-your-writes), its p50 and p99.
PUT latency modelled at 30 ms (`FENVIX_SIM_S3_PUT=30,0.3,200`), lane window 100 ms (the default).
  latency_mix.py OUT_DIR [--secs 15] [--clients 1,16]
"""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'agent_lane'))
from pymongo import WriteConcern
from pymongo.errors import PyMongoError
from common import *

args = sys.argv[1:]
OUT = Out(args[0], 'latency_mix')
SECS = float(opt(args, '--secs', '15'))
CLIENTS = [int(x) for x in opt(args, '--clients', '1,16').split(',')]
WORK = os.path.join(OUT.dir, 'work')
kill_leftovers((28141,), 9600)
shutil.rmtree(WORK, ignore_errors=True)
os.makedirs(WORK)
log = Log(os.path.join(WORK, 'log'), 9600)
log.up()
P = Engine(OUT.dir, 'P', 28141, ['--store', 'local', '--local-dir', os.path.join(WORK, 'store')], log=log, env={'FENVIX_SIM_S3_PUT': '30,0.3,200'})
assert P.start(True)
c = P.client()
for d in ('m', 'o'):
    c[d].create_collection('fast')
    c[d].create_collection('agent')
c.close()


def run(mode, n):
    stop = threading.Event()
    lat, rlat = [], []
    lock = threading.Lock()

    def lane(dbn):
        cl = P.client()
        col = cl[dbn].get_collection('agent', write_concern=WriteConcern(w='majority'))
        i = 0
        while not stop.is_set():
            try:
                col.insert_one({'_id': f'{mode}{n}-{i}', 'pad': 'a' * 400})
            except PyMongoError:
                pass
            i += 1

    def fast(k):
        cl, rc = P.client(), P.client()
        col = cl['m'].get_collection('fast', write_concern=WriteConcern(w='majority'))
        i = 0
        while not stop.is_set():
            _id = f'{mode}{n}-{k}-{i}'
            i += 1
            t0 = time.perf_counter()
            try:
                col.insert_one({'_id': _id, 'pad': 'f' * 400})
            except PyMongoError:
                continue
            t1 = time.perf_counter()
            found = rc['m']['fast'].find_one({'_id': _id}, {'_id': 1}) is not None
            t2 = time.perf_counter()
            with lock:
                lat.append((t1 - t0) * 1000)
                rlat.append((t2 - t1) * 1000)
                if not found:
                    rlat.append(float('inf'))
    th = []
    if mode == 'lane-same':
        th += [threading.Thread(target=lane, args=('m',)) for _ in range(4)]
    if mode == 'lane-other':
        th += [threading.Thread(target=lane, args=('o',)) for _ in range(4)]
    th += [threading.Thread(target=fast, args=(k,)) for k in range(n)]
    for t in th:
        t.daemon = True
        t.start()
    time.sleep(SECS)
    stop.set()
    for t in th:
        t.join(30)
    misses = sum(1 for x in rlat if x == float('inf'))
    rl = [x for x in rlat if x != float('inf')]
    OUT.say(f'{mode:10s} clients={n:2d}: write p50 {pct(lat, 50):7.2f} ms p99 {pct(lat, 99):7.2f} ms (n={len(lat)}); read after ack p50 {pct(rl, 50):6.2f} ms p99 {pct(rl, 99):6.2f} ms; misses {misses}')


OUT.say(f'binary {ENGINE}')
for n in CLIENTS:
    for mode in ('alone', 'lane-same', 'lane-other'):
        run(mode, n)
P.kill9()
log.down()
