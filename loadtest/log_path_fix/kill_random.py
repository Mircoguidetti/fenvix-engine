#!/usr/bin/env python3
"""Log-path fix (P-log-path-fix, 2026-11-08): kill -9 at random points; no acknowledged write lost, no dirty read.

One engine with the replicated log (3 copies, `common.Log`) and the agent lane on `agent*`. Each round restarts it
(a fresh local directory every second round; the upload interval drawn from 2-30 s, so some kills land near an upload),
runs writers and readers for a random 0.5-4 s, then kills it (kill -9) with everything in flight. Writers (w:majority):
default-path inserts, agent-lane inserts, mixed transactions over both, and `$inc` on a hot document of each lane.
Before each insert the writer announces the id, so readers can ask for writes that are still on their way.

A dirty read is a read that returned a write that was then lost: readers (readConcern "majority", and readConcern
"local", which on the log path reads the same confirmed state) look up announced ids and the hot counters; a change
stream records every inserted id it announces. After the restart, every acknowledged write must be there, every id a
reader or the stream saw must be there, and each hot counter must be at least the highest value a reader saw and at
least its number of acknowledged increments.
  kill_random.py OUT_DIR [--rounds 100] [--seed N] [--one-copy-down]
"""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'agent_lane'))
from pymongo import WriteConcern
from pymongo.read_concern import ReadConcern
from pymongo.errors import PyMongoError
from common import *

args = sys.argv[1:]
OUT = Out(args[0], 'kill_random')
ROUNDS = int(opt(args, '--rounds', '100'))
SEED = int(opt(args, '--seed', str(int(time.time()))))
# --one-copy-down: one of the 3 log copies is frozen (SIGSTOP) for the whole round, a different one each round
ONE_DOWN = '--one-copy-down' in args
rng = random.Random(SEED)
WORK = os.path.join(OUT.dir, 'work')
DB = 'kr'
kill_leftovers((28131,), 9500)
shutil.rmtree(WORK, ignore_errors=True)
os.makedirs(WORK)
log = Log(os.path.join(WORK, "log"), 9500)
log.up()
P = Engine(OUT.dir, 'P', 28131, ['--store', 'local', '--local-dir', os.path.join(WORK, 'store')], log=log, extra=['--agent-lane-window-ms', '50'])
OUT.say(f'seed {SEED}, {ROUNDS} rounds' + (', one log copy frozen in every round' if ONE_DOWN else ''))

acked = {'fast': set(), 'agent': set()}
seen = {'fast': set(), 'agent': set()}
hot_acked = {'fast': 0, 'agent': 0}
hot_seen = {'fast': 0, 'agent': 0}
tot = {'lost': 0, 'dirty': 0, 'reads_of_inflight': 0, 'stream_events': 0}


def run_round(rnd, secs):
    stop = threading.Event()
    lock = threading.Lock()
    inflight = []           # (coll, id) announced, maybe not acknowledged
    got = {'fast': [], 'agent': [], 'hot_fast': 0, 'hot_agent': 0}
    saw = {'fast': set(), 'agent': set(), 'hot_fast': 0, 'hot_agent': 0, 'reads': 0, 'events': 0}

    def ins(coll, t):
        c = P.client(socketTimeoutMS=20000)
        col = c[DB].get_collection(coll, write_concern=WriteConcern(w='majority'))
        i = 0
        while not stop.is_set():
            _id = f'{coll[0]}{rnd}-{t}-{i}'
            i += 1
            with lock:
                inflight.append((coll, _id))
            try:
                col.insert_one({'_id': _id, 'pad': 'x' * 100})
            except PyMongoError:
                time.sleep(0.05)   # the previous run's lease (4 s), or the kill: go on until the round ends
                continue
            with lock:
                got[coll].append(_id)

    def txn():
        c = P.client(socketTimeoutMS=20000)
        i = 0
        while not stop.is_set():
            a, b = f'xf{rnd}-{i}', f'xa{rnd}-{i}'
            i += 1
            with lock:
                inflight.extend([('fast', a), ('agent', b)])
            try:
                with c.start_session() as s:
                    with s.start_transaction(write_concern=WriteConcern(w='majority')):
                        c[DB]['fast'].insert_one({'_id': a}, session=s)
                        c[DB]['agent'].insert_one({'_id': b}, session=s)
            except PyMongoError:
                time.sleep(0.05)   # the previous run's lease (4 s), or the kill: go on until the round ends
                continue
            with lock:
                got['fast'].append(a)
                got['agent'].append(b)

    def hot(coll):
        c = P.client(socketTimeoutMS=20000)
        col = c[DB].get_collection(coll, write_concern=WriteConcern(w='majority'))
        while not stop.is_set():
            try:
                col.update_one({'_id': 'hot'}, {'$inc': {'n': 1}}, upsert=True)
            except PyMongoError:
                time.sleep(0.05)   # the previous run's lease (4 s), or the kill: go on until the round ends
                continue
            with lock:
                got['hot_' + coll] += 1

    def reader(level):
        c = P.client(socketTimeoutMS=20000)
        cols = {k: c[DB].get_collection(k, read_concern=ReadConcern(level)) for k in ('fast', 'agent')}
        while not stop.is_set():
            with lock:
                pick = inflight[-8:]
            try:
                for coll, _id in pick:
                    if cols[coll].find_one({'_id': _id}, {'_id': 1}) is not None:
                        with lock:
                            saw[coll].add(_id)
                    with lock:
                        saw['reads'] += 1
                for coll in ('fast', 'agent'):
                    h = cols[coll].find_one({'_id': 'hot'})
                    if h:
                        with lock:
                            saw['hot_' + coll] = max(saw['hot_' + coll], h.get('n', 0))
            except PyMongoError:
                time.sleep(0.05)   # the previous run's lease (4 s), or the kill: go on until the round ends
                continue

    def stream():
        c = P.client(socketTimeoutMS=20000)
        while not stop.is_set():
            try:
                with c[DB].watch([{'$match': {'operationType': 'insert'}}], max_await_time_ms=200) as cs:
                    while not stop.is_set():
                        ev = cs.try_next()
                        if ev is None:
                            continue
                        coll = ev['ns']['coll']
                        if coll in saw:
                            with lock:
                                saw[coll].add(ev['documentKey']['_id'])
                                saw['events'] += 1
            except PyMongoError:
                time.sleep(0.05)

    th = [threading.Thread(target=ins, args=('fast', 0)), threading.Thread(target=ins, args=('fast', 1)),
          threading.Thread(target=ins, args=('agent', 0)), threading.Thread(target=ins, args=('agent', 1)),
          threading.Thread(target=txn), threading.Thread(target=hot, args=('fast',)), threading.Thread(target=hot, args=('agent',)),
          threading.Thread(target=reader, args=('majority',)), threading.Thread(target=reader, args=('local',)), threading.Thread(target=stream)]
    for t in th:
        t.daemon = True
        t.start()
    time.sleep(secs)
    P.kill9()
    stop.set()
    for t in th:
        t.join(30)
    return got, saw


for rnd in range(ROUNDS):
    up = rng.choice([2, 5, 10, 30])
    P.upload_secs = up
    assert P.start(rnd % 2 == 1), 'engine did not start'
    c = P.client()
    # the database is open as a writer here (the killed run's lease has lapsed) before the timed part starts
    retry(lambda: c[DB].get_collection('fast', write_concern=WriteConcern(w='majority')).update_one({'_id': 'probe'}, {'$set': {'r': rnd}}, upsert=True), 120)
    c.close()
    secs = rng.uniform(0.5, 4.0)
    if ONE_DOWN:
        log.cl('sigstop', str(rnd % 3))
    got, saw = run_round(rnd, secs)
    if ONE_DOWN:
        log.cl('sigcont', str(rnd % 3))
    for k in ('fast', 'agent'):
        acked[k].update(got[k])
        seen[k].update(saw[k])
        hot_acked[k] += got['hot_' + k]
        hot_seen[k] = max(hot_seen[k], saw['hot_' + k])
    tot['reads_of_inflight'] += saw['reads']
    tot['stream_events'] += saw['events']
    # restart and check everything so far
    assert P.start(rnd % 2 == 0), 'engine did not restart'
    c = P.client(socketTimeoutMS=120000)
    retry(lambda: c[DB]['fast'].find_one({'_id': 'hot'}), 120)
    lost = dirty = 0
    for k in ('fast', 'agent'):
        have = find_ids(c, DB, k, acked[k] | seen[k])
        lost += len(acked[k] - have)
        dirty += len(seen[k] - have)
        h = (c[DB][k].find_one({'_id': 'hot'}) or {}).get('n', 0)
        lost += max(0, hot_acked[k] - h)
        dirty += 1 if hot_seen[k] > h else 0
    c.close()
    P.kill9()
    tot['lost'] += lost
    tot['dirty'] += dirty
    OUT.say(f'round {rnd}: upload {up} s, killed after {secs:.2f} s; acknowledged fast={len(acked["fast"])} agent={len(acked["agent"])} '
            f'hot={hot_acked["fast"]}/{hot_acked["agent"]}; seen by readers or the stream fast={len(seen["fast"])} agent={len(seen["agent"])}; lost={lost} dirty={dirty}')
log.down()
ok = tot['lost'] == 0 and tot['dirty'] == 0
OUT.say(f'reads of in-flight writes {tot["reads_of_inflight"]}, change-stream insert events {tot["stream_events"]}')
OUT.say(f'RESULT kill_random rounds={ROUNDS} acked_fast={len(acked["fast"])} acked_agent={len(acked["agent"])} acked_hot={hot_acked["fast"]}+{hot_acked["agent"]} '
        f'seen={len(seen["fast"]) + len(seen["agent"])} lost={tot["lost"]} dirty={tot["dirty"]} ->', 'PASS' if ok else 'FAIL')
sys.exit(0 if ok else 1)
