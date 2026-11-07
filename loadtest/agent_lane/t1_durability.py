#!/usr/bin/env python3
"""Agent lane, done-means 1: durability. kill -9 of the primary at 100 points; 0 acknowledged writes lost against a model.

Each round, 8 writer threads on 3 databases, every write remembered (acknowledged or not):
  agent insert  (collection `agent`, agent lane, ack after the WAL object is in object storage)
  agent txn     (2 documents in `agent`, 1 in `agent_b`: agent lane, one PUT at the commit)
  fast insert   (collection `fast`, the default path: ack by 2 of 3 log copies)
  mixed txn     (1 in `agent`, 1 in `fast`: takes the default path as a whole)
  hot update    each thread's own document in `agent` (`hot-<thread>`), its version raised alternately by an agent-lane
                update and by a mixed transaction (that also writes `fast`): after a restart its version must be at least
                the last acknowledged one (an older batch replayed from the log over a newer one in object storage fails)
The kill point of the round, in turn:
  random        any moment after 0.3-2 s of writes
  during_put    a WAL PUT is in flight (fault point `delay_put /wal/ 40`: the kill lands inside the 40 ms of the PUT)
  after_put     a WAL object is stored and its writer has not been told yet (`delay_after_put /wal/ 120`)
Every second round restarts with a NEW local directory (the primary's disk wiped, as after the loss of its zone).
Checks after every restart: every acknowledged document is there; every transaction (acknowledged or not) is all there or
not at all; and at the end, the same over everything.
Negative control (`--control`): agent-lane writes acknowledged before their PUT (MONGO_TEST_AGENT_ACK_EARLY=1): the check
must see losses.
  t1_durability.py OUT_DIR [--rounds 100] [--control] [--seed 1]      exit 0 = PASS (or, with --control, loss seen)
"""
import itertools
from pymongo import WriteConcern
from pymongo.errors import PyMongoError
from common import *

args = sys.argv[1:]
OUT = Out(args[0], 't1_durability')
ROUNDS = int(opt(args, '--rounds', '100'))
CONTROL = '--control' in args
random.seed(int(opt(args, '--seed', '1')))
WORK = os.path.join(OUT.dir, 'work')
DBS = [f'dur_{j}' for j in range(3)]
FAULTS = os.path.join(OUT.dir, 'faults')

acked = set()          # (db, coll, _id) acknowledged
hot_acked = {}         # (db, thread) -> last acknowledged version of the thread's hot document
hot_tried = {}         # (db, thread) -> highest version tried
txns = {}              # txn key -> [(db, coll, _id)] of every transaction tried
lock = threading.Lock()
seq = itertools.count()


def writer(eng, k, rnd, stop):
    c = eng.client()
    while not stop.is_set():
        i = next(seq)
        db = DBS[i % len(DBS)]
        kind = random.choice(['a1', 'a1', 'at', 'f1', 'mx', 'hot', 'hot'])
        base = f'r{rnd}-t{k}-{i}'
        try:
            if kind == 'hot':
                key = (db, k)
                v = hot_tried.get(key, 0) + 1
                hot_tried[key] = v
                if v % 2:
                    c[db].get_collection('agent', write_concern=WriteConcern(w='majority')).update_one({'_id': f'hot-{k}'}, {'$set': {'v': v}}, upsert=True)
                else:
                    with c.start_session() as s:
                        with s.start_transaction(write_concern=WriteConcern(w='majority')):
                            c[db]['agent'].update_one({'_id': f'hot-{k}'}, {'$set': {'v': v}}, upsert=True, session=s)
                            c[db]['fast'].update_one({'_id': f'hot-{k}'}, {'$set': {'v': v}}, upsert=True, session=s)
                with lock:
                    hot_acked[key] = max(hot_acked.get(key, 0), v)
            elif kind in ('a1', 'f1'):
                coll = 'agent' if kind == 'a1' else 'fast'
                c[db].get_collection(coll, write_concern=WriteConcern(w='majority')).insert_one({'_id': base, 'pad': 'x' * 300})
                with lock:
                    acked.add((db, coll, base))
            else:
                docs = [(db, 'agent', base + '-0'), (db, 'agent', base + '-1'), (db, 'agent_b', base + '-2')] if kind == 'at' else [(db, 'agent', base + '-0'), (db, 'fast', base + '-1')]
                with lock:
                    txns[base] = docs
                with c.start_session() as s:
                    with s.start_transaction(write_concern=WriteConcern(w='majority')):
                        for (d, coll, _id) in docs:
                            c[d][coll].insert_one({'_id': _id, 'pad': 'y' * 300}, session=s)
                with lock:
                    acked.update(docs)
        except PyMongoError:
            pass
    c.close()


def check(eng, everything):
    c = eng.client()
    lost, torn = 0, 0
    want = {}
    for (db, coll, _id) in (acked if everything else [x for x in acked if x[2].startswith(f'r{cur_round}-')]):
        want.setdefault((db, coll), set()).add(_id)
    present = set()
    for (db, coll), ids in want.items():
        f = find_ids(c, db, coll, ids)
        lost += len(ids) - len(f)
        present.update((db, coll, x) for x in f)
    tx = txns.items() if everything else [(k, v) for k, v in txns.items() if k.startswith(f'r{cur_round}-')]
    for key, docs in tx:
        n = 0
        for (db, coll, _id) in docs:
            if (db, coll, _id) in present or retry(lambda: c[db][coll].find_one({'_id': _id}, {'_id': 1})) is not None:
                n += 1
        if n not in (0, len(docs)):
            torn += 1
            OUT.say(f'  TORN transaction {key}: {n} of {len(docs)} documents')
    stale = 0
    for (db, k), v in hot_acked.items():
        d = retry(lambda: c[db]['agent'].find_one({'_id': f'hot-{k}'}))
        got = d['v'] if d else 0
        if got < v or got > hot_tried[(db, k)]:
            stale += 1
            OUT.say(f'  STALE hot document {db}.agent hot-{k}: version {got}, last acknowledged {v}, highest tried {hot_tried[(db, k)]}')
    c.close()
    return lost, torn + stale


cur_round = 0


def main():
    global cur_round
    kill_leftovers((27961,), 3200)
    shutil.rmtree(WORK, ignore_errors=True)
    os.makedirs(WORK)
    open(FAULTS, 'w').write('')
    log = Log(os.path.join(WORK, 'log'), 3200)
    log.up()
    env = {'MONGO_TEST_AGENT_ACK_EARLY': '1'} if CONTROL else {}
    eng = Engine(OUT.dir, 'p', 27961, ['--store', 'local', '--local-dir', os.path.join(WORK, 'store')], log=log, env=env, faults=FAULTS)
    assert eng.start(True), 'engine did not start'
    OUT.say('START', time.strftime('%F %T'), 'rounds', ROUNDS, 'control' if CONTROL else '')
    total_lost = total_torn = 0
    counts = {}
    t0 = time.time()
    for rnd in range(ROUNDS):
        cur_round = rnd
        kind = ['random', 'during_put', 'after_put'][rnd % 3]
        rule = {'random': 'delay_put /wal/ 30\n', 'during_put': 'delay_put /wal/ 40\n', 'after_put': 'delay_after_put /wal/ 120\n'}[kind]
        open(FAULTS, 'w').write(rule)
        time.sleep(0.05)
        stop = threading.Event()
        ws = [threading.Thread(target=writer, args=(eng, k, rnd, stop)) for k in range(8)]
        [w.start() for w in ws]
        time.sleep(random.uniform(0.3, 2.0))
        hit = True
        if kind != 'random':
            needle = 'FAULT delay_put' if kind == 'during_put' else 'FAULT delay_after_put'
            hit = eng.wait_line(eng.log_size(), needle, 10) is not None
            time.sleep(random.uniform(0, 0.035 if kind == 'during_put' else 0.110))
        eng.kill9()
        stop.set()
        [w.join() for w in ws]
        counts[kind] = counts.get(kind, 0) + (1 if hit else 0)
        fresh = rnd % 2 == 1
        if not eng.start(fresh):
            OUT.say(f'round {rnd}: ENGINE DID NOT RESTART')
            total_lost = -1
            break
        lost, torn = check(eng, False)
        total_lost += lost
        total_torn += torn
        OUT.say(f'round {rnd} {kind} point_hit={hit} disk_wiped={fresh} acked_total={len(acked)} txns_tried={len(txns)} hot_updates_acked={sum(hot_acked.values())} lost={lost} torn_or_stale={torn} t={time.time() - t0:.0f}s')
        if CONTROL and lost > 0:
            break
    if not CONTROL and total_lost == 0:
        lost, torn = check(eng, True)
        total_lost += lost
        total_torn += torn
        OUT.say(f'final full check: acked={len(acked)} lost={lost} torn={torn}')
    st = lane_stats(eng.client())
    OUT.say('agentLane counters (last run of the engine):', st)
    eng.kill9()
    log.down()
    n_agent = sum(1 for x in acked if x[1].startswith('agent'))
    if CONTROL:
        ok = total_lost > 0
        OUT.say(f'RESULT control: lost={total_lost} ->', 'LOSS SEEN (the check works)' if ok else 'NO LOSS SEEN (the check is blind!)')
    else:
        ok = total_lost == 0 and total_torn == 0
        OUT.say(f'RESULT kills={ROUNDS} points_hit={counts} acked={len(acked)} (agent lane {n_agent}) txns_tried={len(txns)} hot_versions_acked={sum(hot_acked.values())} lost={total_lost} torn_or_stale={total_torn} ->', 'PASS' if ok else 'FAIL')
    sys.exit(0 if ok else 1)


if __name__ == '__main__':
    main()
