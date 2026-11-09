#!/usr/bin/env python3
"""Agent lane, done-means 4 (second half): transactions over both lanes, the same as mongod.

The same seeded sequence of 3,000 transactions runs against mongod 7.0 (replica set) and against mongo-server (replicated
log on, `agent*` on the agent lane, PUT latency modelled at 30 ms). Each transaction does 1-6 operations, each on `agent`,
`agent_b` (agent lane) or `fast` (default path): insert, update ($inc / $set / $push), replace, delete, with now and
then a duplicate _id (the transaction fails and aborts); then commits, or aborts on purpose (1 in 5). Some transactions
touch only the agent lane, some only the default path, most both. Every outcome (committed, aborted, error code) and, at
the end, every document of the three collections are compared. Then mongo-server is killed (kill -9) and restarted with a
new local directory, and the documents are compared again (atomic across the crash too).
  t4_mixed_txn.py OUT_DIR [--txns 3000] [--seed 7]
"""
from pymongo import MongoClient, WriteConcern
from pymongo.errors import PyMongoError, OperationFailure
from common import *

args = sys.argv[1:]
OUT = Out(args[0], 't4_mixed_txn')
N = int(opt(args, '--txns', '3000'))
SEED = int(opt(args, '--seed', '7'))
WORK = os.path.join(OUT.dir, 'work')
DB = 'mixed'
COLLS = ['agent', 'agent_b', 'fast']
MONGOD = os.path.join(JH, 'env', 'bin', 'mongod')


def plan():
    r = random.Random(SEED)
    out = []
    for t in range(N):
        shape = r.choice(['agent', 'fast', 'both', 'both', 'both'])
        pool = {'agent': ['agent', 'agent_b'], 'fast': ['fast'], 'both': COLLS}[shape]
        ops = []
        for _ in range(r.randint(1, 6)):
            coll = r.choice(pool)
            k = r.randint(0, 300)
            kind = r.choice(['ins', 'ins', 'inc', 'set', 'push', 'rep', 'del'])
            ops.append((kind, coll, k, r.randint(0, 10 ** 6)))
        out.append((t, ops, 'abort' if r.random() < 0.2 else 'commit'))
    return out


def run(c, steps):
    outcomes = []
    for (t, ops, end) in steps:
        try:
            with c.start_session() as s:
                s.start_transaction(write_concern=WriteConcern(w='majority'))
                try:
                    for (kind, coll, k, v) in ops:
                        col = c[DB][coll]
                        if kind == 'ins':
                            col.insert_one({'_id': k, 'v': v, 'arr': [], 't': t}, session=s)
                        elif kind == 'inc':
                            col.update_one({'_id': k}, {'$inc': {'n': 1}, '$set': {'t': t}}, upsert=True, session=s)
                        elif kind == 'set':
                            col.update_many({'_id': {'$gte': k, '$lt': k + 3}}, {'$set': {'v': v, 't': t}}, session=s)
                        elif kind == 'push':
                            col.update_one({'_id': k}, {'$push': {'arr': v % 100}}, session=s)
                        elif kind == 'rep':
                            col.replace_one({'_id': k}, {'v': v, 'replaced': True, 't': t}, session=s)
                        else:
                            col.delete_one({'_id': k}, session=s)
                except OperationFailure as e:
                    s.abort_transaction()
                    outcomes.append((t, 'error', e.code))
                    continue
                if end == 'abort':
                    s.abort_transaction()
                    outcomes.append((t, 'aborted', None))
                else:
                    s.commit_transaction()
                    outcomes.append((t, 'committed', None))
        except PyMongoError as e:
            if not any(o[1] == 'failed' for o in outcomes):
                OUT.say(f'  first failure at transaction {t}: {type(e).__name__} {e}'[:400])
            outcomes.append((t, 'failed', getattr(e, 'code', None)))
    return outcomes


def dump(c):
    return {coll: sorted((repr(sorted(d.items())) for d in c[DB][coll].find()), key=str) for coll in COLLS}


def diff(a, b, show=False):
    n = 0
    for coll in COLLS:
        sa, sb = set(a[coll]), set(b[coll])
        n += len(sa ^ sb)
        if show:
            for x in sorted(sa - sb)[:6]:
                OUT.say(f'  {coll} only in mongod: {x}'[:300])
            for x in sorted(sb - sa)[:6]:
                OUT.say(f'  {coll} only in ours:   {x}'[:300])
    return n


def main():
    kill_leftovers((27030, 27993), 9000)
    shutil.rmtree(WORK, ignore_errors=True)
    os.makedirs(os.path.join(WORK, 'mongod'))
    steps = plan()
    m = subprocess.Popen([MONGOD, '--replSet', 'rs0', '--port', '27030', '--bind_ip', '127.0.0.1', '--dbpath', os.path.join(WORK, 'mongod')],
                         stdout=open(os.path.join(OUT.dir, 'mongod.log'), 'ab'), stderr=subprocess.STDOUT)
    wait_port(27030, 60)
    mc = MongoClient('mongodb://127.0.0.1:27030/?directConnection=true')
    mc.admin.command('replSetInitiate', {'_id': 'rs0', 'members': [{'_id': 0, 'host': 'localhost:27030'}]})
    while not mc.admin.command('hello').get('isWritablePrimary'):
        time.sleep(0.2)
    mc.close()
    mc = MongoClient('mongodb://127.0.0.1:27030/?directConnection=true')   # a new client sees the replica set (sessions)
    log = Log(os.path.join(WORK, 'log'), 9000)
    log.up()
    E = Engine(OUT.dir, 'mixed', 27993, ['--store', 'local', '--local-dir', os.path.join(WORK, 'store')], log=log, env={'FENVIX_SIM_S3_PUT': '30,0.3,90'})
    assert E.start(True)
    ec = E.client()
    for c in (mc, ec):
        for coll in COLLS:
            c[DB].create_collection(coll)
    t0 = time.time()
    om = run(mc, steps)
    tm = time.time() - t0
    t0 = time.time()
    oe = run(ec, steps)
    te = time.time() - t0
    st = lane_stats(ec)
    out_diff = sum(1 for a, b in zip(om, oe) if a != b)
    for a, b in zip(om, oe):
        if a != b:
            OUT.say('  outcome differs:', a, b)
            break
    dm, de = dump(mc), dump(ec)
    d1 = diff(dm, de)
    ec.close()
    E.kill9()
    assert E.start(True)
    ec = E.client()
    de2 = retry(lambda: dump(ec))
    d2 = diff(dm, de2, show=True)
    counts = {k: sum(1 for o in om if o[1] == k) for k in ('committed', 'aborted', 'error', 'failed')}
    OUT.say(f'transactions {N}: mongod {counts} in {tm:.0f} s; ours in {te:.0f} s; agentLane {st}')
    OUT.say(f'outcome differences {out_diff}; document differences {d1}; after kill -9 and restart on a new local directory {d2}; '
            f'documents: {", ".join(f"{k} {len(v)}" for k, v in dm.items())}')
    ec.close()
    E.kill9()
    log.down()
    m.terminate()
    ok = out_diff == 0 and d1 == 0 and d2 == 0
    OUT.say('RESULT', 'PASS' if ok else 'FAIL')
    sys.exit(0 if ok else 1)


if __name__ == '__main__':
    main()
