#!/usr/bin/env python3
"""Agent lane, done-means 3: read your writes. 100,000 write-then-read and 100,000 transaction-then-read: 0 misses; then the
warm spare takes over and serves every confirmed write.

Primary P (replicated log on, agent lane on `agent*`, PUT latency modelled at 30 ms) and a warm spare S on the same
object storage, following the database's WAL objects (`--agent-lane-follow`). 64 threads; each acknowledged write is read
back at once through another connection (and so is each acknowledged transaction's every document). Then P is killed
(kill -9), S takes the database over when P's lease lapses (4 s) and every acknowledged document is read on S.
Also 20,000 write-then-read on the default path (collection `fast`, acknowledged by the log), the same check.
The spare's check after the takeover covers the default-path documents too; they reach object storage within the
upload interval (5 s here) before the kill, see the report (a live takeover does not read the log's records).
  t3_read_your_writes.py OUT_DIR [--writes 100000] [--txns 100000] [--fast-writes 20000] [--threads 64]
"""
from pymongo import WriteConcern
from pymongo.errors import PyMongoError
from common import *

args = sys.argv[1:]
OUT = Out(args[0], 't3_read_your_writes')
NW = int(opt(args, '--writes', '100000'))
NT = int(opt(args, '--txns', '100000'))
NF = int(opt(args, '--fast-writes', '20000'))
TH = int(opt(args, '--threads', '64'))
WORK = os.path.join(OUT.dir, 'work')
DB = 'ryw'


def phase(P, n, txn, coll_a='agent', tag=''):
    cnt = {'done': 0, 'miss': 0, 'err': 0}
    acked = []
    lock = threading.Lock()
    it = iter(range(n))

    def run(k):
        wc = P.client()
        rc = P.client()
        wcoll = wc[DB].get_collection(coll_a, write_concern=WriteConcern(w='majority'))
        wcoll2 = wc[DB].get_collection('agent_b', write_concern=WriteConcern(w='majority'))
        while True:
            with lock:
                i = next(it, None)
            if i is None:
                break
            try:
                if not txn:
                    ids = [(coll_a, f'w{tag}{i}')]
                    wcoll.insert_one({'_id': f'w{tag}{i}', 'k': k, 'pad': 'p' * 200})
                else:
                    ids = [('agent', f't{i}a'), ('agent_b', f't{i}b')]
                    with wc.start_session() as s:
                        with s.start_transaction(write_concern=WriteConcern(w='majority')):
                            wcoll.insert_one({'_id': f't{i}a', 'pad': 'q' * 200}, session=s)
                            wcoll2.insert_one({'_id': f't{i}b', 'pad': 'q' * 200}, session=s)
            except PyMongoError:
                with lock:
                    cnt['err'] += 1
                continue
            miss = sum(1 for (coll, _id) in ids if rc[DB][coll].find_one({'_id': _id}, {'_id': 1}) is None)
            with lock:
                cnt['done'] += 1
                cnt['miss'] += miss
                acked.extend(ids)
        wc.close()
        rc.close()

    t0 = time.time()
    ts = [threading.Thread(target=run, args=(k,)) for k in range(TH)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    return cnt, acked, time.time() - t0


def main():
    kill_leftovers((27981, 27982), 6000)
    shutil.rmtree(WORK, ignore_errors=True)
    os.makedirs(WORK)
    log = Log(os.path.join(WORK, 'log'), 6000)
    log.up()
    st = ['--store', 'local', '--local-dir', os.path.join(WORK, 'store')]
    env = {'FENVIX_SIM_S3_PUT': '30,0.3,90'}
    P = Engine(OUT.dir, 'P', 27981, st, log=log, env=env, upload_secs=5)
    S = Engine(OUT.dir, 'S', 27982, st, log=log, env=env, upload_secs=5, extra=['--agent-lane-follow', DB])
    assert P.start(True), 'primary did not start'
    c = P.client()
    c[DB].create_collection('agent')
    c[DB].create_collection('agent_b')
    c[DB].create_collection('fast')
    c.close()
    time.sleep(6)       # the catalog reaches object storage before the spare starts (see the report: takeover of log records)
    assert S.start(True), 'spare did not start'
    OUT.say('START', time.strftime('%F %T'), f'writes={NW} txns={NT} threads={TH}')
    cw, aw, tw = phase(P, NW, False)
    OUT.say(f'write-then-read: {cw} in {tw:.0f} s ({cw["done"] / tw:.0f}/s)')
    ct, at, tt = phase(P, NT, True)
    OUT.say(f'transaction-then-read: {ct} in {tt:.0f} s ({ct["done"] / tt:.0f}/s)')
    cf, af, tf = phase(P, NF, False, coll_a='fast', tag='f')
    OUT.say(f'default path (the log) write-then-read: {cf} in {tf:.0f} s ({cf["done"] / tf:.0f}/s)')
    OUT.say('primary agentLane:', lane_stats(P.client()))
    OUT.say('spare agentLane before takeover:', lane_stats(S.client()))
    time.sleep(7)       # one upload of the log (every 5 s here) puts the default-path writes in object storage
    P.kill9()
    killed = time.time()
    sc = S.client(socketTimeoutMS=120000)
    first = None
    while time.time() - killed < 120:
        try:
            sc[DB]['agent'].find_one({'_id': 'w0'})
            first = time.time() - killed
            break
        except PyMongoError:
            time.sleep(0.05)
    OUT.say(f'spare served its first read {first:.2f} s after the kill (lease ttl 4 s)' if first else 'spare never served')
    acked = aw + at + af
    by = {}
    for coll, _id in acked:
        by.setdefault(coll, []).append(_id)
    missing = 0
    for coll, ids in by.items():
        missing += len(ids) - len(find_ids(sc, DB, coll, ids))
    OUT.say(f'spare after takeover: {len(acked)} acknowledged documents checked, missing {missing}')
    OUT.say('spare agentLane after takeover:', lane_stats(sc))
    sc.close()
    S.kill9()
    log.down()
    ok = cw['miss'] == 0 and ct['miss'] == 0 and cf['miss'] == 0 and missing == 0 and first is not None
    OUT.say(f'RESULT write_misses={cw["miss"]} txn_misses={ct["miss"]} default_path_misses={cf["miss"]} spare_missing={missing} ->', 'PASS' if ok else 'FAIL')
    sys.exit(0 if ok else 1)


if __name__ == '__main__':
    main()
