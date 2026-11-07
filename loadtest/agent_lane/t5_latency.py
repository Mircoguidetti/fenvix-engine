#!/usr/bin/env python3
"""Agent lane, done-means 5: latency. Acknowledgement p50 and p99 per write and per transaction, with the PUT latency
modelled (`FENVIX_SIM_S3_PUT`, lognormal, median M ms, sigma 0.3) at M = 30, 100 and 200 ms (200: the top of the range S3
declares for small objects); the default path (2 of 3 log copies) beside it. Then the extra seconds of a failover.

Latency, for each M: a fresh primary with the replicated log; `agent` on the agent lane, `fast` on the default path.
  lone       one client, one write after the other (an agent between two steps)
  64 agents  64 clients at once (one writer per agent), 20 s each
For writes: single inserts of a 400-byte document. For transactions: 2 inserts (`agent`, `agent_b`) and the commit; the
time is from the start of the transaction to the commit's answer.

Failover: the primary writes the agent lane with 64 clients for --tail-secs, then is killed; a spare on the same object
storage (GET latency modelled too, `FENVIX_SIM_S3=25,0.6,90`, the M31 model) takes the database over after the lease (4 s).
Time from the kill to the spare's first answer, three ways: nothing written since the last memtable flush (baseline),
an agent-lane tail without following, the same tail with the spare following the WAL (`--agent-lane-follow`).
  t5_latency.py OUT_DIR [--secs 20] [--tail-secs 60] [--models 30,100,200]
"""
from pymongo import WriteConcern
from pymongo.errors import PyMongoError
from common import *

args = sys.argv[1:]
OUT = Out(args[0], 't5_latency')
SECS = float(opt(args, '--secs', '20'))
TAIL = float(opt(args, '--tail-secs', '60'))
MODELS = [int(x) for x in opt(args, '--models', '30,100,200').split(',')]
WORK = os.path.join(OUT.dir, 'work')
DB = 'lat'


def load(P, kind, clients, secs, n_max=None):
    lat = []
    lock = threading.Lock()
    stop = time.time() + secs
    seq = iter(range(10 ** 9))

    def run(k):
        c = P.client()
        a = c[DB].get_collection('agent' if kind != 'fast' else 'fast', write_concern=WriteConcern(w='majority'))
        b = c[DB].get_collection('agent_b' if kind != 'fasttxn' else 'fast_b', write_concern=WriteConcern(w='majority'))
        if kind == 'fasttxn':
            a = c[DB].get_collection('fast', write_concern=WriteConcern(w='majority'))
        while time.time() < stop and (n_max is None or len(lat) < n_max):
            with lock:
                i = next(seq)
            t = time.perf_counter()
            try:
                if kind in ('agent', 'fast'):
                    a.insert_one({'_id': f'{kind}{k}-{i}', 'pad': 'x' * 400})
                else:
                    with c.start_session() as s:
                        with s.start_transaction(write_concern=WriteConcern(w='majority')):
                            a.insert_one({'_id': f'{kind}{k}-{i}a', 'pad': 'x' * 400}, session=s)
                            b.insert_one({'_id': f'{kind}{k}-{i}b', 'pad': 'x' * 400}, session=s)
            except PyMongoError:
                continue
            with lock:
                lat.append((time.perf_counter() - t) * 1e3)
        c.close()

    ts = [threading.Thread(target=run, args=(k,)) for k in range(clients)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    return lat


def fmt(lat):
    return f'n={len(lat)} p50={pct(lat, 50):.1f} ms p99={pct(lat, 99):.1f} ms'


def latency(m):
    work = os.path.join(WORK, f'm{m}')
    shutil.rmtree(work, ignore_errors=True)
    log = Log(os.path.join(work, 'log'), 7000)
    log.up()
    P = Engine(OUT.dir, f'lat{m}', 27991, ['--store', 'local', '--local-dir', os.path.join(work, 'store')], log=log, env={'FENVIX_SIM_S3_PUT': f'{m},0.3,90'})
    assert P.start(True)
    c = P.client()
    for coll in ('agent', 'agent_b', 'fast', 'fast_b'):
        c[DB].create_collection(coll)
    c.close()
    res = {}
    for kind in ('agent', 'agenttxn', 'fast', 'fasttxn'):
        for clients in (1, 64):
            lat = load(P, kind, clients, SECS)
            res[(kind, clients)] = lat
            OUT.say(f'PUT model median {m} ms | {kind:8s} {"lone" if clients == 1 else "64 agents"}: {fmt(lat)}')
    st = lane_stats(P.client())
    OUT.say(f'PUT model median {m} ms | agentLane {st} | writes per PUT {st["commits"] / max(1, st["flushes"]):.1f}')
    P.kill9()
    log.down()
    return res


def takeover(mode):
    """mode: base (no tail), tail (agent-lane tail, spare not following), follow (same tail, spare following)."""
    work = os.path.join(WORK, f'fo-{mode}')
    shutil.rmtree(work, ignore_errors=True)
    log = Log(os.path.join(work, 'log'), 7000)
    log.up()
    st = ['--store', 'local', '--local-dir', os.path.join(work, 'store')]
    env = {'FENVIX_SIM_S3_PUT': '30,0.3,90', 'FENVIX_SIM_S3': '25,0.6,90'}
    P = Engine(OUT.dir, f'fo-{mode}-P', 27991, st, log=log, env=env, upload_secs=5)
    assert P.start(True)
    c = P.client()
    c[DB].create_collection('agent')
    c[DB].create_collection('agent_b')
    c[DB]['agent'].insert_one({'_id': 'probe'})
    c.close()
    time.sleep(6)
    S = Engine(OUT.dir, f'fo-{mode}-S', 27992, st, log=log, env=env, upload_secs=5, extra=(['--agent-lane-follow', DB] if mode == 'follow' else []))
    assert S.start(True)
    n = 0
    if mode != 'base':
        n = len(load(P, 'agent', 64, TAIL))
    ps = lane_stats(P.client())
    time.sleep(0.5)
    P.kill9()
    killed = time.time()
    sc = S.client(socketTimeoutMS=300000)
    first = None
    while time.time() - killed < 300:
        try:
            sc[DB]['agent'].find_one({'_id': 'probe'})
            first = time.time() - killed
            break
        except PyMongoError:
            time.sleep(0.05)
    reads = []
    for _ in range(200):
        t = time.perf_counter()
        sc[DB]['agent'].find_one({'_id': 'probe'})
        reads.append((time.perf_counter() - t) * 1e3)
    ss = lane_stats(sc)
    OUT.say(f'failover {mode}: tail writes {n}, WAL objects written by the primary {ps["flushes"]} (memtable flushes {ps.get("memtableFlushes")}), '
            f'spare followed {ss["followed"]} and answered {ss["followHits"]} reads from them; first answer {first:.2f} s after the kill '
            f'(lease ttl 4 s); then 200 point reads on the spare p50 {pct(reads, 50):.2f} ms p99 {pct(reads, 99):.2f} ms')
    sc.close()
    S.kill9()
    log.down()
    return first


def main():
    kill_leftovers((27991, 27992), 7000)
    shutil.rmtree(WORK, ignore_errors=True)
    os.makedirs(WORK)
    OUT.say('START', time.strftime('%F %T'), f'models={MODELS} secs={SECS} tail={TAIL}')
    for m in MODELS:
        latency(m)
    t = {mode: takeover(mode) for mode in ('base', 'tail', 'follow')}
    OUT.say(f'RESULT failover first answer after kill: base {t["base"]:.2f} s, agent-lane tail {t["tail"]:.2f} s (extra {t["tail"] - t["base"]:.2f} s), '
            f'tail with the spare following {t["follow"]:.2f} s (extra {t["follow"] - t["base"]:.2f} s)')


if __name__ == '__main__':
    main()
