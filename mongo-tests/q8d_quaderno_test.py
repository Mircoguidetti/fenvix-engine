#!/usr/bin/env python3
"""Q8d: the replicated log (quaderno) inside the engine. No acknowledged write is ever lost.

A local group of 3 log copies (proto/quaderno/scripts/cluster.py, real fdatasync) and one mongo-server with
`--quaderno` and a long upload interval (30 s), so at every kill the acknowledged writes are only in the log, not in the
object store (here `--store local`, a directory). Each round:
  - 4 writer threads insert documents into 6 databases (w:majority and w:1 mixed, retryWrites off) and remember every
    acknowledged _id; one collection per database has a secondary index on `v`;
  - the engine is killed with SIGKILL at a random moment (1 to 3 s of writes);
  - depending on the round, also: the log's leader is killed during the writes (the engine follows the new leader),
    one log copy stays down for the whole round (2 of 3 left), or the engine is killed a second time during its
    restart (while it replays);
  - the engine restarts, half of the times with a NEW local directory (as on another machine: nothing of the old one);
  - check: every acknowledged _id is there, and per database the index on `v` counts the same documents as the
    collection (indexes consistent with data).
Negative control (`--control`): the same with the replay from the log disabled (test-only MONGO_TEST_LOCALLOG_NO_REPLAY):
it must find lost writes, which shows that the check can see a loss.

  q8d_quaderno_test.py OUT_DIR [--rounds 100] [--control] [--seed 1]
Exit 0 = PASS (or, with --control, = the loss was seen).
"""
import json, os, random, shutil, signal, subprocess, sys, threading, time

from pymongo import MongoClient, WriteConcern
from pymongo.errors import PyMongoError

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
ENGINE = os.environ.get('MONGO_SERVER_BIN', os.path.join(ROOT, 'target', 'release', 'mongo-server'))
PQ = os.environ.get('PQ_BIN', os.path.join(ROOT, 'proto', 'quaderno', 'target', 'release', 'pq'))
CLUSTER = os.path.join(ROOT, 'proto', 'quaderno', 'scripts', 'cluster.py')


def opt(a, k, d):
    return a[a.index(k) + 1] if k in a else d


args = sys.argv[1:]
OUT = os.path.abspath(args[0])
ROUNDS = int(opt(args, '--rounds', '100'))
CONTROL = '--control' in args
random.seed(int(opt(args, '--seed', '1')))
WORK = os.path.join(OUT, 'work')
QWORK = os.path.join(WORK, 'log')
STORE = os.path.join(WORK, 'store')
PORT = int(opt(args, '--port', '27941'))
OFF = int(os.environ.get('Q8D_PORT_OFF', '3000'))
REPLICAS = ','.join(f'127.0.0.1:{OFF + 7200 + i}' for i in range(3))
DBS = [f'q8d_{j}' for j in range(6)]
os.makedirs(OUT, exist_ok=True)
LOG = open(os.path.join(OUT, 'q8d_quaderno_test.log'), 'a')
QENV = dict(os.environ, PQ_WORK=QWORK, PQ_BIN=PQ, PQ_PORT_OFF=str(OFF), PQ_CORSIE='1', PQ_RICICLA='1')


def say(*x):
    s = ' '.join(str(v) for v in x)
    print(s, flush=True)
    LOG.write(s + '\n')
    LOG.flush()


def cl(*a):
    return subprocess.run(['python3', CLUSTER] + list(a), env=QENV, capture_output=True, text=True, timeout=120).stdout


def log_leader():
    out = subprocess.run([PQ, 'status', '--replicas', REPLICAS], capture_output=True, text=True, timeout=20).stdout
    for l in out.splitlines():
        try:
            s = json.loads(l)
        except Exception:
            continue
        if s.get('role') == 'Leader':
            return s['id']
    return -1


class Engine:
    def __init__(self):
        self.p = None
        self.n = 0

    def start(self, fresh_dir, env=None, wait=True):
        self.n += 1
        d = os.path.join(WORK, f'engine-local-{self.n if fresh_dir else 0}')
        os.makedirs(d, exist_ok=True)
        cmd = [ENGINE, '--listen', f'127.0.0.1:{PORT}', '--store', 'local', '--local-dir', STORE, '--no-ttl',
               '--replica-set', 'rs0', '--lease-ttl-secs', '4', '--holder', f'q8d-{self.n if fresh_dir else 0}', '--quaderno', REPLICAS, '--local-log-upload-secs', '30', '--local-log-dir', d, '--idle-secs', '600']
        e = dict(os.environ, **(env or {}))
        self.p = subprocess.Popen(cmd, stdout=open(os.path.join(OUT, 'engine.log'), 'ab'), stderr=subprocess.STDOUT, env=e, cwd=d)
        if wait:
            return self.wait()
        return True

    def wait(self, t=120):
        end = time.time() + t
        import socket
        while time.time() < end:
            if self.p.poll() is not None:
                return False
            try:
                socket.create_connection(('127.0.0.1', PORT), 0.3).close()
                return True
            except OSError:
                time.sleep(0.05)
        return False

    def kill9(self):
        if self.p and self.p.poll() is None:
            self.p.send_signal(signal.SIGKILL)
            self.p.wait()


def client():
    return MongoClient(f'mongodb://127.0.0.1:{PORT}/?directConnection=true', retryWrites=False, serverSelectionTimeoutMS=5000,
                       socketTimeoutMS=20000, connectTimeoutMS=2000)


acked = {db: set() for db in DBS}
acked_lock = threading.Lock()


def writer(k, rnd, stop):
    c = client()
    i = 0
    while not stop.is_set():
        db = DBS[(k + i) % len(DBS)]
        w = 'majority' if (i % 2 == 0) else 1
        coll = c[db].get_collection('c', write_concern=WriteConcern(w=w))
        _id = f'r{rnd}-t{k}-{i}'
        try:
            coll.insert_one({'_id': _id, 'v': rnd * 1_000_000 + k * 100_000 + i, 'pad': 'x' * 200})
            with acked_lock:
                acked[db].add(_id)
                round_ids[db].add(_id)
        except PyMongoError:
            pass   # not acknowledged: may or may not be there
        i += 1
    c.close()


round_ids = {db: set() for db in DBS}


def retry(f, t=120):
    """Run f; a database still in recovery (DatabaseRecoveryPending, 134: its records from the log are being applied)
    or a reconnect is retried, as a driver would."""
    end = time.time() + t
    while True:
        try:
            return f()
        except PyMongoError as e:
            if time.time() > end or not (getattr(e, 'code', None) == 134 or 'recovery' in str(e) or 'AutoReconnect' in type(e).__name__):
                raise
            time.sleep(0.2)


def check(rnd, full=False):
    """Every _id acknowledged in this round, plus a random sample of 1,000 older ones per database (all of them with
    `full`); every 10 rounds and at the end, the index check."""
    c = client()
    lost = 0
    bad_index = []
    for db in DBS:
        old = list(acked[db] - round_ids[db])
        ids = list(round_ids[db]) + (old if full else random.sample(old, min(1000, len(old))))
        found = set()
        for j in range(0, len(ids), 2000):
            for d in retry(lambda: list(c[db]['c'].find({'_id': {'$in': ids[j:j + 2000]}}, {'_id': 1}))):
                found.add(d['_id'])
        lost += len(ids) - len(found)
        if ids and (full or rnd % 10 == 9):
            n_coll = retry(lambda: c[db]['c'].count_documents({}))
            n_idx = retry(lambda: c[db]['c'].count_documents({'v': {'$gte': 0}}, hint='v_1'))
            if n_coll != n_idx:
                bad_index.append((db, n_coll, n_idx))
    c.close()
    return lost, bad_index


def main():
    shutil.rmtree(WORK, ignore_errors=True)
    os.makedirs(STORE)
    say('START', time.strftime('%F %T'), 'rounds', ROUNDS, 'control' if CONTROL else '')
    cl('up', '--fresh', '--no-proxy', '--window-ms', '60000', '--mem-cap-mb', '1024')
    say('log leader', log_leader())
    eng = Engine()
    assert eng.start(True), 'engine did not start'
    c = client()
    for db in DBS:
        c[db]['c'].create_index('v', name='v_1')
    c.close()
    total_lost = 0
    kills = 0
    t0 = time.time()
    for rnd in range(ROUNDS):
        kind = ['plain', 'log_leader_dies', 'one_copy_off', 'kill_during_replay', 'plain'][rnd % 5]
        down = None
        if kind == 'one_copy_off':
            down = random.choice([0, 1, 2])
            cl('kill', str(down))
        for db in DBS:
            round_ids[db].clear()
        stop = threading.Event()
        ws = [threading.Thread(target=writer, args=(k, rnd, stop)) for k in range(4)]
        [w.start() for w in ws]
        dur = random.uniform(1.0, 3.0)
        if kind == 'log_leader_dies':
            time.sleep(dur / 2)
            ld = log_leader()
            if ld >= 0:
                cl('kill', str(ld))
                time.sleep(0.3)
                cl('start', str(ld))
            time.sleep(dur / 2)
        else:
            time.sleep(dur)
        eng.kill9()
        kills += 1
        stop.set()
        [w.join() for w in ws]
        env = {'MONGO_TEST_LOCALLOG_NO_REPLAY': '1'} if CONTROL else {}
        fresh = rnd % 2 == 1
        if kind == 'kill_during_replay':
            eng.start(fresh, env, wait=False)
            time.sleep(random.uniform(0.2, 1.0))
            eng.kill9()
            kills += 1
        ok = eng.start(fresh, env)
        if not ok:
            say(f'round {rnd} {kind}: ENGINE DID NOT RESTART (see engine.log)')
            total_lost = -1
            break
        if down is not None:
            cl('start', str(down))
        lost, bad_index = check(rnd)
        total_lost += lost + (1 if bad_index else 0)
        n = sum(len(v) for v in acked.values())
        say(f'round {rnd} {kind} fresh_dir={fresh} writes {dur:.1f}s acked_total={n} lost={lost} index_mismatch={bad_index} t={time.time() - t0:.0f}s')
        if CONTROL and lost > 0:
            break
    n = sum(len(v) for v in acked.values())
    if not CONTROL and total_lost == 0:
        for db in DBS:
            round_ids[db].clear()
        lost, bad_index = check(ROUNDS, full=True)
        total_lost += lost
        say(f'final full check: acked={n} lost={lost} index_mismatch={bad_index}')
        if bad_index:
            total_lost += 1
    eng.kill9()
    cl('down')
    if CONTROL:
        ok = total_lost > 0
        say(f'RESULT control: lost={total_lost} acked={n} ->', 'LOSS SEEN (the check works)' if ok else 'NO LOSS SEEN (the check is blind!)')
    else:
        ok = total_lost == 0
        say(f'RESULT engine_kills={kills} rounds={ROUNDS} acked={n} lost={total_lost} ->', 'PASS' if ok else 'FAIL')
    sys.exit(0 if ok else 1)


if __name__ == '__main__':
    main()
