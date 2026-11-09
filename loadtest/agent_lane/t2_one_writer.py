#!/usr/bin/env python3
"""Agent lane, done-means 2: one writer. Two primaries race on the same database with a forced takeover, 50 times:
0 writes confirmed by the fenced one, 0 gaps or duplicates in the sequence of WAL objects.

Object storage is a local MinIO: real S3 conditional writes (`If-None-Match: *`) between two processes. Both engines
(A and B) share the bucket and the replicated log. Each trial uses a new database whose agent-lane collection is created
first (and uploaded), then only agent-lane writes, each remembered with which engine acknowledged it and when.
  trials 0-24 "frozen primary": writers on A; A is frozen (SIGSTOP), half of the times in the middle of a WAL PUT; writers
     on B, which takes the database over when A's lease lapses (4 s) and fences A's WAL; once B acknowledged 20 writes,
     A is woken (SIGCONT) and its writers go on for 1.5 s more.
  trials 25-49 "two live primaries": both engines run without leases (`--no-leases`) and without the replicated log (its
     ownership guard refuses a database when leases are off), and writers send to both at the same time for 3 s: a write
     on one engine opens the database as writer there and fences the other. Writers start on A, then on B (B fences A,
     maybe in the middle of A's batch window), then A is killed and started again (A fences B the same way). Only slatedb's
     WAL fencing keeps one writer.
Checks: every acknowledged write (from A or B) is in the database at the end (read on B and on A after a fresh start);
no acknowledgement from a primary after it was fenced, i.e. from A after it was woken for a write that is not in the
database, and no WAL object written by A after its wake at a number at or above B's first (B's fence) (PUTLOG lines; a
PUT that reached the store before the freeze and was answered after the wake is below the fence, counted apart); the WAL numbers written (PUTLOG `ok` lines of both
engines, fences included) are 1..N, each exactly once.
Negative control (`--control`, frozen trials only): A acknowledges agent-lane writes before their PUT; the check must see
acknowledged writes missing.
  t2_one_writer.py OUT_DIR [--trials 50] [--seed 1] [--control]
"""
from pymongo import WriteConcern
from pymongo.errors import PyMongoError
from common import *

args = sys.argv[1:]
OUT = Out(args[0], 't2_one_writer')
TRIALS = int(opt(args, '--trials', '50'))
CONTROL = '--control' in args   # negative control: A acknowledges agent-lane writes before their PUT; the check must see it
random.seed(int(opt(args, '--seed', '1')))
WORK = os.path.join(OUT.dir, 'work')
BUCKET = 'race'
FA, FB = os.path.join(OUT.dir, 'faults-a'), os.path.join(OUT.dir, 'faults-b')
WAL_RE = re.compile(r'PUTLOG (ok|exists|error) tenants/([^/]+)/wal/(\d+)\.sst')


def writer(eng, tag, db, stop, acks, lock, n0):
    c = eng.client(socketTimeoutMS=60000)
    i = 0
    while not stop.is_set():
        _id = f'{tag}-{n0}-{i}'
        i += 1
        try:
            c[db].get_collection('agent', write_concern=WriteConcern(w='majority')).insert_one({'_id': _id, 'pad': 'z' * 200})
            with lock:
                acks.append((_id, tag, time.time()))
        except PyMongoError:
            time.sleep(0.02)
    c.close()


def wal_events(eng, off, db):
    out = []
    for m in WAL_RE.finditer(eng.log_since(off)):
        if m.group(2) == db:
            out.append((m.group(1), int(m.group(3))))
    return out


def main():
    kill_leftovers((27971, 27972, 27973), 5000, 9100)
    shutil.rmtree(WORK, ignore_errors=True)
    os.makedirs(WORK)
    for f in (FA, FB):
        open(f, 'w').write('log_put /wal/\ndelay_put /wal/ 40\n')
    minio = Minio(WORK)
    minio.up(BUCKET)
    log = Log(os.path.join(WORK, 'log'), 5000)
    log.up()
    st = minio.args(BUCKET)
    totals = dict(acked=0, lost=0, fenced_acks=0, fenced_puts=0, gaps=0, dups=0, a_after_wake=0)
    t0 = time.time()
    for trial in range(TRIALS):
        live = trial >= TRIALS // 2 and not CONTROL
        extra = ['--no-leases', '--process-lease', 'false'] if live else []
        lg = None if live else log
        ea = dict(Minio.env(), AWS_ALLOW_HTTP='true', **({'MONGO_TEST_AGENT_ACK_EARLY': '1'} if CONTROL else {}))
        A = Engine(OUT.dir, 'A', 27971, st, log=lg, env=ea, faults=FA, upload_secs=1, extra=extra)
        B = Engine(OUT.dir, 'B', 27972, st, log=lg, env=dict(Minio.env(), AWS_ALLOW_HTTP='true'), faults=FB, upload_secs=1, extra=extra)
        offa, offb = A.log_size(), B.log_size()
        assert A.start(True) and B.start(True), 'engines did not start'
        db = f'race_{trial}'
        c = A.client()
        c[db].create_collection('agent')
        c[db]['agent'].insert_one({'_id': 'seed'})
        c.close()
        time.sleep(2.5)                                  # the catalog record reaches object storage (upload every 1 s)
        acks, lock = [], threading.Lock()
        stop_a, stop_b = threading.Event(), threading.Event()
        wa = [threading.Thread(target=writer, args=(A, 'A', db, stop_a, acks, lock, k)) for k in range(4)]
        wb = [threading.Thread(target=writer, args=(B, 'B', db, stop_b, acks, lock, k)) for k in range(4)]
        wake = None
        if not live:
            [w.start() for w in wa]
            time.sleep(random.uniform(0.5, 1.5))
            mid_put = trial % 2 == 0
            if mid_put:
                A.wait_line(A.log_size(), 'FAULT delay_put', 10)
                time.sleep(random.uniform(0, 0.03))
            A.sigstop()
            frozen = time.time()
            [w.start() for w in wb]
            end = time.time() + 30
            while time.time() < end and sum(1 for a in acks if a[1] == 'B') < 20:
                time.sleep(0.05)
            wake_off = A.log_size()
            A.sigcont()
            wake = time.time()
            time.sleep(1.5)
        else:
            mid_put = False
            wake_off = None
            [w.start() for w in wa]
            time.sleep(random.uniform(0.2, 1.0))
            [w.start() for w in wb]                      # B's first write fences A, maybe in the middle of A's batch
            time.sleep(random.uniform(0.5, 1.0))
            A.kill9()                                    # A comes back: its next write fences B, maybe mid-batch
            A.start(False)
            time.sleep(1.5)
        stop_a.set()
        stop_b.set()
        [w.join() for w in wa + wb]
        A.stop()
        B.stop()
        # read everything back on a fresh engine (no state of A or B in memory)
        R = Engine(OUT.dir, 'R', 27973, st, log=lg, env=dict(Minio.env(), AWS_ALLOW_HTTP='true'), upload_secs=1)
        assert R.start(True)
        rc = R.client()
        ids = [a[0] for a in acks]
        found = find_ids(rc, db, 'agent', ids)
        rc.close()
        R.stop()
        lost = [a for a in acks if a[0] not in found]
        a_after = [a for a in acks if a[1] == 'A' and wake is not None and a[2] > wake]
        fenced_acks = [a for a in a_after if a[0] not in found]
        ev = wal_events(A, offa, db) + wal_events(B, offb, db)
        okids = sorted(i for (k, i) in ev if k == 'ok')
        dups = len(okids) - len(set(okids))
        gaps = (okids[-1] - okids[0] + 1 - len(set(okids))) if okids else 0
        # A WAL object A wrote after its wake is a fenced write only at a number at or above B's first one (B's fence):
        # below it, the PUT reached the store before the freeze and only its answer came after (B replayed it).
        fenced_puts = late_ok = 0
        if wake_off is not None:
            b_ids = [i for (k, i) in wal_events(B, offb, db)]
            b_first = min(b_ids) if b_ids else None
            for (k, i) in wal_events(A, wake_off, db):
                if k == 'ok':
                    if b_first is not None and i >= b_first:
                        fenced_puts += 1
                    else:
                        late_ok += 1
        totals['acked'] += len(acks)
        totals['lost'] += len(lost)
        totals['fenced_acks'] += len(fenced_acks)
        totals['fenced_puts'] += fenced_puts
        totals['gaps'] += gaps
        totals['dups'] += dups
        totals['a_after_wake'] += len(a_after)
        totals['late_answers'] = totals.get('late_answers', 0) + late_ok
        na = sum(1 for a in acks if a[1] == 'A')
        exists = sum(1 for (k, i) in ev if k == 'exists')
        OUT.say(f'trial {trial} {"two-live" if live else "frozen"} mid_put={mid_put} acks A={na} B={len(acks) - na} lost={len(lost)} '
                f'A_acks_after_wake={len(a_after)} fenced_acks={len(fenced_acks)} A_wal_puts_at_or_after_fence={fenced_puts} A_puts_answered_after_wake_below_fence={late_ok} '
                f'wal_ids={okids[0] if okids else "-"}..{okids[-1] if okids else "-"} ({len(okids)} written) gaps={gaps} dups={dups} '
                f'create_refused(exists)={exists} t={time.time() - t0:.0f}s')
    log.down()
    minio.down()
    ok = totals['lost'] == 0 and totals['fenced_acks'] == 0 and totals['fenced_puts'] == 0 and totals['gaps'] == 0 and totals['dups'] == 0
    if CONTROL:
        seen = totals['lost'] > 0
        OUT.say(f'RESULT control trials={TRIALS} {totals} ->', 'LOSS SEEN (the check works)' if seen else 'NO LOSS SEEN (the check is blind!)')
        sys.exit(0 if seen else 1)
    OUT.say(f'RESULT trials={TRIALS} {totals} ->', 'PASS' if ok else 'FAIL')
    sys.exit(0 if ok else 1)


if __name__ == '__main__':
    main()
