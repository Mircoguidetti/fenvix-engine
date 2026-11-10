#!/usr/bin/env python3
"""Review test R2 (a write visible before it is durable): an agent-lane insert that is still waiting for its PUT (window
4 s here, so the wait is easy to land in) must not be returned to a read with readConcern "majority", and no change-stream
event may announce it, because a kill -9 before the PUT loses it: the reader would have seen a write that never happened.

MongoDB: readConcern "majority" returns only data acknowledged by a majority, which can never be rolled back (DECLARED,
https://www.mongodb.com/docs/manual/reference/read-concern-majority/); change streams only report majority-committed
changes (DECLARED, https://www.mongodb.com/docs/manual/changeStreams/). With the agent-lane fix of dbs.rs (`durable_reads`
false whenever the log is on) every read sees the writer's memtable, including writes not durable yet.
Expected on `agent-lane` 8fbdd8d: FAIL (the majority read sees the ghost, the ghost is gone after the restart).
  r2_visible_before_durable.py WORK_DIR [--rounds 5] [--control]
"""
import threading
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern
from rcommon import *

work = fresh(os.path.abspath(sys.argv[1]))
ROUNDS = int(sys.argv[sys.argv.index('--rounds') + 1]) if '--rounds' in sys.argv else 5
# --control: the same lane without the log, so reads stay on the durable state (durable_reads true): the test must PASS
logargs = [] if '--control' in sys.argv else ['--local-log-dir', os.path.join(work, 'log'), '--local-log-upload-secs', '60']
args = ['--store', 'local', '--local-dir', os.path.join(work, 'store')] + logargs + [
        '--agent-lane', '*.agent*', '--agent-lane-window-ms', '4000', '--lease-ttl-secs', '4']
s = Srv(work, 'p', 28102, args)
s.start()
c = s.client()
c['r2'].create_collection('agent')
c['r2']['agent'].insert_one({'_id': 'base'})     # waits for its window: the catalog is durable too
c.close()
seen_major, seen_event, lost_seen = 0, 0, 0
for rnd in range(ROUNDS):
    gid = f'ghost{rnd}'
    w, r = s.client(), s.client()
    events = []
    stream = r['r2']['agent'].watch()

    def watch():
        try:
            for ev in stream:
                events.append(ev.get('documentKey', {}).get('_id'))
        except Exception:
            pass
    tw = threading.Thread(target=watch, daemon=True); tw.start()
    time.sleep(0.5)

    def write():
        try:
            w['r2'].get_collection('agent', write_concern=WriteConcern(w='majority')).insert_one({'_id': gid})
        except Exception:
            pass
    t = threading.Thread(target=write, daemon=True); t.start()
    time.sleep(1.0)            # the insert is staged, its window (4 s) is still open: not durable, not acknowledged
    maj = r['r2'].get_collection('agent', read_concern=ReadConcern('majority')).find_one({'_id': gid}) is not None
    time.sleep(0.3)
    ev = gid in events
    s.kill9()                  # before the window ends: the PUT never happens
    seen_major += maj
    seen_event += ev
    s.start()
    c = s.client()
    survived = retry(lambda: c['r2']['agent'].find_one({'_id': gid})) is not None
    c.close()
    if (maj or ev) and not survived:
        lost_seen += 1
    print(f'round {rnd}: majority read saw the unacknowledged write: {maj}; change stream announced it: {ev}; it survived the kill: {survived}', flush=True)
s.kill9()
result('r2_visible_before_durable', lost_seen == 0, f'rounds={ROUNDS} majority_reads_of_unacked={seen_major} events_of_unacked={seen_event} seen_then_lost={lost_seen}')
