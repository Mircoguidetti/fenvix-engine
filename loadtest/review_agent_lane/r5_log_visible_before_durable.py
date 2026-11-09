#!/usr/bin/env python3
"""Review test R5 (the Q8d fix alone, agent lane not used): with the replicated log on, a default-path write is staged in
the writer's memtable and acknowledged once 2 of the 3 log copies have it. While the log cannot confirm it (here the 3
copies are frozen with SIGSTOP), a read with readConcern "majority" must not return it: if the primary dies then, the write
was never acknowledged and is gone. With the fix of dbs.rs (`durable_reads` false whenever the log is on) the read sees it.
Expected on `agent-lane` 8fbdd8d: FAIL. On `integration` d6ce3d4 reads see only object storage (no dirty read; but R1 fails).
  r5_log_visible_before_durable.py OUT_DIR [--rounds 3]
"""
import threading
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern
import rcommon  # puts loadtest/agent_lane on the path
rcommon.no_lane_flag_on_base()
from common import *

args = sys.argv[1:]
OUT = Out(args[0], 'r5_log_visible_before_durable')
ROUNDS = int(opt(args, '--rounds', '3'))
WORK = os.path.join(OUT.dir, 'work')
kill_leftovers((28107,), 9000)
shutil.rmtree(WORK, ignore_errors=True)
os.makedirs(WORK)
log = Log(os.path.join(WORK, 'log'), 9000)
log.up()
P = Engine(OUT.dir, 'P', 28107, ['--store', 'local', '--local-dir', os.path.join(WORK, 'store')], log=log, upload_secs=30)
assert P.start(True), 'primary did not start'
c = P.client()
c['r5']['fast'].insert_one({'_id': 'base'})
c.close()
lost_seen = seen = 0
for rnd in range(ROUNDS):
    gid = f'ghost{rnd}'
    w, r = P.client(socketTimeoutMS=60000), P.client()
    for i in range(3):
        log.cl('sigstop', str(i))

    def write():
        try:
            w['r5'].get_collection('fast', write_concern=WriteConcern(w='majority')).insert_one({'_id': gid})
        except Exception:
            pass
    t = threading.Thread(target=write, daemon=True); t.start()
    time.sleep(1.0)
    maj = r['r5'].get_collection('fast', read_concern=ReadConcern('majority')).find_one({'_id': gid}) is not None
    P.kill9()
    for i in range(3):
        log.cl('sigcont', str(i))
    assert P.start(False), 'primary did not restart'
    c = P.client()
    survived = retry(lambda: c['r5']['fast'].find_one({'_id': gid})) is not None
    c.close()
    seen += maj
    lost_seen += maj and not survived
    OUT.say(f'round {rnd}: majority read saw the write the log never confirmed: {maj}; it survived the kill: {survived}')
P.kill9()
log.down()
ok = lost_seen == 0
OUT.say(f'RESULT r5_log_visible_before_durable rounds={ROUNDS} majority_reads_of_unacked={seen} seen_then_lost={lost_seen} ->', 'PASS' if ok else 'FAIL')
sys.exit(0 if ok else 1)
