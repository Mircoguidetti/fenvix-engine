#!/usr/bin/env python3
"""Review test R4 (guarantees 1 and 3 on the warm spare, the Q8d log path, lane or not): writes on the default path are
acknowledged once 2 of the 3 log copies have them; they reach object storage only at the next upload. A warm spare that
takes the database over after the primary's death must serve them. The agent-lane test t3 hides this by sleeping one
upload interval before the kill ("the default-path writes reach object storage before the kill"); this test kills at once.

The same set-up as t3 (common.py: 3 log copies, two engines on the same object storage, lease 4 s), upload every 30 s.
Expected on `agent-lane` 8fbdd8d: FAIL (the report's open item "a live takeover does not read the log's records").
  r4_spare_takeover_log_records.py OUT_DIR [--writes 200]
"""
from pymongo.write_concern import WriteConcern
from pymongo.errors import PyMongoError
import rcommon  # puts loadtest/agent_lane on the path
rcommon.no_lane_flag_on_base()
from common import *

args = sys.argv[1:]
OUT = Out(args[0], 'r4_spare_takeover_log_records')
NW = int(opt(args, '--writes', '200'))
WORK = os.path.join(OUT.dir, 'work')
kill_leftovers((28105, 28106), 8000)
shutil.rmtree(WORK, ignore_errors=True)
os.makedirs(WORK)
log = Log(os.path.join(WORK, 'log'), 8000)
log.up()
st = ['--store', 'local', '--local-dir', os.path.join(WORK, 'store')]
P = Engine(OUT.dir, 'P', 28105, st, log=log, upload_secs=30)
S = Engine(OUT.dir, 'S', 28106, st, log=log, upload_secs=30)
assert P.start(True), 'primary did not start'
c = P.client()
c['r4'].create_collection('fast')
time.sleep(35)          # as t3: the catalog reaches object storage before the spare starts
assert S.start(True), 'spare did not start'
coll = c['r4'].get_collection('fast', write_concern=WriteConcern(w='majority'))
acked = []
for i in range(NW):
    coll.insert_one({'_id': i})
    acked.append(i)
P.kill9()
time.sleep(5)
sc = S.client(socketTimeoutMS=120000)
found = find_ids(sc, 'r4', 'fast', acked)
missing = len(acked) - len(found)
OUT.say(f'{len(acked)} acknowledged default-path writes, the spare serves {len(found)} after the takeover')
S.kill9()
log.down()
ok = missing == 0
OUT.say(f'RESULT r4_spare_takeover_log_records acked={len(acked)} missing_on_spare={missing} ->', 'PASS' if ok else 'FAIL')
sys.exit(0 if ok else 1)
