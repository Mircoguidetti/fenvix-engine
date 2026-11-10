#!/usr/bin/env python3
"""Agent lane smoke test: writes to an agent-lane collection go on the lane, other writes on the log; mixed transactions on
the log; the option through collMod. Prints serverStatus().fenvix agentLane counters.   smoke.py OUT_DIR"""
import sys, time
from common import *

OUT = Out(sys.argv[1], 'smoke')
work = os.path.join(OUT.dir, 'work')
shutil.rmtree(work, ignore_errors=True)
log = Log(os.path.join(work, 'log'), 3100)
log.up()
e = Engine(OUT.dir, 'a', 27951, ['--store', 'local', '--local-dir', os.path.join(work, 'store')], log=log, env={'FENVIX_SIM_S3_PUT': '30,0.3,90', 'FENVIX_DEBUG_LANE': '1'})
assert e.start(True), 'engine did not start'
c = e.client()
db = c['smoke']
db['fast'].insert_one({'_id': 0})
db['agent_ck'].insert_one({'_id': 0})
st = lambda: lane_stats(c)
OUT.say('after creates', st())
t = time.time(); db['fast'].insert_one({'_id': 1}); OUT.say(f'fast insert {1e3 * (time.time() - t):.1f} ms')
for i in range(1, 4):
    t = time.time(); db['agent_ck'].insert_one({'_id': i * 100}); OUT.say(f'agent insert {1e3 * (time.time() - t):.1f} ms', st())
with c.start_session() as s:
    t = time.time()
    with s.start_transaction():
        db['agent_ck'].insert_one({'_id': 2}, session=s)
        db['agent_ck'].insert_one({'_id': 3}, session=s)
    OUT.say(f'agent txn {1e3 * (time.time() - t):.1f} ms')
    t = time.time()
    with s.start_transaction():
        db['agent_ck'].insert_one({'_id': 4}, session=s)
        db['fast'].insert_one({'_id': 4}, session=s)
    OUT.say(f'mixed txn {1e3 * (time.time() - t):.1f} ms')
db.create_collection('other')
db.command('collMod', 'other', fenvixAgentLane=True)
t = time.time(); db['other'].insert_one({'_id': 1}); OUT.say(f'collMod lane insert {1e3 * (time.time() - t):.1f} ms')
OUT.say('listCollections other:', [x['options'] for x in db.list_collections(filter={'name': 'other'})])
OUT.say('final', st())
c.close()
e.kill9()
assert e.start(False)
c = e.client()
OUT.say('after kill -9 and restart:', retry(lambda: sorted(d['_id'] for d in c['smoke']['agent_ck'].find())), retry(lambda: sorted(d['_id'] for d in c['smoke']['fast'].find())), retry(lambda: list(c['smoke']['other'].find())))
e.kill9()
log.down()
