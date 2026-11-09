#!/usr/bin/env python3
"""Review test R3 (guarantees 6 and 1, the warm spare's follower): a spare following a database's WAL objects
(`--agent-lane-follow`) keeps them in memory by path, and answers slatedb's reads of those paths from memory at takeover.
dropDatabase deletes every object and a recreated database writes its WAL objects again from number 1 at the SAME paths
(`tenants/{db}/wal/NNN.sst`). The follower never forgets the old ones, so at takeover the spare replays the dropped
database's objects in place of the new ones: dropped documents come back, acknowledged new ones are missing.

Primary P and spare S on the same object storage (a local directory; P is dead before S writes, so no race on it).
Expected on `agent-lane` 8fbdd8d: FAIL.
  r3_spare_cache_after_drop.py WORK_DIR [--control]
"""
from pymongo.write_concern import WriteConcern
from rcommon import *

work = fresh(os.path.abspath(sys.argv[1]))
common = ['--store', 'local', '--local-dir', os.path.join(work, 'store'), '--local-log-upload-secs', '60', '--agent-lane', '*.agent*', '--agent-lane-window-ms', '20',
          '--lease-ttl-secs', '4']
P = Srv(work, 'P', 28103, common + ['--local-log-dir', os.path.join(work, 'logP')])
# --control: the same spare without the follower (the test must PASS then)
follow = [] if '--control' in sys.argv else ['--agent-lane-follow', 'r3']
S = Srv(work, 'S', 28104, common + ['--local-log-dir', os.path.join(work, 'logS')] + follow)
P.start()
S.start()
c = P.client()
coll = lambda: c['r3'].get_collection('agent', write_concern=WriteConcern(w='majority'))
c['r3'].create_collection('agent')
for i in range(40):
    coll().insert_one({'_id': f'old{i}', 'gen': 'dropped'})
time.sleep(1.5)                     # the spare fetches the WAL objects
c.drop_database('r3')
c['r3'].create_collection('agent')
acked = []
for i in range(5):
    coll().insert_one({'_id': f'new{i}', 'gen': 'new'})
    acked.append(f'new{i}')
time.sleep(1.0)
st = S.client().admin.command('tenantStats').get('agentLane')
print('spare agentLane before the takeover:', st, flush=True)
P.kill9()
time.sleep(5)                       # P's lease (4 s) lapses
sc = S.client()
docs = retry(lambda: list(sc['r3']['agent'].find()), 120)
ids = sorted(d['_id'] for d in docs)
back = [i for i in ids if i.startswith('old')]
missing = [i for i in acked if i not in ids]
print('spare after the takeover serves:', ids, flush=True)
S.kill9()
result('r3_spare_cache_after_drop', not back and not missing, f'dropped_documents_back={len(back)} acknowledged_missing={len(missing)}')
