#!/usr/bin/env python3
"""Review test R1 (guarantee 3, the Q8d fix): with the local log on and the agent lane OFF, every read path sees an
acknowledged write at once: point read by _id, read through a secondary index, range read, count, aggregate, the value
after an update, the absence after a delete, a transaction's writes after its commit, and a read inside a transaction.

Expected: FAIL on `integration` d6ce3d4 (reads under the shared lock see only what object storage has, and with the log
on a write reaches object storage only at the next upload), PASS on `agent-lane` (fix in dbs.rs `durable_reads`).
  r1_log_read_your_writes.py WORK_DIR [--n 100]
"""
import sys
from rcommon import *

work = fresh(os.path.abspath(sys.argv[1]))
N = int(sys.argv[sys.argv.index('--n') + 1]) if '--n' in sys.argv else 100
s = Srv(work, 'p', 28101, ['--store', 'local', '--local-dir', os.path.join(work, 'store'), '--local-log-dir', os.path.join(work, 'log'),
                           '--local-log-upload-secs', '60'])
s.start()
w, r = s.client(), s.client()
db, rdb = w['r1'], r['r1']
db['c'].insert_one({'_id': -1, 'k': -1})
db['c'].create_index('k')
time.sleep(1)
miss = {k: 0 for k in ['point', 'index', 'range', 'count', 'aggregate', 'update', 'delete', 'txn', 'in_txn']}
for i in range(N):
    db['c'].insert_one({'_id': i, 'k': i, 'v': 'a'})
    if rdb['c'].find_one({'_id': i}) is None: miss['point'] += 1
    if rdb['c'].find_one({'k': i}) is None: miss['index'] += 1
    if not list(rdb['c'].find({'_id': {'$gte': i, '$lte': i}})): miss['range'] += 1
    if rdb['c'].count_documents({'_id': i}) != 1: miss['count'] += 1
    if not list(rdb['c'].aggregate([{'$match': {'_id': i}}])): miss['aggregate'] += 1
    db['c'].update_one({'_id': i}, {'$set': {'v': 'b'}})
    d = rdb['c'].find_one({'_id': i})
    if d is None or d.get('v') != 'b': miss['update'] += 1
    db['c'].delete_one({'_id': i})
    if rdb['c'].find_one({'_id': i}) is not None: miss['delete'] += 1
    with w.start_session() as ss:
        with ss.start_transaction():
            db['c'].insert_one({'_id': f't{i}', 'k': f't{i}'}, session=ss)
    if rdb['c'].find_one({'_id': f't{i}'}) is None: miss['txn'] += 1
    db['c'].insert_one({'_id': f'x{i}'})
    with r.start_session() as rs:
        with rs.start_transaction():
            if rdb['c'].find_one({'_id': f'x{i}'}, session=rs) is None: miss['in_txn'] += 1
s.kill9()
total = sum(miss.values())
result('r1_log_read_your_writes', total == 0, f'n={N} misses={miss}')
