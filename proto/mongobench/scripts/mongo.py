#!/usr/bin/env python3
"""M16: accende mongod 8.0 sulla stessa macchina del quaderno.

  mongo.py rs3     replica set di 3 membri (porte 27017-27019): primario sul core 0, secondari sul core 1
  mongo.py single  un nodo solo senza replica (porta 27017) sul core 0
  mongo.py down

Ogni membro: cache WiredTiger 2 GB (MB_CACHE_GB; con 1 GB lo sfratto delle pagine sporche costava a mongod 0,6 core), journal acceso (predefinito), dati in MB_WORK (stesso disco del quaderno).
"""
import os, subprocess, sys, time, shutil
from pymongo import MongoClient
MONGOD = os.environ.get('MONGOD', '/opt/mongo8/bin/mongod')
WORK = os.environ.get('MB_WORK', '/root/mb-work')
CACHE = os.environ.get('MB_CACHE_GB', '2')
# MB_PIN=0: nessun core fisso (il sistema sceglie), per il tetto di mongod su tutta la macchina
PIN = os.environ.get('MB_PIN', '1') == '1'

def down():
    subprocess.run(['pkill', '-x', 'mongod'])
    for _ in range(60):
        if subprocess.run(['pgrep', '-x', 'mongod'], capture_output=True).returncode != 0:
            return
        time.sleep(0.5)
    subprocess.run(['pkill', '-9', '-x', 'mongod'])

def start(port, core, rs):
    d = os.path.join(WORK, str(port))
    os.makedirs(d, exist_ok=True)
    cmd = ['taskset', '-c', str(core) if PIN else '0-%d' % (os.cpu_count() - 1), MONGOD, '--port', str(port), '--bind_ip', '127.0.0.1', '--dbpath', d, '--logpath', d + '.log',
           '--fork', '--wiredTigerCacheSizeGB', CACHE]
    if rs:
        cmd += ['--replSet', 'rs0']
    subprocess.run(cmd, check=True, capture_output=True)

def pin_all(port_core):
    if not PIN:
        return
    # i thread nati dopo il fork ereditano il core; ripinna tutto per sicurezza
    out = subprocess.run(['pgrep', '-a', '-x', 'mongod'], capture_output=True, text=True).stdout
    for l in out.splitlines():
        pid, rest = l.split(' ', 1)
        for p, c in port_core.items():
            if f'--port {p} ' in rest + ' ':
                subprocess.run(['taskset', '-a', '-p', '-c', str(c), pid], capture_output=True)

cmd = sys.argv[1]
down()
if cmd == 'down':
    sys.exit(0)
shutil.rmtree(WORK, ignore_errors=True)
if cmd == 'single':
    start(27017, 0, False)
    pin_all({27017: 0})
    print('mongodb://127.0.0.1:27017')
elif cmd == 'rs3':
    pc = {27017: 0, 27018: 1, 27019: 1}
    for p, c in pc.items():
        start(p, c, True)
    c = MongoClient('mongodb://127.0.0.1:27017', directConnection=True)
    c.admin.command('replSetInitiate', {'_id': 'rs0', 'members': [
        {'_id': 0, 'host': '127.0.0.1:27017', 'priority': 2},
        {'_id': 1, 'host': '127.0.0.1:27018', 'priority': 1},
        {'_id': 2, 'host': '127.0.0.1:27019', 'priority': 1}]})
    for _ in range(120):
        s = c.admin.command('replSetGetStatus')
        st = sorted((m['name'], m['stateStr']) for m in s['members'])
        if [x[1] for x in st].count('PRIMARY') == 1 and dict(st).get('127.0.0.1:27017') == 'PRIMARY' and [x[1] for x in st].count('SECONDARY') == 2:
            break
        time.sleep(1)
    pin_all(pc)
    print('mongodb://127.0.0.1:27017,127.0.0.1:27018,127.0.0.1:27019/?replicaSet=rs0')
