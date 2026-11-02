#!/usr/bin/env python3
"""Prova M8 "senza cuoco fisso": due mongo-server servono lo stesso database sopra proto-quaderno.

Ogni scrittura va al quaderno come commit validato (mongo-server/src/qmode.rs); nessun processo possiede il database.

  prova.py tutto            tutte le prove, risultati in results/
  prova.py <nome> [...]     solo alcune: fumo stesso_id stessa_chiave inc cambia_chiave morte_server morte_capo senza_guardia

Porte: quaderno 7200-7202 (repliche), mongo-server A 27101, B 27102. Cartella di lavoro: $SC_WORK (default /root/sc-work).
"""
import json, os, random, signal, socket, subprocess, sys, threading, time, uuid
from collections import defaultdict

import pymongo
from pymongo.errors import DuplicateKeyError, OperationFailure, AutoReconnect, ConnectionFailure, NetworkTimeout, ServerSelectionTimeoutError

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, '..', '..'))
MS = os.path.join(ROOT, 'target', 'release', 'mongo-server')
CLUSTER = os.path.join(ROOT, 'proto', 'quaderno', 'scripts', 'cluster.py')
WORK = os.environ.get('SC_WORK', '/root/sc-work')
RES = os.path.join(HERE, 'results')
PORTS = {'A': 27101, 'B': 27102}
QADDR = '127.0.0.1:7200,127.0.0.1:7201,127.0.0.1:7202'
DB = 'cucina'
procs = {}


def sh(args, **kw):
    return subprocess.run(args, capture_output=True, text=True, env=dict(os.environ, PQ_WORK=os.path.join(WORK, 'pq')), **kw)


def wait_port(port, t=20):
    end = time.time() + t
    while time.time() < end:
        try:
            socket.create_connection(('127.0.0.1', port), 0.2).close()
            return True
        except Exception:
            time.sleep(0.05)
    return False


def quaderno_up():
    os.makedirs(os.path.join(WORK, 'pq'), exist_ok=True)
    r = sh([sys.executable, CLUSTER, 'up', '--fresh', '--no-proxy', '--mem-cap-mb', '1024'])
    print('quaderno:', r.stdout.strip(), r.stderr.strip()[-300:])


def quaderno_down():
    sh([sys.executable, CLUSTER, 'down'])


def capo():
    return int(sh([sys.executable, CLUSTER, 'leader']).stdout.strip() or -1)


def server_up(name, env_extra=None):
    d = os.path.join(WORK, f'ms-{name}')
    os.makedirs(d, exist_ok=True)
    log = open(os.path.join(WORK, f'ms-{name}.log'), 'ab')
    env = dict(os.environ)
    env.update(env_extra or {})
    args = [MS, '--listen', f'127.0.0.1:{PORTS[name]}', '--store', 'local', '--local-dir', d, '--no-leases', '--no-ttl',
            '--no-maintenance', '--holder', f'sc-{name}', '--process-lease', 'false', '--quaderno', QADDR]
    procs[name] = subprocess.Popen(args, stdout=log, stderr=log, env=env, start_new_session=True)
    assert wait_port(PORTS[name]), f'mongo-server {name} non parte'


def server_kill(name):
    p = procs.get(name)
    if p and p.poll() is None:
        p.send_signal(signal.SIGKILL)
        p.wait()


def servers_down():
    for n in list(procs):
        server_kill(n)


def client(name, timeout_ms=15000):
    return pymongo.MongoClient(f'mongodb://127.0.0.1:{PORTS[name]}/?directConnection=true', serverSelectionTimeoutMS=3000,
                               socketTimeoutMS=timeout_ms, connectTimeoutMS=2000, retryWrites=False, retryReads=False, maxPoolSize=64)


def esito(e):
    """ok | dup (sicuramente non applicata) | no (sicuramente non applicata) | ignoto"""
    if isinstance(e, DuplicateKeyError):
        return 'dup'
    if isinstance(e, OperationFailure):
        if e.code in (112, 91):  # conflitto dopo tutti i tentativi, freno: non è entrata
            return 'no'
        if e.code == 11000:
            return 'dup'
        return 'ignoto'
    return 'ignoto'


class Carico:
    """Lavoratori su A e B che girano `op` finché `stop` non è alzato. Registra ogni operazione con il suo esito."""

    def __init__(self, op, workers=8, servers=('A', 'B')):
        self.op, self.workers, self.servers = op, workers, servers
        self.log = []  # (t, server, chiave, valore, esito, codice)
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.ths = []

    def run_worker(self, srv, wi):
        c = client(srv)
        coll = c[DB]['prova']
        seq = 0
        rnd = random.Random(hash((srv, wi, time.time())))
        while not self.stop.is_set():
            seq += 1
            tag = f'{srv}-{wi}-{seq}'
            key, val = None, None
            t0 = time.time()
            try:
                key, val = self.op(coll, rnd, tag)
                res, code = 'ok', 0
            except Exception as e:
                key, val = getattr(e, '_key', key), getattr(e, '_val', val)
                res, code = esito(e), getattr(e, 'code', None) or type(e).__name__
                if res == 'ignoto':
                    time.sleep(0.05)
            with self.lock:
                self.log.append((t0, time.time(), srv, key, val, res, code))
        c.close()

    def start(self):
        for s in self.servers:
            for w in range(self.workers):
                t = threading.Thread(target=self.run_worker, args=(s, w), daemon=True)
                t.start()
                self.ths.append(t)

    def finish(self):
        self.stop.set()
        for t in self.ths:
            t.join(30)


def tagged(fn):
    """Avvolge un'operazione: se fallisce, l'eccezione porta chiave e valore."""
    def w(coll, rnd, tag):
        key, val, call = fn(coll, rnd, tag)
        try:
            call()
        except Exception as e:
            e._key, e._val = key, val
            raise
        return key, val
    return w


def leggi_tutto(name):
    c = client(name, 60000)
    docs = list(c[DB]['prova'].find({}, batch_size=1_000_000))
    c.close()
    return docs


def conta(log):
    r = defaultdict(int)
    for x in log:
        r[x[5]] += 1
    return dict(r)


def per_secondo(log, t0, t1, srv=None):
    """operazioni confermate per secondo, secondo per secondo"""
    n = int(t1 - t0) + 1
    v = [0] * n
    for x in log:
        if x[5] in ('ok', 'dup') and (srv is None or x[2] == srv):
            i = int(x[1] - t0)
            if 0 <= i < n:
                v[i] += 1
    return v


def buco_massimo(log, t_from, srv=None):
    """il tempo più lungo, dopo t_from, tra due operazioni confermate (ok o dup) consecutive"""
    ts = sorted(x[1] for x in log if x[5] in ('ok', 'dup') and x[1] >= t_from and (srv is None or x[2] == srv))
    if len(ts) < 2:
        return None
    return max(b - a for a, b in zip(ts, ts[1:]))


def nuova_prova(nome, unique=True):
    """Riparte da zero: quaderno nuovo, due server nuovi, indice unico su email creato da A."""
    servers_down()
    quaderno_down()
    quaderno_up()
    server_up('A')
    server_up('B')
    c = client('A')
    if unique:
        c[DB]['prova'].create_index('email', unique=True)
    c.close()


def stats(name):
    try:
        c = client(name, 5000)
        s = c[DB].command('qstats')
        c.close()
        return s
    except Exception as e:
        return str(e)


# ---------------------------------------------------------------------------------------------------- le prove

def controlla_inserimenti(log, docs, campo):
    """Per ogni valore di `campo` (_id o email): al massimo una conferma; il documento c'è se c'è la conferma ed è
    quello confermato; mai due documenti con lo stesso valore."""
    errori = []
    conferme = defaultdict(list)
    ignoti = defaultdict(list)
    for (_, _, srv, key, val, res, code) in log:
        if key is None:
            continue
        if res == 'ok':
            conferme[key].append(val)
        elif res == 'ignoto':
            ignoti[key].append(val)
    per_valore = defaultdict(list)
    for d in docs:
        per_valore[d.get(campo)].append(d)
    doppioni = {k: len(v) for k, v in per_valore.items() if len(v) > 1}
    if doppioni:
        errori.append(f'{len(doppioni)} valori di {campo} con più documenti (es. {list(doppioni.items())[:3]})')
    doppie_conferme = {k: v for k, v in conferme.items() if len(v) > 1}
    if doppie_conferme:
        errori.append(f'{len(doppie_conferme)} valori di {campo} confermati due volte (es. {list(doppie_conferme.items())[:3]})')
    perse = [k for k, v in conferme.items() if not per_valore.get(k)]
    if perse:
        errori.append(f'{len(perse)} inserimenti confermati e assenti (es. {perse[:3]})')
    sbagliati = []
    for k, v in conferme.items():
        ds = per_valore.get(k) or []
        if ds and ds[0].get('w') not in v:
            sbagliati.append((k, v, ds[0].get('w')))
    if sbagliati:
        errori.append(f'{len(sbagliati)} documenti scritti da un altro (la conferma è stata sovrascritta), es. {sbagliati[:3]}')
    venuti_dal_nulla = []
    for k, ds in per_valore.items():
        for d in ds:
            if d.get('w') not in conferme.get(k, []) and d.get('w') not in ignoti.get(k, []):
                venuti_dal_nulla.append((k, d.get('w')))
    if venuti_dal_nulla:
        errori.append(f'{len(venuti_dal_nulla)} documenti senza conferma né esito ignoto, es. {venuti_dal_nulla[:3]}')
    return errori, {'valori_confermati': len(conferme), 'documenti': len(docs), 'valori_con_ignoti': len(ignoti)}


def conteso(rnd, per_slot=4, slot_ms=20):
    """Chiave contesa che scorre nel tempo: tutti i lavoratori (di A e di B) mirano agli stessi pochi valori della
    finestra di tempo corrente, così la gara c'è per tutta la prova e non solo all'inizio."""
    return int(time.time() * 1000 / slot_ms) * per_slot + rnd.randrange(per_slot)


def op_stesso_id(K):
    @tagged
    def op(coll, rnd, tag):
        k = conteso(rnd, K)
        return k, tag, lambda: coll.insert_one({'_id': k, 'w': tag, 'email': f'{tag}@x'})
    return op


def op_stessa_chiave(K):
    @tagged
    def op(coll, rnd, tag):
        e = f'u{conteso(rnd, K)}@x'
        return e, tag, lambda: coll.insert_one({'_id': tag, 'w': tag, 'email': e})
    return op


def op_inc(C):
    @tagged
    def op(coll, rnd, tag):
        k = f'cont{rnd.randrange(C)}'
        def call():
            r = coll.update_one({'_id': k}, {'$inc': {'n': 1}})
            if r.matched_count != 1:
                raise RuntimeError(f'contatore {k} non trovato')
        return k, tag, call
    return op


def controlla_inc(log, docs, C):
    errori = []
    ok = defaultdict(int)
    ign = defaultdict(int)
    for x in log:
        if x[5] == 'ok':
            ok[x[3]] += 1
        elif x[5] == 'ignoto':
            ign[x[3]] += 1
    val = {d['_id']: d.get('n', 0) for d in docs if str(d['_id']).startswith('cont')}
    for i in range(C):
        k = f'cont{i}'
        n = val.get(k)
        if n is None:
            errori.append(f'{k} sparito')
            continue
        if n < ok[k]:
            errori.append(f'{k}: {n} ma {ok[k]} incrementi confermati (persi {ok[k] - n})')
        if n > ok[k] + ign[k]:
            errori.append(f'{k}: {n} ma solo {ok[k]} confermati e {ign[k]} ignoti (doppiati {n - ok[k] - ign[k]})')
    return errori, {'incrementi_confermati': sum(ok.values()), 'ignoti': sum(ign.values()), 'somma_finale': sum(val.values())}


def prepara_contatori(C):
    c = client('A')
    c[DB]['prova'].insert_many([{'_id': f'cont{i}', 'n': 0, 'w': 'prep', 'email': f'cont{i}@x'} for i in range(C)])
    c.close()


def indice_coerente(name, docs):
    """Ogni documento si trova dal suo email attraverso l'indice, e l'indice non dà documenti con un altro email."""
    c = client(name, 60000)
    coll = c[DB]['prova']
    errori = []
    emails = [d.get('email') for d in docs]
    for e in random.sample(emails, min(400, len(emails))):
        got = list(coll.find({'email': e}).hint('email_1'))
        if len(got) != 1 or got[0].get('email') != e:
            errori.append((e, [g.get('_id') for g in got]))
    c.close()
    return [f'indice incoerente su {len(errori)} valori, es. {errori[:3]}'] if errori else []


def corsa(nome, op, secs, workers, evento=None, prima=None):
    if prima:
        prima()
    car = Carico(op, workers)
    t0 = time.time()
    car.start()
    info = {}
    if evento:
        info = evento(t0, car) or {}
    while time.time() < t0 + secs:
        time.sleep(0.2)
    car.finish()
    t1 = time.time()
    return car, t0, t1, info


def prova_fumo():
    nuova_prova('fumo')
    a, b = client('A'), client('B')
    a[DB]['prova'].insert_one({'_id': 1, 'email': 'a@x', 'n': 1})
    d = b[DB]['prova'].find_one({'_id': 1})
    assert d and d['email'] == 'a@x', d
    b[DB]['prova'].update_one({'_id': 1}, {'$inc': {'n': 5}, '$set': {'email': 'b@x'}})
    d = a[DB]['prova'].find_one({'_id': 1})
    assert d['n'] == 6 and d['email'] == 'b@x', d
    try:
        a[DB]['prova'].insert_one({'_id': 2, 'email': 'b@x'})
        raise AssertionError('doppione accettato')
    except DuplicateKeyError:
        pass
    assert a[DB]['prova'].find_one({'email': 'b@x'})['_id'] == 1
    assert b[DB]['prova'].count_documents({}) == 1
    a.close(), b.close()
    return {'esito': 'ok'}


def prova_inserimenti(nome, op, campo, secs=20, workers=8, evento=None, unique=True):
    nuova_prova(nome, unique)
    car, t0, t1, info = corsa(nome, op, secs, workers, evento)
    leggi = [s for s in ('A', 'B') if procs[s].poll() is None]
    docsA = leggi_tutto(leggi[0])
    docsB = leggi_tutto(leggi[-1])
    errori, numeri = controlla_inserimenti(car.log, docsA, campo)
    if sorted(map(str, (d['_id'] for d in docsA))) != sorted(map(str, (d['_id'] for d in docsB))):
        errori.append('A e B non vedono gli stessi documenti')
    if unique:
        errori += indice_coerente(leggi[-1], docsA)
    return risultato(nome, car, t0, t1, errori, numeri, info)


def risultato(nome, car, t0, t1, errori, numeri, info):
    r = {'prova': nome, 'secondi': round(t1 - t0, 1), 'esiti': conta(car.log), 'per_server': {s: conta([x for x in car.log if x[2] == s]) for s in ('A', 'B')},
         'ops_confermate_al_secondo': round(sum(1 for x in car.log if x[5] in ('ok', 'dup')) / (t1 - t0), 1),
         'numeri': numeri, 'errori': errori, 'vince': not errori, 'stats': {s: stats(s) for s in ('A', 'B') if procs[s].poll() is None}}
    r.update(info)
    codes = defaultdict(int)
    for x in car.log:
        if x[5] != 'ok':
            codes[f'{x[5]}:{x[6]}'] += 1
    r['codici'] = dict(codes)
    return r


def prova_inc(nome='inc', secs=20, workers=8, C=10, evento=None):
    nuova_prova(nome)
    prepara_contatori(C)
    car, t0, t1, info = corsa(nome, op_inc(C), secs, workers, evento)
    leggi = [s for s in ('A', 'B') if procs[s].poll() is None]
    docs = leggi_tutto(leggi[0])
    errori, numeri = controlla_inc(car.log, docs, C)
    return risultato(nome, car, t0, t1, errori, numeri, info)


def op_cambia_chiave(K):
    @tagged
    def op(coll, rnd, tag):
        i = rnd.randrange(K)
        e = f'u{rnd.randrange(2 * K)}@x'
        return i, e, lambda: coll.update_one({'_id': i}, {'$set': {'email': e, 'w': tag}})
    return op


def prova_cambia_chiave(secs=20, workers=8, K=300):
    nuova_prova('cambia_chiave')
    c = client('A')
    c[DB]['prova'].insert_many([{'_id': i, 'email': f'u{i}@x', 'w': 'prep'} for i in range(K)])
    c.close()
    car, t0, t1, info = corsa('cambia_chiave', op_cambia_chiave(K), secs, workers)
    docs = leggi_tutto('A')
    errori = []
    per = defaultdict(list)
    for d in docs:
        per[d['email']].append(d['_id'])
    dup = {k: v for k, v in per.items() if len(v) > 1}
    if dup:
        errori.append(f'{len(dup)} email con più documenti, es. {list(dup.items())[:3]}')
    if len(docs) != K:
        errori.append(f'{len(docs)} documenti invece di {K}')
    # l'ultimo valore scritto deve essere uno confermato o ignoto per quel documento
    scritti = defaultdict(set)
    for x in car.log:
        if x[5] in ('ok', 'ignoto'):
            scritti[x[3]].add(x[4])
    strani = [(d['_id'], d['email']) for d in docs if d['w'] != 'prep' and d['email'] not in scritti[d['_id']]]
    if strani:
        errori.append(f'{len(strani)} documenti con un email mai scritto da un update riuscito, es. {strani[:3]}')
    # ogni valore dell'indice porta al documento giusto, anche per gli email non più usati
    c = client('B', 60000)
    coll = c[DB]['prova']
    stantie = []
    for j in range(2 * K):
        e = f'u{j}@x'
        got = [g['_id'] for g in coll.find({'email': e}).hint('email_1')]
        want = per.get(e, [])
        if sorted(got) != sorted(want):
            stantie.append((e, got, want))
    c.close()
    if stantie:
        errori.append(f'indice incoerente su {len(stantie)} email, es. {stantie[:3]}')
    return risultato('cambia_chiave', car, t0, t1, errori, {'documenti': len(docs)}, info)


def evento_morte_server(dopo=7, riparti=6):
    def ev(t0, car):
        def go():
            time.sleep(dopo)
            car.t_kill = time.time()
            server_kill('A')
            time.sleep(riparti)
            t = time.time()
            server_up('A')
            car.t_restart = time.time()
            car.restart_s = car.t_restart - t
        car.killer = threading.Thread(target=go, daemon=True)
        car.killer.start()
    return ev


def evento_morte_capo(dopo=7):
    def ev(t0, car):
        def go():
            time.sleep(dopo)
            l = capo()
            car.capo_ucciso = l
            car.t_kill = time.time()
            sh([sys.executable, CLUSTER, 'kill', str(l)])
            time.sleep(0.5)
            car.capo_nuovo = capo()
            car.t_nuovo = time.time()
        car.killer = threading.Thread(target=go, daemon=True)
        car.killer.start()
    return ev


def dopo_evento(r, car, t0):
    if hasattr(car, 't_kill'):
        r['t_uccisione_s'] = round(car.t_kill - t0, 2)
        r['buco_massimo_dopo_uccisione_s'] = {s: (round(buco_massimo(car.log, car.t_kill - 0.5, s), 3) if buco_massimo(car.log, car.t_kill - 0.5, s) else None) for s in ('A', 'B')}
        r['ops_al_secondo'] = {s: per_secondo(car.log, t0, t0 + r['secondi'], s) for s in ('A', 'B')}
    for k in ('capo_ucciso', 'capo_nuovo', 'restart_s'):
        if hasattr(car, k):
            r[k] = getattr(car, k)
    if hasattr(car, 't_restart'):
        firsts = [x[1] for x in car.log if x[2] == 'A' and x[5] in ('ok', 'dup') and x[0] >= car.t_restart]
        r['prima_op_di_A_dopo_ripartenza_s'] = round(min(firsts) - car.t_restart, 3) if firsts else None
    return r


def tipo_di(log, t):
    return [(a, b, s, k[1] if isinstance(k, tuple) else k, v, r, c) for (a, b, s, k, v, r, c) in log if isinstance(k, tuple) and k[0] == t]


def prova_mista(nome, evento, secs=25, workers=8, K=4, C=10):
    nuova_prova(nome)
    prepara_contatori(C)
    # le eccezioni di `tagged` portano la chiave senza tipo: la si ricostruisce qui
    a, b, c = op_stesso_id(K), op_stessa_chiave(K), op_inc(C)
    def op(coll, rnd, tag):
        x = rnd.random()
        f, t = (a, 'id') if x < 1 / 3 else (b, 'email') if x < 2 / 3 else (c, 'inc')
        try:
            k, v = f(coll, rnd, tag)
            return (t, k), v
        except Exception as e:
            e._key = (t, getattr(e, '_key', None))
            raise
    car, t0, t1, info = corsa(nome, op, secs, workers, evento)
    if hasattr(car, 'killer'):
        car.killer.join(30)
    time.sleep(1)
    leggi = [s for s in ('A', 'B') if procs[s].poll() is None]
    docs = leggi_tutto(leggi[0])
    docsB = leggi_tutto(leggi[-1])
    errori = []
    if sorted(map(str, (d['_id'] for d in docs))) != sorted(map(str, (d['_id'] for d in docsB))):
        errori.append('A e B non vedono gli stessi documenti')
    di = [d for d in docs if isinstance(d['_id'], int)]
    de = [d for d in docs if isinstance(d['_id'], str) and not d['_id'].startswith('cont')]
    e1, n1 = controlla_inserimenti(tipo_di(car.log, 'id'), di, '_id')
    e2, n2 = controlla_inserimenti(tipo_di(car.log, 'email'), de, 'email')
    e3, n3 = controlla_inc(tipo_di(car.log, 'inc'), docs, C)
    per_email = defaultdict(int)
    for d in docs:
        per_email[d.get('email')] += 1
    e4 = [f'{sum(1 for v in per_email.values() if v > 1)} email doppie su tutta la collezione'] if any(v > 1 for v in per_email.values()) else []
    errori += [f'stesso _id: {x}' for x in e1] + [f'stessa email: {x}' for x in e2] + [f'incrementi: {x}' for x in e3] + e4 + indice_coerente(leggi[-1], docs)
    r = risultato(nome, car, t0, t1, errori, {'stesso_id': n1, 'stessa_email': n2, 'incrementi': n3}, info)
    return dopo_evento(r, car, t0)


def prova_senza_guardia(secs=20, workers=8, K=4):
    """Prova negativa: la guardia dell'indice unico spenta. Ci si aspettano doppioni sull'email."""
    servers_down()
    quaderno_down()
    quaderno_up()
    server_up('A', {'FENVIX_Q_NO_GUARD': '1'})
    server_up('B', {'FENVIX_Q_NO_GUARD': '1'})
    c = client('A')
    c[DB]['prova'].create_index('email', unique=True)
    c.close()
    car, t0, t1, info = corsa('senza_guardia', op_stessa_chiave(K), secs, workers)
    docs = leggi_tutto('A')
    errori, numeri = controlla_inserimenti(car.log, docs, 'email')
    r = risultato('senza_guardia', car, t0, t1, errori, numeri, info)
    r['nota'] = 'prova negativa: qui gli errori sono attesi (mostrano perché serve la guardia)'
    return r


PROVE = {
    'fumo': prova_fumo,
    'stesso_id': lambda: prova_inserimenti('stesso_id', op_stesso_id(4), '_id'),
    'stessa_chiave': lambda: prova_inserimenti('stessa_chiave', op_stessa_chiave(4), 'email'),
    'inc': prova_inc,
    'cambia_chiave': prova_cambia_chiave,
    'morte_server': lambda: prova_mista('morte_server', evento_morte_server()),
    'morte_capo': lambda: prova_mista('morte_capo', evento_morte_capo()),
    'senza_guardia': prova_senza_guardia,
}


def main():
    names = sys.argv[1:] or ['tutto']
    if names == ['tutto']:
        names = list(PROVE)
    os.makedirs(RES, exist_ok=True)
    os.makedirs(WORK, exist_ok=True)
    out = []
    try:
        for n in names:
            print(f'== {n}', flush=True)
            t = time.time()
            try:
                r = PROVE[n]()
            except Exception as e:
                import traceback
                traceback.print_exc()
                r = {'prova': n, 'eccezione': repr(e), 'vince': False}
            r['durata_s'] = round(time.time() - t, 1)
            print(json.dumps(r, ensure_ascii=False, default=str), flush=True)
            out.append(r)
            json.dump(r, open(os.path.join(RES, f'{n}.json'), 'w'), indent=1, ensure_ascii=False, default=str)
    finally:
        servers_down()
        quaderno_down()


if __name__ == '__main__':
    main()
