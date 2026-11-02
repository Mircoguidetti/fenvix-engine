#!/usr/bin/env python3
"""Prova M8 "senza cuoco fisso": due mongo-server servono lo stesso database sopra proto-quaderno.

Ogni scrittura va al quaderno come commit validato (mongo-server/src/qmode.rs); nessun processo possiede il database.

  prova.py tutto            tutte le prove, risultati in results/
  prova.py <nome> [...]     solo alcune (vedi PROVE in fondo)
  prova.py correzioni       solo le prove dei tre errori trovati dal revisore e le sue prove in gara

I server partono con --replica-set e i client con retryWrites=true (come in produzione); SC_RS=0 per il modo vecchio.

Porte: quaderno 7200-7202 (repliche), mongo-server A 27101, B 27102. Cartella di lavoro: $SC_WORK (default /root/sc-work).
"""
import json, os, random, signal, socket, struct, subprocess, sys, threading, time, uuid
from collections import defaultdict

import bson
import pymongo
from pymongo.errors import DuplicateKeyError, OperationFailure, AutoReconnect, ConnectionFailure, NetworkTimeout, ServerSelectionTimeoutError

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, '..', '..'))
MS = os.environ.get('SC_MS') or os.path.join(ROOT, 'target', 'release', 'mongo-server')
CLUSTER = os.path.join(ROOT, 'proto', 'quaderno', 'scripts', 'cluster.py')
WORK = os.environ.get('SC_WORK', '/root/sc-work')
RES = os.path.join(HERE, 'results')
PORTS = {'A': 27101, 'B': 27102}
QADDR = '127.0.0.1:7200,127.0.0.1:7201,127.0.0.1:7202'
DB = 'cucina'
RS = os.environ.get('SC_RS', '1') != '0'
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
    if RS:
        args += ['--replica-set', 'rs0', '--advertise', f'127.0.0.1:{PORTS[name]}']
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
                               socketTimeoutMS=timeout_ms, connectTimeoutMS=2000, retryWrites=RS, retryReads=False, maxPoolSize=64)


def esito(e):
    """ok | dup (sicuramente non applicata) | no (sicuramente non applicata) | ignoto"""
    if isinstance(e, DuplicateKeyError):
        return 'dup'
    if isinstance(e, OperationFailure):
        if e.code in (112, 91, 257):  # conflitto dopo tutti i tentativi, freno, comando troppo grande: non è entrata
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


# ------------------------------------------------------------------------------------- prove del revisore (M8-correzioni)

def prova_cancella_reinserisci(secs=20, workers=8, K=6):
    """Revisore, prova 1: findOneAndDelete per email e insert con _id nuovo, sulle stesse poche email, da A e da B."""
    nuova_prova('cancella_reinserisci')
    emails = [f'c{i}@x' for i in range(K)]

    def op(coll, rnd, tag):
        e = rnd.choice(emails)
        if rnd.random() < 0.5:
            try:
                coll.insert_one({'_id': tag, 'w': tag, 'email': e})
            except Exception as x:
                x._key, x._val = ('ins', e), tag
                raise
            return ('ins', e), tag
        try:
            d = coll.find_one_and_delete({'email': e})
        except Exception as x:
            x._key, x._val = ('del', e), None
            raise
        return ('del', e), (d or {}).get('_id')

    car, t0, t1, info = corsa('cancella_reinserisci', op, secs, workers)
    docs = leggi_tutto('A')
    errori = []
    vivi = {d['_id']: d for d in docs}
    ins_ok = {x[4] for x in car.log if x[3] and x[3][0] == 'ins' and x[5] == 'ok'}
    ins_ign = {x[4] for x in car.log if x[3] and x[3][0] == 'ins' and x[5] == 'ignoto'}
    dels = [x[4] for x in car.log if x[3] and x[3][0] == 'del' and x[5] == 'ok' and x[4] is not None]
    del_ign = {x[3][1] for x in car.log if x[3] and x[3][0] == 'del' and x[5] == 'ignoto'}
    contati = defaultdict(int)
    for i in dels:
        contati[i] += 1
    due = [k for k, v in contati.items() if v > 1]
    if due:
        errori.append(f'{len(due)} documenti cancellati due volte (es. {due[:3]})')
    zombi = [i for i in contati if i in vivi]
    if zombi:
        errori.append(f'{len(zombi)} documenti cancellati e ancora vivi (es. {zombi[:3]})')
    fantasmi = [i for i in contati if i not in ins_ok and i not in ins_ign]
    if fantasmi:
        errori.append(f'{len(fantasmi)} cancellazioni hanno restituito un documento mai inserito (es. {fantasmi[:3]})')
    email_di = {x[4]: x[3][1] for x in car.log if x[3] and x[3][0] == 'ins'}
    # un inserimento confermato che non c'è più deve essere stato cancellato: da una cancellazione confermata che lo ha
    # restituito, o da una con esito ignoto sulla stessa email
    perse = [i for i in ins_ok if i not in vivi and i not in contati and email_di.get(i) not in del_ign]
    if perse:
        errori.append(f'{len(perse)} inserimenti confermati spariti senza una cancellazione (es. {perse[:3]})')
    dal_nulla = [i for i in vivi if i not in ins_ok and i not in ins_ign]
    if dal_nulla:
        errori.append(f'{len(dal_nulla)} documenti vivi senza inserimento confermato o ignoto (es. {dal_nulla[:3]})')
    per = defaultdict(int)
    for d in docs:
        per[d['email']] += 1
    if any(v > 1 for v in per.values()):
        errori.append(f'email doppie: {[k for k, v in per.items() if v > 1]}')
    errori += indice_valori('B', emails, docs)
    numeri = {'inserimenti_confermati': len(ins_ok), 'cancellazioni_con_documento': len(dels), 'documenti_finali': len(docs)}
    return risultato('cancella_reinserisci', car, t0, t1, errori, numeri, info)


def indice_valori(name, valori, docs):
    """Per ogni valore: la ricerca attraverso l'indice email dà esattamente i documenti che hanno quel valore."""
    c = client(name, 60000)
    coll = c[DB]['prova']
    per = defaultdict(list)
    for d in docs:
        per[d.get('email')].append(str(d['_id']))
    male = []
    for e in valori:
        got = sorted(str(g['_id']) for g in coll.find({'email': e}).hint('email_1'))
        if got != sorted(per.get(e, [])):
            male.append((e, got, per.get(e, [])))
    c.close()
    return [f'indice incoerente su {len(male)} valori, es. {male[:3]}'] if male else []


def prova_toglie_rimette(secs=20, workers=8, N=20, K=4):
    """Revisore, prova 2: N documenti spostano l'email tra K valori contesi e un valore privato, mentre altri lavoratori
    inseriscono e cancellano quegli stessi valori contesi."""
    nuova_prova('toglie_rimette')
    c = client('A')
    c[DB]['prova'].insert_many([{'_id': i, 'email': f'p{i}@x', 'w': 'prep'} for i in range(N)])
    c.close()
    contesi = [f'c{i}@x' for i in range(K)]

    def op(coll, rnd, tag):
        x = rnd.random()
        e = rnd.choice(contesi)
        try:
            if x < 0.5:
                i = rnd.randrange(N)
                v = e if rnd.random() < 0.6 else f'p{i}@x'
                coll.update_one({'_id': i}, {'$set': {'email': v, 'w': tag}})
                return ('sposta', i), v
            if x < 0.75:
                coll.insert_one({'_id': tag, 'email': e, 'w': tag})
                return ('ins', e), tag
            coll.delete_one({'email': e, '_id': {'$type': 'string'}})
            return ('del', e), None
        except Exception as ex:
            ex._key = ('?', e)
            raise

    car, t0, t1, info = corsa('toglie_rimette', op, secs, workers)
    docs = leggi_tutto('A')
    errori = []
    per = defaultdict(int)
    for d in docs:
        per[d['email']] += 1
    if any(v > 1 for v in per.values()):
        errori.append(f'email doppie: {[k for k, v in per.items() if v > 1][:5]}')
    fissi = [d for d in docs if isinstance(d['_id'], int)]
    if len(fissi) != N:
        errori.append(f'{len(fissi)} documenti fissi invece di {N}')
    errori += indice_valori('B', contesi + [f'p{i}@x' for i in range(N)], docs)
    return risultato('toglie_rimette', car, t0, t1, errori, {'documenti_finali': len(docs)}, info)


def prova_upsert(secs=20, workers=8, K=4):
    """Revisore, prova 3: updateOne({email}, {$inc: {n: 1}}, upsert) in gara sulla chiave unica."""
    nuova_prova('upsert')

    def op(coll, rnd, tag):
        e = f'u{conteso(rnd, K, 100)}@x'
        try:
            coll.update_one({'email': e}, {'$inc': {'n': 1}}, upsert=True)
        except Exception as ex:
            ex._key, ex._val = e, tag
            raise
        return e, tag

    car, t0, t1, info = corsa('upsert', op, secs, workers)
    docs = leggi_tutto('A')
    errori = []
    ok = defaultdict(int)
    ign = defaultdict(int)
    for x in car.log:
        if x[5] == 'ok':
            ok[x[3]] += 1
        elif x[5] == 'ignoto':
            ign[x[3]] += 1
    per = defaultdict(list)
    for d in docs:
        per[d['email']].append(d.get('n', 0))
    dup = [k for k, v in per.items() if len(v) > 1]
    if dup:
        errori.append(f'{len(dup)} email con più documenti (es. {dup[:3]})')
    for e in set(ok) | set(per):
        n = sum(per.get(e, []))
        if n < ok[e]:
            errori.append(f'{e}: n={n} ma {ok[e]} confermati')
        if n > ok[e] + ign[e]:
            errori.append(f'{e}: n={n} ma {ok[e]} confermati e {ign[e]} ignoti')
    errori = errori[:20]
    numeri = {'upsert_confermati': sum(ok.values()), 'somma_n': sum(sum(v) for v in per.values()), 'email': len(per)}
    return risultato('upsert', car, t0, t1, errori, numeri, info)


def prova_coda(secs=20, workers=8, N=4000):
    """Revisore, prova 4: una coda di lavori presi con findOneAndUpdate({stato: libero}, sort prio). Il controllo
    ottimistico qui passa per letture per scansione, non per get."""
    nuova_prova('coda', unique=False)
    c = client('A')
    co = c[DB]['prova']
    co.create_index([('stato', 1), ('prio', 1)])
    r = random.Random(7)
    co.insert_many([{'_id': i, 'stato': 'libero', 'prio': r.randrange(1000)} for i in range(N)])
    c.close()

    def op(coll, rnd, tag):
        try:
            d = coll.find_one_and_update({'stato': 'libero'}, {'$set': {'stato': 'preso', 'da': tag}}, sort=[('prio', 1)])
        except Exception as ex:
            ex._key, ex._val = 'job', tag
            raise
        return 'job', (tag, d['_id'] if d else None)

    car, t0, t1, info = corsa('coda', op, secs, workers)
    docs = leggi_tutto('A')
    errori = []
    presi = defaultdict(list)
    for x in car.log:
        if x[5] == 'ok' and x[4] and x[4][1] is not None:
            presi[x[4][1]].append(x[4][0])
    due = {k: v for k, v in presi.items() if len(v) > 1}
    if due:
        errori.append(f'{len(due)} lavori presi da due lavoratori (es. {list(due.items())[:3]})')
    ign = {x[4] for x in car.log if x[5] == 'ignoto'}
    byid = {d['_id']: d for d in docs}
    sbagliati = [(k, v, byid[k].get('da')) for k, v in presi.items() if byid[k].get('da') != v[0]]
    if sbagliati:
        errori.append(f'{len(sbagliati)} lavori con il prenditore sbagliato (es. {sbagliati[:3]})')
    presi_db = [d for d in docs if d['stato'] == 'preso']
    orfani = [d['_id'] for d in presi_db if d['_id'] not in presi and d.get('da') not in ign]
    if orfani:
        errori.append(f'{len(orfani)} lavori presi senza conferma né esito ignoto (es. {orfani[:3]})')
    return risultato('coda', car, t0, t1, errori, {'lavori_presi': len(presi), 'lavori_presi_nel_db': len(presi_db)}, info)


def prova_letture_lineari(secs=20, dopo=8.3):
    """Revisore, prova 5: uno scrive su A e rilegge subito da B (e viceversa); 4 lettori controllano che il valore non
    torni mai indietro; il capo del quaderno viene ucciso a metà."""
    nuova_prova('letture_lineari', unique=False)
    c = client('A')
    c[DB]['prova'].insert_many([{'_id': 'wA', 'v': 0}, {'_id': 'wB', 'v': 0}])
    c.close()
    stop = threading.Event()
    viol = []
    cnt = defaultdict(int)
    lock = threading.Lock()

    def writer(src, dst, key):
        cs, cd = client(src), client(dst)
        v = 0
        while not stop.is_set():
            v += 1
            try:
                cs[DB]['prova'].update_one({'_id': key}, {'$set': {'v': v}})
            except Exception:
                with lock:
                    cnt['scritture_non_confermate'] += 1
                time.sleep(0.05)
                continue
            try:
                d = cd[DB]['prova'].find_one({'_id': key})
            except Exception:
                with lock:
                    cnt['riletture_fallite'] += 1
                continue
            with lock:
                cnt['scritture_rilette'] += 1
                if d['v'] < v:
                    viol.append(f'{key}: scritto {v} su {src}, letto {d["v"]} su {dst}')

    def reader(srv):
        cl = client(srv)
        last = {'wA': 0, 'wB': 0}
        while not stop.is_set():
            for k in ('wA', 'wB'):
                try:
                    d = cl[DB]['prova'].find_one({'_id': k})
                except Exception:
                    continue
                with lock:
                    cnt['letture_controllate'] += 1
                    if d['v'] < last[k]:
                        viol.append(f'{k}: lettore su {srv} ha visto {last[k]} poi {d["v"]}')
                last[k] = max(last[k], d['v'])

    ths = [threading.Thread(target=writer, args=('A', 'B', 'wA'), daemon=True), threading.Thread(target=writer, args=('B', 'A', 'wB'), daemon=True)]
    ths += [threading.Thread(target=reader, args=(s,), daemon=True) for s in ('A', 'A', 'B', 'B')]
    t0 = time.time()
    for t in ths:
        t.start()
    time.sleep(dopo)
    l = capo()
    sh([sys.executable, CLUSTER, 'kill', str(l)])
    t_kill = time.time() - t0
    time.sleep(max(0, secs - (time.time() - t0)))
    stop.set()
    for t in ths:
        t.join(30)
    errori = viol[:10] + ([f'... {len(viol)} violazioni in tutto'] if len(viol) > 10 else [])
    if cnt['scritture_rilette'] < 100:
        errori.append(f'troppe poche scritture rilette: {cnt["scritture_rilette"]}')
    return {'prova': 'letture_lineari', 'secondi': round(time.time() - t0, 1), 'capo_ucciso': l, 't_uccisione_s': round(t_kill, 2),
            'numeri': dict(cnt), 'violazioni': len(viol), 'errori': errori, 'vince': not errori}


# --- messaggi a mano (OP_MSG) per i ritentativi con lsid e txnNumber scelti da noi

_rid = [0]


def opmsg(port, cmd, timeout=30):
    _rid[0] += 1
    body = struct.pack('<I', 0) + b'\x00' + bson.encode(cmd)
    s = socket.create_connection(('127.0.0.1', port), timeout)
    try:
        s.sendall(struct.pack('<iiii', 16 + len(body), _rid[0], 0, 2013) + body)
        hdr = b''
        while len(hdr) < 16:
            x = s.recv(16 - len(hdr))
            if not x:
                raise ConnectionError('chiusa')
            hdr += x
        n = struct.unpack('<i', hdr[:4])[0] - 16
        buf = b''
        while len(buf) < n:
            x = s.recv(n - len(buf))
            if not x:
                raise ConnectionError('chiusa')
            buf += x
        return bson.decode(buf[5:])
    finally:
        s.close()


def inc_cmd(lsid, txn, key='r'):
    return {'update': 'prova', 'updates': [{'q': {'_id': key}, 'u': {'$inc': {'n': 1}}}], 'ordered': True, 'lsid': lsid, 'txnNumber': bson.Int64(txn), '$db': DB}


def prova_ritentativo():
    """Revisore, prova 6: il driver ritenta la stessa scrittura (stesso lsid e txnNumber) su un altro cameriere."""
    nuova_prova('ritentativo', unique=False)
    c = client('A')
    co = c[DB]['prova']
    co.insert_many([{'_id': 'r1', 'n': 0}, {'_id': 'r2', 'n': 0}])
    errori = []
    risposte = {}
    # caso 1: B non ha mai visto la sessione
    L1 = {'id': bson.Binary(uuid.uuid4().bytes, 4)}
    risposte['caso1'] = [opmsg(PORTS['A'], inc_cmd(L1, 1, 'r1')), opmsg(PORTS['B'], inc_cmd(L1, 1, 'r1'))]
    n1 = co.find_one({'_id': 'r1'})['n']
    if n1 != 1:
        errori.append(f'caso 1: n = {n1} invece di 1')
    # caso 2: B ha la sessione in memoria ferma a txn 1, A fa txn 2, il driver ritenta txn 2 su B
    L2 = {'id': bson.Binary(uuid.uuid4().bytes, 4)}
    risposte['caso2'] = [opmsg(PORTS['B'], inc_cmd(L2, 1, 'r2')), opmsg(PORTS['A'], inc_cmd(L2, 2, 'r2')), opmsg(PORTS['B'], inc_cmd(L2, 2, 'r2'))]
    n2 = co.find_one({'_id': 'r2'})['n']
    if n2 != 2:
        errori.append(f'caso 2: n = {n2} invece di 2')
    # caso 3: txn vecchio dopo uno nuovo su un altro cameriere: TransactionTooOld
    r3 = opmsg(PORTS['B'], inc_cmd(L2, 1, 'r2'))
    risposte['caso3'] = [r3]
    if r3.get('code') != 225:
        errori.append(f'caso 3: txn 1 dopo txn 2 non rifiutato con TransactionTooOld: {r3}')
    n3 = co.find_one({'_id': 'r2'})['n']
    if n3 != 2:
        errori.append(f'caso 3: n = {n3} invece di 2')
    c.close()
    return {'prova': 'ritentativo', 'numeri': {'caso1_n': n1, 'caso2_n': n2, 'caso3_n': n3}, 'risposte': risposte, 'errori': errori, 'vince': not errori}


def prova_ritentativi_in_gara(secs=20, workers=6, C=4):
    """Lo stesso comando (stesso lsid e txnNumber) mandato insieme ad A e a B, come un ritentativo che parte mentre
    l'originale è ancora in volo. Ogni (lsid, txnNumber) deve contare una volta sola."""
    nuova_prova('ritentativi_in_gara', unique=False)
    c = client('A')
    c[DB]['prova'].insert_many([{'_id': f'k{i}', 'n': 0} for i in range(C)])
    c.close()
    stop = threading.Event()
    log = []
    lock = threading.Lock()

    def one(port, cmd, out, i):
        try:
            r = opmsg(port, cmd)
            out[i] = 'ok' if r.get('ok') == 1 and not r.get('writeErrors') and r.get('n') == 1 else f'no:{r.get("code")}:{r.get("errmsg", "")[:60]}'
        except Exception as e:
            out[i] = f'ignoto:{type(e).__name__}'

    def worker(w):
        rnd = random.Random(w * 7919 + int(time.time()))
        lsid = {'id': bson.Binary(uuid.uuid4().bytes, 4)}
        txn = 0
        while not stop.is_set():
            txn += 1
            k = f'k{rnd.randrange(C)}'
            cmd = inc_cmd(lsid, txn, k)
            out = [None, None]
            ths = [threading.Thread(target=one, args=(PORTS[s], cmd, out, i)) for i, s in enumerate(('A', 'B') if rnd.random() < 0.5 else ('B', 'A'))]
            for t in ths:
                t.start()
            for t in ths:
                t.join()
            with lock:
                log.append((k, txn, out[0], out[1]))

    ths = [threading.Thread(target=worker, args=(w,), daemon=True) for w in range(workers)]
    t0 = time.time()
    for t in ths:
        t.start()
    time.sleep(secs)
    stop.set()
    for t in ths:
        t.join(60)
    docs = {d['_id']: d['n'] for d in leggi_tutto('A')}
    ok = defaultdict(int)
    ign = defaultdict(int)
    esiti = defaultdict(int)
    for (k, txn, a, b) in log:
        esiti[(a or '').split(':')[0] + '+' + (b or '').split(':')[0]] += 1
        if 'ok' in (a, b):
            ok[k] += 1
        elif any((x or '').startswith('ignoto') for x in (a, b)):
            ign[k] += 1
    errori = []
    for i in range(C):
        k = f'k{i}'
        if docs[k] < ok[k]:
            errori.append(f'{k}: n={docs[k]} ma {ok[k]} scritture confermate (perse)')
        if docs[k] > ok[k] + ign[k]:
            errori.append(f'{k}: n={docs[k]} ma solo {ok[k]} scritture distinte confermate e {ign[k]} ignote (doppiate {docs[k] - ok[k] - ign[k]})')
    return {'prova': 'ritentativi_in_gara', 'secondi': round(time.time() - t0, 1), 'numeri': {'scritture_distinte': len(log), 'confermate': sum(ok.values()), 'ignote': sum(ign.values()), 'somma_n': sum(docs.values())},
            'esiti_coppie': dict(esiti), 'errori': errori, 'vince': not errori, 'stats': {s: stats(s) for s in ('A', 'B')}}


def prova_insert_mille():
    """insert di più di 1.000 documenti in un comando solo (mandato a mano: il driver non lo spezza), un client solo."""
    nuova_prova('insert_mille', unique=False)
    errori = []
    tempi = {}
    for n in (500, 999, 1001, 1500, 2500):
        coll = f'm{n}'
        t = time.time()
        r = opmsg(PORTS['A'], {'insert': coll, 'documents': [{'_id': i, 'x': 'y' * 20} for i in range(n)], 'ordered': True, '$db': DB}, timeout=120)
        tempi[n] = round(time.time() - t, 2)
        c = client('B')
        cnt = c[DB][coll].count_documents({})
        nat = len(list(c[DB][coll].find({}, batch_size=100000)))
        c.close()
        if r.get('ok') != 1 or r.get('n') != n:
            errori.append(f'{n} documenti: risposta {str(r)[:200]}')
        if cnt != n or nat != n:
            errori.append(f'{n} documenti: contati {cnt}, scansione {nat}')
    return {'prova': 'insert_mille', 'secondi_per_comando': tempi, 'errori': errori, 'vince': not errori, 'stats': {s: stats(s) for s in ('A', 'B')}}


def prova_troppo_grande(N=40000, KB=2):
    """Un comando più grande di un record del quaderno deve fallire subito con un errore chiaro e senza scrivere niente.
    Un insert_many grande dal driver deve invece passare (il driver lo spezza secondo maxWriteBatchSize)."""
    nuova_prova('troppo_grande', unique=False)
    errori = []
    c = client('A', 120000)
    co = c[DB]['prova']
    pad = 'p' * (KB * 1024)
    t = time.time()
    for a in range(0, N, 100):  # a pezzi da 100: questa parte prepara i dati, non prova i comandi grandi
        co.insert_many([{'_id': i, 'pad': pad, 'v': 0} for i in range(a, min(N, a + 100))])
    t_ins = round(time.time() - t, 2)
    if co.count_documents({}) != N:
        errori.append(f'insert_many di {N}: {co.count_documents({})} documenti')
    # 1) troppi byte: updateMany su tutto (N x KB = circa 80 MB più le chiavi, come nella prova del revisore)
    t = time.time()
    try:
        r = co.update_many({}, {'$set': {'pad': 'q' * (KB * 1024), 'v': 1}})
        esito1 = f'riuscito: {r.modified_count}'
        errori.append(f'updateMany troppo grande riuscito ({r.modified_count})')
    except OperationFailure as e:
        esito1 = f'errore {e.code}: {str(e)[:200]}'
        if e.code != 257:
            errori.append(f'updateMany troppo grande: codice {e.code} invece di 257 ({str(e)[:120]})')
    except Exception as e:
        esito1 = f'eccezione {type(e).__name__}: {str(e)[:200]}'
        errori.append(f'updateMany troppo grande: {esito1}')
    t_upd = round(time.time() - t, 2)
    toccati = co.count_documents({'v': 1})
    if toccati:
        errori.append(f'updateMany fallito ma {toccati} documenti cambiati')
    # il tempo è quello delle letture del comando (40.000 documenti letti a S) prima del controllo; prima della
    # correzione la risposta era "esito ignoto" dopo i 10 s di scadenza del commit, ritentati
    if t_upd > 60:
        errori.append(f'updateMany troppo grande: risposta dopo {t_upd} s')
    # 2) troppe chiavi: un comando insert solo (a mano) di 40.000 documenti piccoli: 80.000 scritture (documento e numero d'ordine), più di 65.535
    t = time.time()
    try:
        r2 = opmsg(PORTS['A'], {'insert': 'tante', 'documents': [{'_id': i} for i in range(40000)], 'ordered': True, '$db': DB}, timeout=120)
    except Exception as e:
        r2 = {'eccezione': repr(e)}
    t_ins2 = round(time.time() - t, 2)
    n2 = c[DB]['tante'].count_documents({})
    if r2.get('code') != 257:
        errori.append(f'insert di 40.000 in un comando: {str(r2)[:200]}')
    if n2:
        errori.append(f'insert di 40.000 fallito ma {n2} documenti scritti')
    # 3) lo stesso con insert_many del driver: spezzato in più comandi, passa
    t = time.time()
    try:
        c[DB]['tante2'].insert_many([{'_id': i} for i in range(40000)])
    except Exception as e:
        errori.append(f'insert_many di 40.000 dal driver: {type(e).__name__} {str(e)[:120]}')
    t_ins3 = round(time.time() - t, 2)
    n3 = c[DB]['tante2'].count_documents({})
    if n3 != 40000:
        errori.append(f'insert_many di 40.000 dal driver: {n3} documenti')
    c.close()
    return {'prova': 'troppo_grande', 'numeri': {'insert_many_s': t_ins, 'update_many_troppo_grande': esito1, 'update_many_s': t_upd,
            'insert_40000_un_comando': str(r2)[:300], 'insert_40000_un_comando_s': t_ins2, 'insert_many_40000_driver_s': t_ins3},
            'errori': errori, 'vince': not errori, 'stats': {s: stats(s) for s in ('A', 'B')}}


PROVE = {
    'fumo': prova_fumo,
    'stesso_id': lambda: prova_inserimenti('stesso_id', op_stesso_id(4), '_id'),
    'stessa_chiave': lambda: prova_inserimenti('stessa_chiave', op_stessa_chiave(4), 'email'),
    'inc': prova_inc,
    'cambia_chiave': prova_cambia_chiave,
    'morte_server': lambda: prova_mista('morte_server', evento_morte_server()),
    'morte_capo': lambda: prova_mista('morte_capo', evento_morte_capo()),
    'senza_guardia': prova_senza_guardia,
    # M8-correzioni: le prove del revisore e una per ognuno dei tre errori
    'cancella_reinserisci': prova_cancella_reinserisci,
    'toglie_rimette': prova_toglie_rimette,
    'upsert': prova_upsert,
    'coda': prova_coda,
    'letture_lineari': prova_letture_lineari,
    'ritentativo': prova_ritentativo,
    'ritentativi_in_gara': prova_ritentativi_in_gara,
    'insert_mille': prova_insert_mille,
    'troppo_grande': prova_troppo_grande,
}
CORREZIONI = ['ritentativo', 'ritentativi_in_gara', 'insert_mille', 'troppo_grande', 'cancella_reinserisci', 'toglie_rimette', 'upsert', 'coda', 'letture_lineari']


def main():
    names = sys.argv[1:] or ['tutto']
    if names == ['tutto']:
        names = list(PROVE)
    elif names == ['correzioni']:
        names = CORREZIONI
    res = os.environ.get('SC_RES', RES)
    os.makedirs(res, exist_ok=True)
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
            json.dump(r, open(os.path.join(res, f'{n}.json'), 'w'), indent=1, ensure_ascii=False, default=str)
    finally:
        servers_down()
        quaderno_down()


if __name__ == '__main__':
    main()
