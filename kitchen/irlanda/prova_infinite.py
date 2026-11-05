#!/usr/bin/env python3
"""FENVIX P1 - prova delle letture fredde su Atlas Infinite (versione p1-1.6.1, giro finale su M30).

Il comando del giro finale e' 'giro' (vedi GIRO-FINALE.md): tutte le celle di MATRICE-COMPLETA.md in un comando,
ogni fase salvata appena finisce, ripresa con --da-fase, clienti segnati su file prima di ogni lettura fredda.

Si lancia dal computer del fondatore. La stringa di connessione si legge SOLO dalla variabile
d'ambiente FENVIX_P1_URI e non viene mai stampata ne' scritta su file. Le chiavi dell'Atlas Admin API
(facoltative, sola lettura) si leggono SOLO da ATLAS_PUBLIC_KEY e ATLAS_PRIVATE_KEY, mai scritte.

Comandi (vedi GIRO-FINALE.md):
  controlla     si collega, misura la rete (hello), non scrive niente
  carica        crea 3 collezioni con indici e carica il carico misto di M26 (INVARIATO dalla p1-1.0)
  misura        il giro a fasi separate (Q2, Q3, Q1d, Q4, Q5, poi explain a campione), con profiler,
                serverStatus ogni 30 s, eventi del driver; salva tutto grezzo in una cartella nuova
  latenza       300 insert_one da circa 2 KB con w=1 e 300 con w="majority", hello prima di ognuna
  scrivi        scritture singole da circa 2 KB per 8 minuti (B23); con --per-cliente su una collezione
                clustered con _id per cliente (revisore D1)
  composizione  di cosa sono fatti i GB: dati, indici, oplog (solo numeri di controllo)
  scarica_atlas tutto quello che l'Atlas Admin API espone (metriche, log, eventi, fatture), sola lettura
  fredda        collezione con storageTier freddo (archive_events): se Atlas la accetta, copia circa 1 GiB di eventi,
                aspetta 10 minuti, letture fredde con il profiler, prova un aggiornamento, poi la cancella
  riassunto     riassunto dei giri vecchi (file di stato) o, con --cartella, di un giro p1-1.5 dai dati grezzi
  tutto, pulisci, estese: vedi sotto (estese e' TOLTO: da' solo un errore)

Dipendenze: python 3.9+, pymongo 4.x (pip install "pymongo>=4.6"). Facoltativo: backports.zstd.
"""
import argparse
import bisect
import datetime as dt
import hashlib
import json
import os
import platform
import random
import re
import shutil
import signal
import statistics
import sys
import threading
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
import zlib
from collections import defaultdict

try:
    import bson
    import pymongo
    from bson import ObjectId, json_util
    from bson.codec_options import CodecOptions
    from bson.raw_bson import RawBSONDocument
    from bson.son import SON
    from pymongo import InsertOne, MongoClient, ReadPreference, monitoring
    from pymongo.errors import PyMongoError, BulkWriteError
    from pymongo.read_concern import ReadConcern
    from pymongo.write_concern import WriteConcern
except ImportError:
    print("Manca pymongo. Installa con:  python3 -m pip install 'pymongo>=4.6' backports.zstd")
    sys.exit(2)

VERSIONE = "p1-1.6.2"  # p1-1.6.1 (correzioni dei revisori A e B) piu' una correzione del revisore finale (2026-11-05)
ENV_URI = "FENVIX_P1_URI"
DB_NOME_DEFAULT = "fenvix_p1"  # database esistente del carico: resta com'e' (prompt p1-1.5)
FILE_DEFAULT = "risultati_p1.json"
APPNAME = "app-demo"  # nome neutro (RULES: prove sui concorrenti)
MB = 1 << 20
GB = 1 << 30

# Carico misto di M26 (fenvix-engine, branch lavaggio-per-cliente, proto/lavaggio-per-cliente/src/main.rs):
# 50% messaggi (150-450 byte), 35% eventi di dispositivo (120-280), 15% ordini (400-800).
MIX = (("messaggi", 0.50), ("eventi", 0.85), ("ordini", 1.00))
T_INIZIO = dt.datetime(2025, 11, 1, tzinfo=dt.timezone.utc)
T_SPAN_S = 365 * 86400  # un anno di tempo simulato
FINE_DORMIENTI = 0.6  # i clienti riservati smettono di scrivere al 60% del caricamento

STOP = threading.Event()


# ----------------------------------------------------------------------------- utilita'

def ora():
    return dt.datetime.now(dt.timezone.utc)


def iso(t):
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def iso_us(t=None):
    """Ora UTC con i microsecondi."""
    t = t or ora()
    return t.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def iso_ns(ns):
    return iso_us(dt.datetime.fromtimestamp(ns / 1e9, dt.timezone.utc))


def _segreti():
    """Stringhe da non scrivere mai: stringa di connessione, utente, password, chiavi Atlas."""
    out = []
    uri = os.environ.get(ENV_URI, "")
    if len(uri) >= 12:
        out.append((uri, "<URI>"))
        m = re.match(r"mongodb(\+srv)?://([^@/]+)@", uri)
        if m:
            cred = m.group(2)
            out.append((cred, "<credenziali>"))
            for parte in cred.split(":"):
                if len(parte) >= 4:
                    out.append((parte, "<x>"))
                    dec = urllib.parse.unquote(parte)
                    if dec != parte and len(dec) >= 4:
                        out.append((dec, "<x>"))
    for k in ("ATLAS_PUBLIC_KEY", "ATLAS_PRIVATE_KEY"):
        v = os.environ.get(k, "")
        if len(v) >= 4:
            out.append((v, "<chiave>"))
    return out


EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}")


def scrub(s):
    """Toglie segreti ed email da un testo che va su file (i nomi delle macchine del cluster restano)."""
    for v, sost in _segreti():
        s = s.replace(v, sost)
    s = re.sub(r"mongodb(\+srv)?://[^\s'\"]+", "<URI>", s)
    return EMAIL_RE.sub("<email>", s)


def pulito(msg):
    """Toglie dai messaggi d'errore qualsiasi stringa di connessione, utente, password o chiave."""
    s = scrub(str(msg))
    s = re.sub(r"[\w.-]+\.mongodb(-dev)?\.net(:\d+)?", "<host>", s)  # nomi delle macchine del cluster
    return s[:500]


def stampa(*a):
    print(f"[{dt.datetime.now().strftime('%H:%M:%S')}]", *a, flush=True)


def percentile(xs, p):
    if not xs:
        return None
    s = sorted(xs)
    k = (len(s) - 1) * p / 100.0
    f = int(k)
    c = min(f + 1, len(s) - 1)
    return s[f] + (s[c] - s[f]) * (k - f)


def stat(xs):
    xs = [x for x in xs if x is not None]
    if not xs:
        return {"n": 0}
    return {
        "n": len(xs),
        "p50": round(percentile(xs, 50), 3),
        "p95": round(percentile(xs, 95), 3),
        "p99": round(percentile(xs, 99), 3),
        "media": round(statistics.fmean(xs), 3),
        "min": round(min(xs), 3),
        "max": round(max(xs), 3),
    }


def compressori():
    comp = ["zlib"]
    try:  # pymongo usa backports.zstd (python < 3.14), compression.zstd (3.14+) o zstandard (pymongo vecchi)
        import pymongo.compression_support as _cs
        ha = getattr(_cs, "_have_zstd", None) or getattr(_cs, "have_zstd", None)
        if ha and ha():
            comp = ["zstd", "zlib"]
    except Exception:
        pass
    return comp


def opzioni_nodo(host):
    """p1-1.6.1 (L49, L50): le opzioni per un client collegato direttamente a UN nodo (host:porta), con le stesse
    credenziali e lo stesso TLS della stringa di connessione, mai scritte. Serve per il profiler del secondario
    (il profiler e' per nodo)."""
    from pymongo import uri_parser
    uri = os.environ[ENV_URI]
    p = uri_parser.parse_uri(uri)
    op = {k.lower(): v for k, v in (p.get("options") or {}).items()}
    kw = {"host": host, "directConnection": True}
    if p.get("username"):
        kw["username"] = p["username"]
        kw["password"] = p.get("password")
        kw["authSource"] = op.get("authsource") or "admin"
        if op.get("authmechanism"):
            kw["authMechanism"] = op["authmechanism"]
    tls = op.get("tls", op.get("ssl"))
    if tls is None:
        tls = uri.startswith("mongodb+srv://")
    kw["tls"] = bool(tls)
    return kw


def connetti(pool=4, ascoltatori=None, esci=True, nodo=None):
    uri = os.environ.get(ENV_URI)
    if not uri:
        print(f"Manca la variabile d'ambiente {ENV_URI}. Vedi GIRO-FINALE.md.")
        sys.exit(2)
    try:
        dove = opzioni_nodo(nodo) if nodo else {"host": uri}
        c = MongoClient(**dove, appname=APPNAME, compressors=compressori(), maxPoolSize=pool,
                        serverSelectionTimeoutMS=30000, connectTimeoutMS=20000,
                        socketTimeoutMS=600000, retryWrites=True, w=1,
                        event_listeners=ascoltatori or [])
        c.admin.command("ping")
        return c
    except PyMongoError as e:
        print("Connessione non riuscita:", pulito(e))
        print("Controlla: IP del tuo computer nella lista di accesso, utente e password, rete.")
        if esci:
            sys.exit(3)
        raise


class Stato:
    """File JSON con stato e risultati. Non contiene mai la stringa di connessione."""

    def __init__(self, path):
        self.path = path
        self.d = {}
        if os.path.exists(path):
            with open(path) as f:
                self.d = json.load(f)

    def salva(self):
        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(self.d, f, indent=1, default=str)
        os.replace(tmp, self.path)


def ferma(*_):
    if not STOP.is_set():
        stampa("Fermata richiesta: chiudo e salvo (ci vuole qualche secondo)...")
    STOP.set()


def segnali_giro(cartella_log):
    """Ctrl+C, kill e (p1-1.6.1, E8 di B) la finestra del Terminale chiusa (SIGHUP): tutti salvano e si fermano.
    Con SIGHUP il terminale non c'e' piu': le stampe vanno in un file, cosi' il salvataggio non si rompe."""
    signal.signal(signal.SIGINT, ferma)
    signal.signal(signal.SIGTERM, ferma)

    def hup(*_):
        try:
            f = open(os.path.join(cartella_log, "terminale_chiuso.log"), "a")
            sys.stdout = sys.stderr = f
        except Exception:
            pass
        ferma()
    if hasattr(signal, "SIGHUP"):
        signal.signal(signal.SIGHUP, hup)


class RegistroUsati:
    """p1-1.6.1 (A2 di A, E8 di B): ogni cliente si segna su un file di sole aggiunte PRIMA della lettura fredda,
    con flush e fsync. Se il Mac si spegne di colpo nessun cliente letto torna 'mai letto' (meglio un cliente
    sprecato che una lettura calda contata come fredda). Il file si rilegge all'avvio di 'giro'."""

    def __init__(self, file_stato):
        self.path = file_stato + ".usati.log"
        self.lock = threading.Lock()
        self.f = None

    def leggi(self):
        a, g = set(), set()
        if os.path.exists(self.path):
            with open(self.path) as f:
                for riga in f:
                    p = riga.split()
                    if len(p) >= 2 and p[1].lstrip("-").isdigit():
                        (g if p[0] == "g" else a).add(int(p[1]))
        return a, g

    def scrivi(self, voci):
        """voci: lista di (tipo, i), tipo 'a' (carico di ieri) o 'g' (documenti da 2 KB)."""
        if not voci:
            return
        with self.lock:
            if self.f is None:
                self.f = open(self.path, "a")
            self.f.write("".join(f"{t} {i}\n" for t, i in voci))
            self.f.flush()
            os.fsync(self.f.fileno())

    def chiudi(self):
        with self.lock:
            if self.f:
                self.f.close()
                self.f = None


def dorme(secondi):
    fine = time.time() + secondi
    while not STOP.is_set() and time.time() < fine:
        time.sleep(min(5, max(0, fine - time.time())))


# ----------------------------------------------------------------------------- documenti

PAROLE = (
    "ciao grazie ordine spedizione account password profilo pagamento carta fattura reso rimborso prodotto taglia "
    "colore consegna corriere indirizzo telefono assistenza problema errore app aggiornamento versione schermo "
    "batteria sensore temperatura dispositivo allarme notifica promemoria calendario riunione progetto documento "
    "the order shipping refund payment invoice product delivery address support issue update version device "
    "battery sensor alarm reminder meeting project document agent tool search result summary please thanks "
    "domani oggi ieri settimana mese prezzo sconto codice cliente carrello magazzino disponibile richiesta "
    "conferma annulla modifica salva invia ricevuto letto risposta domanda analisi report grafico dati utente"
).split()
_SILL = ("ba be bi bo bu ca ce ci co cu da de di do du fa fe fi fo fu ga ge gi go gu la le li lo lu ma me mi mo "
         "mu na ne ni no nu pa pe pi po pu ra re ri ro ru sa se si so su ta te ti to tu va ve vi vo vu za ze zi "
         "tra tre pro con per str ch gn sc").split()
_r = random.Random(12345)
# vocabolario: parole vere piu' circa 6.000 parole inventate con frequenza zipf, come un testo vero
VOCAB = PAROLE + ["".join(_r.choice(_SILL) for _ in range(_r.randint(2, 4))) for _ in range(6000)]
CUM_VOCAB = []
_acc = 0.0
for _k in range(len(VOCAB)):
    _acc += 1.0 / (_k + 1) ** 0.9
    CUM_VOCAB.append(_acc)
RUOLI = ("user", "assistant", "tool")
MODELLI = ("small-2026", "medium-2026", "large-2026")
TIPI_EV = ("lettura", "stato", "allarme", "batteria", "posizione", "avvio")
STATI = ("nuovo", "pagato", "spedito", "consegnato", "reso")
CITTA = ("Milano", "Roma", "Torino", "Napoli", "Bologna", "Firenze", "Berlin", "Paris", "Madrid", "Lyon")


def testo(rng, n):
    """Testo con parole a frequenza zipf e qualche numero: si comprime circa 2-4 volte, come un testo vero."""
    parole = rng.choices(VOCAB, cum_weights=CUM_VOCAB, k=n // 5 + 2)
    if rng.random() < 0.5:
        parole[rng.randrange(len(parole))] = str(rng.randrange(10 ** 6))
    return " ".join(parole)[:n]


def doc_messaggio(rng, cli, ts):
    return {"cliente": cli, "ts": ts, "conv": f"conv-{rng.randrange(40)}", "ruolo": RUOLI[rng.randrange(3)],
            "modello": MODELLI[rng.randrange(3)], "token": rng.randrange(20, 2000),
            "testo": testo(rng, rng.randrange(60, 340))}


def doc_evento(rng, cli, ts):
    return {"cliente": cli, "ts": ts, "disp": f"dev-{rng.randrange(8)}", "tipo": TIPI_EV[rng.randrange(6)],
            "fw": "1.4.2", "valori": {"temp": round(rng.uniform(15, 40), 2), "batt": rng.randrange(101),
                                      "rssi": -rng.randrange(30, 95), "ok": rng.random() < 0.97},
            "nota": testo(rng, rng.randrange(10, 120))}


def doc_ordine(rng, cli, ts):
    righe = [{"sku": f"SKU-{rng.randrange(5000):05d}", "q": rng.randrange(1, 4),
              "prezzo": round(rng.uniform(3, 250), 2), "desc": testo(rng, rng.randrange(20, 60))}
             for _ in range(rng.randrange(1, 5))]
    return {"cliente": cli, "ts": ts, "stato": STATI[rng.randrange(5)], "righe": righe,
            "totale": round(sum(r["prezzo"] * r["q"] for r in righe), 2), "valuta": "EUR",
            "indirizzo": {"via": testo(rng, 24), "citta": CITTA[rng.randrange(len(CITTA))],
                          "cap": f"{rng.randrange(10000, 99999)}"},
            "note": testo(rng, rng.randrange(40, 200))}


GEN = {"messaggi": doc_messaggio, "eventi": doc_evento, "ordini": doc_ordine}


def nome_cliente(i):
    return f"cli-{i:06d}"


# ----------------------------------------------------------------------------- caricamento

def prepara_clienti(rng, n, mb_cliente, quota_riservati):
    """Taglie dei clienti intorno a mb_cliente (lognormale, 0,25x-4x) e scelta dei clienti dormienti riservati."""
    taglie = [min(4.0, max(0.25, rng.lognormvariate(0, 0.5))) * mb_cliente for _ in range(n)]
    idx = list(range(n))
    rng.shuffle(idx)
    nris = int(n * quota_riservati)
    riservati = set(idx[:nris])
    return taglie, riservati


def cumulati(pesi):
    out, s = [], 0.0
    for w in pesi:
        s += w
        out.append(s)
    return out


def cmd_carica(a, stato=None):
    signal.signal(signal.SIGINT, ferma)
    signal.signal(signal.SIGTERM, ferma)
    st = stato or Stato(a.file)
    if st.d.get("caricamento", {}).get("finito") and not a.forza:
        stampa("Il caricamento risulta gia' finito in", a.file, "- uso quello (per rifarlo: --forza).")
        return st
    client = connetti(pool=a.thread + 2)
    db = client[a.db]
    rng = random.Random(a.seme)
    obiettivo = int(a.gb * GB)
    n = max(50, int(a.gb * 1024 / a.mb_cliente))
    taglie, riservati = prepara_clienti(rng, n, a.mb_cliente, a.quota_riservati)
    clienti = [nome_cliente(i) for i in range(n)]
    # fase 1: tutti, i dormienti pesano di piu' perche' scrivono solo nel primo 60%
    cum1 = cumulati([taglie[i] / (FINE_DORMIENTI if i in riservati else 1.0) for i in range(n)])
    attivi2 = [i for i in range(n) if i not in riservati]
    cum2 = cumulati([taglie[i] for i in attivi2])

    if a.forza:
        stampa("Cancello il database della prova precedente...")
        for tentativo in range(5):
            try:
                client.drop_database(a.db)
                break
            except PyMongoError as e:
                stampa("dropDatabase non riuscito, riprovo:", pulito(e))
                time.sleep(5)
        st.d = {}
    for c in ("messaggi", "eventi", "ordini"):
        db[c].create_index([("cliente", 1), ("ts", -1)], name="cliente_ts")
    db["ordini"].create_index([("stato", 1), ("ts", -1)], name="stato_ts")
    info = client.admin.command("buildInfo")
    st.d.update({
        "versione_script": VERSIONE, "db": a.db,
        "server": {"versione": info.get("version"), "modules": info.get("modules")},
        "config": {"gb": a.gb, "mb_cliente": a.mb_cliente, "clienti": n, "riservati": len(riservati),
                   "thread": a.thread, "lotto": a.lotto, "seme": a.seme, "tetto_logico_gb": a.tetto_logico_gb},
        "caricamento": {"inizio": iso(ora()), "finito": False},
    })
    # biglietti per Q2: per ogni cliente l'_id di un ordine scelto a caso (reservoir), e quanti ordini ha
    q2_id = [None] * n
    n_ord = [0] * n
    byte_cli = [0] * n
    conta = {"messaggi": 0, "eventi": 0, "ordini": 0}
    lock = threading.Lock()
    prog = {"byte": 0, "doc": 0, "errori": 0, "bloccato": None, "ultimo_ts": None}
    t0 = time.time()
    scadenza = t0 + a.max_ore_carico * 3600

    def lotto(trng):
        with lock:
            p = prog["byte"] / obiettivo
        p = min(p, 0.9999)
        fase1 = p < FINE_DORMIENTI
        per = {"messaggi": [], "eventi": [], "ordini": []}
        meta = []
        base_ts = T_INIZIO.timestamp() + p * T_SPAN_S
        for _ in range(a.lotto):
            if fase1:
                i = bisect.bisect_left(cum1, trng.random() * cum1[-1])
            else:
                i = attivi2[bisect.bisect_left(cum2, trng.random() * cum2[-1])]
            i = min(i, n - 1)
            x = trng.random()
            coll = "messaggi" if x < 0.5 else ("eventi" if x < 0.85 else "ordini")
            ts = dt.datetime.fromtimestamp(base_ts + trng.uniform(0, 600), dt.timezone.utc)
            d = GEN[coll](trng, clienti[i], ts)
            d["_id"] = ObjectId()
            raw = bson.encode(d)
            per[coll].append(RawBSONDocument(raw))
            meta.append((i, coll, d, len(raw)))
        return per, meta

    def lavoratore(k):
        trng = random.Random(a.seme * 1000 + k)
        errori_di_fila = 0
        while not STOP.is_set():
            with lock:
                if prog["byte"] >= obiettivo or prog["bloccato"]:
                    return
            if time.time() > scadenza:
                with lock:
                    prog["bloccato"] = prog["bloccato"] or "tetto di durata del caricamento"
                return
            per, meta = lotto(trng)
            nbyte = sum(m[3] for m in meta)  # dimensione BSON vera dei documenti del lotto
            ok = True
            for coll, docs in per.items():
                if not docs:
                    continue
                try:
                    db[coll].insert_many(docs, ordered=False)
                except BulkWriteError as e:
                    det = e.details or {}
                    errs = det.get("writeErrors", [])
                    if errs and all(x.get("code") == 11000 for x in errs):
                        continue  # chiave doppia dopo un nuovo tentativo: va bene
                    ok = False
                    msg = pulito(errs[0].get("errmsg") if errs else e)
                except PyMongoError as e:
                    ok = False
                    msg = pulito(e)
                if not ok:
                    break
            if not ok:
                errori_di_fila += 1
                with lock:
                    prog["errori"] += 1
                stampa(f"thread {k}: scrittura non riuscita ({errori_di_fila} di fila): {msg}")
                if errori_di_fila >= 5:
                    with lock:
                        prog["bloccato"] = prog["bloccato"] or f"scritture rifiutate: {msg}"
                    return
                time.sleep(10 * errori_di_fila)
                continue
            errori_di_fila = 0
            with lock:
                for i, coll, d, s in meta:
                    byte_cli[i] += s
                    conta[coll] += 1
                    if coll == "ordini":
                        n_ord[i] += 1
                        if trng.random() < 1.0 / n_ord[i]:
                            q2_id[i] = d["_id"]
                prog["byte"] += nbyte
                prog["doc"] += len(meta)
                prog["ultimo_ts"] = meta[-1][2]["ts"]

    def salva_checkpoint(finito=False, motivo=None):
        with lock:
            car = st.d["caricamento"]
            car.update({"byte_bson": prog["byte"], "documenti": prog["doc"], "per_collezione": dict(conta),
                        "errori": prog["errori"], "secondi": round(time.time() - t0, 1),
                        "mb_s": round(prog["byte"] / MB / max(1e-9, time.time() - t0), 3)})
            if finito:
                car.update({"finito": True, "fine": iso(ora()), "motivo_fine": motivo})
            st.d["clienti"] = {
                "nomi_da": "cli-%06d", "n": n,
                "riservati": sorted(riservati),
                "byte": byte_cli,
                "q2_id": [str(x) if x is not None else None for x in q2_id],
                "fine_attivita": {"riservati": FINE_DORMIENTI, "altri": 1.0},
            }
        st.salva()

    stampa(f"Carico {a.gb:g} GB di documenti BSON, {n} clienti da circa {a.mb_cliente:g} MB "
           f"({len(riservati)} dormienti riservati alle letture fredde), {a.thread} thread.")
    ths = [threading.Thread(target=lavoratore, args=(k,), daemon=True) for k in range(a.thread)]
    for t in ths:
        t.start()
    ultimo_stat = 0.0
    ultimo_print = 0.0
    ultimo_ck = time.time()
    st.d["caricamento"]["campioni_dbstats"] = []
    while any(t.is_alive() for t in ths):
        time.sleep(2)
        el = time.time() - t0
        with lock:
            b, d_, bl = prog["byte"], prog["doc"], prog["bloccato"]
        vel = b / max(el, 1e-9)
        eta = (obiettivo - b) / vel if vel > 0 else float("inf")
        if time.time() - ultimo_print >= 30:
            ultimo_print = time.time()
            stampa(f"caricati {b / GB:6.2f} di {a.gb:g} GB ({100 * b / obiettivo:5.1f}%), {d_:,} documenti, "
                   f"{vel / MB:5.2f} MB/s, fine stimata tra {eta / 60:6.1f} minuti")
        if time.time() - ultimo_stat > a.ogni_dbstats_s:
            ultimo_stat = time.time()
            try:
                s = db.command("dbStats")
                logico = (s.get("dataSize", 0) + s.get("indexSize", 0)) / GB
                camp = {"t": iso(ora()), "dataSize_gb": round(s.get("dataSize", 0) / GB, 3),
                        "indexSize_gb": round(s.get("indexSize", 0) / GB, 3),
                        "storageSize_gb": round(s.get("storageSize", 0) / GB, 3)}
                st.d["caricamento"]["campioni_dbstats"].append(camp)
                stampa(f"dbStats: dati {camp['dataSize_gb']} GB + indici {camp['indexSize_gb']} GB "
                       f"= {logico:.2f} GB logici (tetto di sicurezza {a.tetto_logico_gb:g} GB)")
                if logico >= a.tetto_logico_gb:
                    with lock:
                        prog["bloccato"] = f"tetto logico di sicurezza {a.tetto_logico_gb} GB raggiunto"
            except PyMongoError as e:
                stampa("dbStats non riuscito:", pulito(e))
        if time.time() - ultimo_ck > 300:
            ultimo_ck = time.time()
            salva_checkpoint()
    with lock:
        motivo = prog["bloccato"] or ("fermato a mano" if STOP.is_set() else "obiettivo raggiunto")
    salva_checkpoint(finito=True, motivo=motivo)
    try:
        s = db.command("dbStats")
        st.d["caricamento"]["dbstats_fine"] = {k: s.get(k) for k in
                                               ("collections", "objects", "dataSize", "indexSize", "storageSize")}
        st.salva()
    except PyMongoError:
        pass
    car = st.d["caricamento"]
    stampa(f"Caricamento finito ({motivo}): {car['byte_bson'] / GB:.2f} GB, {car['documenti']:,} documenti, "
           f"{car['secondi'] / 3600:.2f} ore, {car['mb_s']} MB/s.")
    client.close()
    return st


# ----------------------------------------------------------------------------- raccolta dei dati grezzi (p1-1.5)

JOPT = json_util.JSONOptions(json_mode=json_util.JSONMode.RELAXED,
                             datetime_representation=json_util.DatetimeRepresentation.ISO8601)
RAWOPT = CodecOptions(document_class=RawBSONDocument, tz_aware=True, tzinfo=dt.timezone.utc)


def scrivi_json(path, obj):
    """Scrive un file JSON completo (tipi BSON compresi), senza segreti ne' email."""
    s = scrub(json_util.dumps(obj, json_options=JOPT, indent=1))
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        f.write(s)
    os.replace(tmp, path)


def leggi_json(path):
    with open(path) as f:
        return json_util.loads(f.read())


def ms_da(t0):
    return (time.perf_counter_ns() - t0) / 1e6


def hello_ms(client):
    t0 = time.perf_counter_ns()
    client.admin.command("hello")
    return ms_da(t0)


def _prova(fn):
    try:
        return fn()
    except Exception as e:  # su Infinite molti comandi possono essere vietati: si scrive e si va avanti
        return {"errore": pulito(e), "tipo_errore": type(e).__name__}


class Registro:
    """Eventi del driver pymongo: comandi (durata, nome, server), pool di connessioni (CMAP), server, topologia,
    battiti di controllo. Le funzioni chiamate dal driver fanno solo un append (costo di pochi microsecondi)."""

    def __init__(self, nome):
        self.nome = nome
        self.comandi, self.pool, self.topologia = [], [], []
        self.getmore = defaultdict(int)
        reg = self

        def th():
            return threading.current_thread().name

        def ind(x):
            return f"{x[0]}:{x[1]}" if isinstance(x, tuple) and len(x) == 2 else str(x)

        class Com(monitoring.CommandListener):
            def started(self, e):
                if e.command_name == "getMore":
                    reg.getmore[threading.get_ident()] += 1
                try:
                    c = e.command.get("comment")
                except Exception:
                    c = None
                reg.comandi.append(("inizio", time.time_ns(), e.command_name, e.request_id, e.operation_id,
                                    e.database_name, e.connection_id, getattr(e, "server_connection_id", None),
                                    th(), c, None, None))

            def succeeded(self, e):
                reg.comandi.append(("fatto", time.time_ns(), e.command_name, e.request_id, e.operation_id,
                                    getattr(e, "database_name", None), e.connection_id,
                                    getattr(e, "server_connection_id", None), th(), None, e.duration_micros, None))

            def failed(self, e):
                reg.comandi.append(("errore", time.time_ns(), e.command_name, e.request_id, e.operation_id,
                                    getattr(e, "database_name", None), e.connection_id,
                                    getattr(e, "server_connection_id", None), th(), None, e.duration_micros,
                                    pulito(e.failure)))

        class Pool(monitoring.ConnectionPoolListener):
            def _r(self, tipo, e):
                reg.pool.append((tipo, time.time_ns(), ind(getattr(e, "address", "")),
                                 getattr(e, "connection_id", None), getattr(e, "reason", None),
                                 getattr(e, "duration", None), th()))

            def pool_created(self, e): self._r("pool_creato", e)
            def pool_ready(self, e): self._r("pool_pronto", e)
            def pool_cleared(self, e): self._r("pool_svuotato", e)
            def pool_closed(self, e): self._r("pool_chiuso", e)
            def connection_created(self, e): self._r("connessione_aperta", e)
            def connection_ready(self, e): self._r("connessione_pronta", e)
            def connection_closed(self, e): self._r("connessione_chiusa", e)
            def connection_check_out_started(self, e): self._r("attesa_iniziata", e)
            def connection_check_out_failed(self, e): self._r("attesa_fallita", e)
            def connection_checked_out(self, e): self._r("connessione_presa", e)
            def connection_checked_in(self, e): self._r("connessione_resa", e)

        def desc(d):
            if d is None:
                return None
            rtt = getattr(d, "round_trip_time", None)
            return {"tipo": getattr(d, "server_type_name", None), "rtt_ms": rtt * 1000 if rtt else None,
                    "errore": pulito(d.error) if getattr(d, "error", None) else None}

        class Srv(monitoring.ServerListener):
            def opened(self, e): reg.topologia.append(("server_aperto", time.time_ns(), ind(e.server_address), None))
            def closed(self, e): reg.topologia.append(("server_chiuso", time.time_ns(), ind(e.server_address), None))

            def description_changed(self, e):
                reg.topologia.append(("server_cambiato", time.time_ns(), ind(e.server_address),
                                      {"prima": desc(e.previous_description), "dopo": desc(e.new_description)}))

        class Topo(monitoring.TopologyListener):
            def opened(self, e): reg.topologia.append(("topologia_aperta", time.time_ns(), None, None))
            def closed(self, e): reg.topologia.append(("topologia_chiusa", time.time_ns(), None, None))

            def description_changed(self, e):
                n = e.new_description
                reg.topologia.append(("topologia_cambiata", time.time_ns(), None, {
                    "tipo": n.topology_type_name,
                    "server": {ind(k): desc(v) for k, v in n.server_descriptions().items()}}))

        class Battito(monitoring.ServerHeartbeatListener):
            def started(self, e): pass

            def succeeded(self, e):
                reg.topologia.append(("battito", time.time_ns(), ind(e.connection_id),
                                      {"ms": e.duration * 1000, "in_attesa": getattr(e, "awaited", None)}))

            def failed(self, e):
                reg.topologia.append(("battito_fallito", time.time_ns(), ind(e.connection_id),
                                      {"ms": e.duration * 1000, "errore": pulito(e.reply)}))

        self.ascoltatori = [Com(), Pool(), Srv(), Topo(), Battito()]

    def lunghezze(self):
        return len(self.comandi), len(self.pool), len(self.topologia)

    def esporta(self, da=(0, 0, 0), a=None):
        a = a or self.lunghezze()
        chiavi_c = ("evento", "t", "comando", "request_id", "operation_id", "db", "server", "connessione_server",
                    "thread", "commento", "durata_us", "errore")
        com = []
        for x in self.comandi[da[0]:a[0]]:
            r = dict(zip(chiavi_c, x))
            r["t"] = iso_ns(r["t"])
            r["server"] = f"{r['server'][0]}:{r['server'][1]}" if isinstance(r["server"], tuple) else str(r["server"])
            com.append(r)
        pool = []
        for x in self.pool[da[1]:a[1]]:
            r = dict(zip(("evento", "t", "server", "connessione", "motivo", "durata_s", "thread"), x))
            r["t"] = iso_ns(r["t"])
            r["motivo"] = str(r["motivo"]) if r["motivo"] is not None else None
            pool.append(r)
        top = []
        for x in self.topologia[da[2]:a[2]]:
            r = dict(zip(("evento", "t", "server", "dettagli"), x))
            r["t"] = iso_ns(r["t"])
            top.append(r)
        return {"client": self.nome, "comandi": com, "pool": pool, "topologia": top}


def doc_2kb(rng, cliente):
    return {"cliente": cliente, "ts": ora(), "conv": f"conv-{rng.randrange(40)}",
            "ruolo": RUOLI[rng.randrange(3)], "modello": MODELLI[rng.randrange(3)],
            "token": rng.randrange(20, 2000), "testo": testo(rng, rng.randrange(1700, 1900))}


class Giro:
    """Una sessione di misura: cartella dei dati, due client (misure e controllo), eventi, serverStatus ogni 30 s."""

    def __init__(self, a, tipo, nome, cartella=None, nodo=None):
        self.a = a
        self.nome = nome
        self.nodo = nodo  # p1-1.6.1: None = il replica set; "host:porta" = un nodo solo (il secondario, L49 e L50)
        nome_pulito = re.sub(r"[^A-Za-z0-9_-]", "_", nome)
        # p1-1.6: con 'giro --da-fase' si riusa la cartella del giro interrotto
        self.cartella = cartella or os.path.join(a.cartella_dati,
                                                 f"{tipo}_{nome_pulito}_{ora().strftime('%Y%m%dT%H%M%SZ')}")
        os.makedirs(self.cartella, exist_ok=True)
        self.reg = Registro("misure" + (f"@{nodo}" if nodo else ""))
        self.reg_c = Registro("controllo" + (f"@{nodo}" if nodo else ""))
        self.client = connetti(pool=2, ascoltatori=self.reg.ascoltatori, esci=nodo is None, nodo=nodo)
        self.mon = connetti(pool=2, ascoltatori=self.reg_c.ascoltatori, esci=nodo is None, nodo=nodo)
        self.db = self.client[a.db]
        self.dbc = self.mon[a.db]
        self.fase = "inizio"
        self.campioni = []
        self._fermo = threading.Event()
        self._th = None
        self.profilo_acceso = False
        self.segna = None  # p1-1.6: chiamata dopo ogni misura (segna il cliente come letto)
        self.prenota = None  # p1-1.6.1: chiamata PRIMA della lettura fredda (segna il cliente su file, con fsync)

    # serverStatus completo ogni a.ogni_s secondi, in un thread a parte con il client di controllo
    def avvia_campioni(self):
        def gira():
            while not self._fermo.is_set():
                t0 = time.time_ns()
                r = _prova(lambda: self.mon.admin.command("serverStatus"))
                self.campioni.append({"t": iso_ns(t0), "fase": self.fase, "serverStatus": r})
                self._fermo.wait(self.a.ogni_s)
        self._th = threading.Thread(target=gira, name="serverStatus", daemon=True)
        self._th.start()

    def ferma_campioni(self):
        self._fermo.set()
        if self._th:
            self._th.join(timeout=60)

    def raw(self, nome):
        return self.db.get_collection(nome, codec_options=RAWOPT)

    def ambiente(self):
        e = {"versione_script": VERSIONE, "python": sys.version, "pymongo": pymongo.version,
             "pymongo_estensione_c": pymongo.has_c(), "bson_estensione_c": bson.has_c(),
             "sistema": platform.platform(), "macchina": platform.machine(), "processore": platform.processor(),
             "ora_locale": dt.datetime.now().astimezone().isoformat(), "ora_utc": iso_us(),
             "fuso": list(time.tzname), "scarto_utc_s": dt.datetime.now().astimezone().utcoffset().total_seconds(),
             "compressori_chiesti": compressori(), "appname": APPNAME, "db": self.a.db,
             "argomenti": {k: v for k, v in vars(self.a).items()}}
        adm = self.mon.admin
        e["buildInfo"] = _prova(lambda: adm.command("buildInfo"))
        e["hello"] = _prova(lambda: adm.command("hello"))
        e["hostInfo"] = _prova(lambda: adm.command("hostInfo"))
        e["getCmdLineOpts"] = _prova(lambda: adm.command("getCmdLineOpts"))
        e["getParameter"] = _prova(lambda: adm.command("getParameter", "*"))
        e["featureCompatibilityVersion"] = _prova(lambda: adm.command("getParameter", 1,
                                                                       featureCompatibilityVersion=1))
        e["serverStatus_compressione"] = _prova(lambda: adm.command("serverStatus").get("network", {}).get("compression"))
        # rete di base: 50 hello di riscaldamento, poi 200 misurati, ognuno con l'ora
        for _ in range(50):
            hello_ms(self.client)
        rete = []
        for _ in range(200):
            t0 = time.time_ns()
            rete.append({"t": iso_ns(t0), "ms": round(hello_ms(self.client), 4)})
        e["rete_hello"] = rete
        e["rete_hello_riassunto"] = stat([x["ms"] for x in rete])
        stampa(f"rete: hello p50 {e['rete_hello_riassunto']['p50']} ms, p95 {e['rete_hello_riassunto']['p95']}, "
               f"p99 {e['rete_hello_riassunto']['p99']} (media {e['rete_hello_riassunto']['media']})")
        return e

    def istantanea(self, etichetta):
        """hello, replSetGetStatus, hostInfo, serverStatus, dbStats, collStats, $collStats, $indexStats, oplog."""
        out = {"etichetta": etichetta, "inizio": iso_us(), "fase": self.fase}
        adm = self.mon.admin
        try:  # p1-1.6: se il cluster non risponde, un ping (30 s) invece di una ventina di comandi da 30 s l'uno
            adm.command("ping")
        except PyMongoError as e:
            out.update({"non_raggiungibile": pulito(e), "fine": iso_us()})
            return out
        out["hello"] = _prova(lambda: adm.command("hello"))
        out["replSetGetStatus"] = _prova(lambda: adm.command("replSetGetStatus"))
        out["hostInfo"] = _prova(lambda: adm.command("hostInfo"))
        out["serverStatus"] = _prova(lambda: adm.command("serverStatus"))
        out["dbStats"] = _prova(lambda: self.dbc.command("dbStats"))
        out["listCollections"] = _prova(lambda: list(self.dbc.list_collections()))
        nomi = _prova(lambda: sorted(self.dbc.list_collection_names()))
        out["collStats"], out["$collStats"], out["$indexStats"] = {}, {}, {}
        if isinstance(nomi, list):
            for c in nomi:
                out["collStats"][c] = _prova(lambda: self.dbc.command("collStats", c))
                out["$collStats"][c] = _prova(lambda: list(self.dbc[c].aggregate([{"$collStats": {
                    "storageStats": {}, "latencyStats": {"histograms": True}, "count": {}, "queryExecStats": {}}}])))
                if not c.startswith("system."):
                    out["$indexStats"][c] = _prova(lambda: list(self.dbc[c].aggregate([{"$indexStats": {}}])))
        out["oplog_collStats"] = _prova(lambda: self.mon["local"].command("collStats", "oplog.rs"))
        out["fine"] = iso_us()
        return out

    # ------------------------------------------------------------------ profiler
    def profiler_acceso(self):
        r = {"inizio": iso_us()}
        try:
            r["spento_prima"] = self.dbc.command("profile", 0)

            def via():
                self.dbc.drop_collection("system.profile")
                return "fatto"

            def crea():
                self.dbc.create_collection("system.profile", capped=True, size=int(self.a.profilo_mb * MB))
                return f"capped {self.a.profilo_mb:g} MB"
            r["drop_system_profile"] = _prova(via)
            r["crea_system_profile"] = _prova(crea)
            # livello 2 = tutte le operazioni del database, qualunque sia slowms (Atlas puo' gestire slowms da se':
            # con il livello 1 una soglia cambiata da Atlas toglierebbe dal profiler le ricerche veloci). slowms 0
            # resta per avere ogni operazione anche nel log. Se il livello 2 e' rifiutato: livello 1, slowms 0.
            try:
                r["acceso"] = self.dbc.command("profile", 2, slowms=0, sampleRate=1.0)
                r["livello"] = 2
            except PyMongoError as e2:
                r["livello_2_rifiutato"] = pulito(e2)
                r["acceso"] = self.dbc.command("profile", 1, slowms=0, sampleRate=1.0)
                r["livello"] = 1
            r["stato_dopo_accensione"] = _prova(lambda: self.dbc.command("profile", -1))
            self.profilo_acceso = True
        except PyMongoError as e:
            r["errore"] = pulito(e)
            self.profilo_acceso = False
            stampa(f"ATTENZIONE: il profiler non si accende ({pulito(e)}). Vado avanti senza: restano i tempi "
                   f"lato cliente e i grafici di Atlas.")
        r["fine"] = iso_us()
        return r

    def profiler_spento(self):
        if not self.profilo_acceso:
            return {"nota": "profiler non acceso"}
        r = {"inizio": iso_us()}
        r["stato_prima_di_spegnere"] = _prova(lambda: self.dbc.command("profile", -1))  # Atlas l'ha cambiato?
        r["spento"] = _prova(lambda: self.dbc.command("profile", 0, slowms=100))
        self.profilo_acceso = False
        r["fine"] = iso_us()
        return r

    def leggi_profilo(self, visti, docs):
        """Aggiunge a docs i documenti di system.profile non ancora letti (tutti i campi)."""
        try:
            ultimo = max((d["ts"] for d in docs if isinstance(d.get("ts"), dt.datetime)), default=None)
            filtro = {"ts": {"$gte": ultimo}} if ultimo else {}  # solo i nuovi (stesso ts: tolti con l'impronta)
            for d in self.dbc["system.profile"].find(filtro).sort("$natural", 1):
                k = hashlib.sha1(json_util.dumps(d, json_options=JOPT).encode()).hexdigest()
                if k not in visti:
                    visti.add(k)
                    docs.append(d)
            return None
        except PyMongoError as e:
            return pulito(e)

    # ------------------------------------------------------------------ una misura
    def trova(self, coll, spec, commento):
        tid = threading.get_ident()
        g0 = self.reg.getmore[tid]
        kw = {"limit": spec["limit"], "batch_size": spec["limit"], "comment": commento}
        if spec["sort"]:
            kw["sort"] = spec["sort"]
        t0s = time.time_ns()
        t0 = time.perf_counter_ns()
        docs = list(coll.find(spec["filtro"], **kw))
        ms = ms_da(t0)
        t1s = time.time_ns()
        return {"inizio": iso_ns(t0s), "fine": iso_ns(t1s), "ms": round(ms, 4), "doc": len(docs),
                "byte": sum(len(d.raw) for d in docs), "getmore": self.reg.getmore[tid] - g0,
                "commento": commento}, docs

    def hello_accoppiato(self):
        t0 = time.time_ns()
        return {"inizio": iso_ns(t0), "ms": round(hello_ms(self.client), 4)}

    def misura_find(self, fase, k, c):
        spec = c["spec"]
        m = {"fase": fase, "k": k, "cliente": c.get("cliente"), "i": c.get("i"), "gruppo": c.get("gruppo"),
             "ricerca": {"collezione": spec["coll"], "filtro": spec["filtro"], "ordine": spec["sort"],
                         "limit": spec["limit"]}, "inizio": iso_us(), "errori": [], "caldo": []}
        coll = self.raw(spec["coll"])
        if spec.get("rc"):  # p1-1.6.1, L51: readConcern majority
            coll = coll.with_options(read_concern=ReadConcern(spec["rc"]))
            m["ricerca"]["readConcern"] = spec["rc"]
        if spec.get("rp") == "secondary":  # p1-1.6.1, L49 e L50 di riserva: dal secondario via il replica set
            coll = coll.with_options(read_preference=ReadPreference.SECONDARY)
            m["ricerca"]["readPreference"] = "secondary"
        if self.nodo:
            m["nodo"] = self.nodo
        base = f"p1|{fase}|{c.get('cliente') or c.get('gruppo') or 'finestra'}|{k}"
        docs_f = docs_c = None
        try:
            m["hello_prima"] = self.hello_accoppiato()
            m["freddo"], docs_f = self.trova(coll, spec, base + "|freddo")
        except PyMongoError as e:
            m["errori"].append({"dove": "freddo", "t": iso_us(), "errore": pulito(e)})
        # p1-1.6: senza il freddo il caldo non serve (e con la rete caduta costerebbe altri 2 x 30 s per ricerca)
        for j in range(self.a.rip_caldo if docs_f is not None else 0):
            try:
                h = self.hello_accoppiato()
                r, d2 = self.trova(coll, spec, base + f"|caldo{j + 1}")
                r["hello_prima"] = h
                m["caldo"].append(r)
                if docs_c is None:
                    docs_c = d2
            except PyMongoError as e:
                m["errori"].append({"dove": f"caldo{j + 1}", "t": iso_us(), "errore": pulito(e)})
        if self.a.controllo and docs_f is not None and not c.get("senza_controllo"):
            try:
                m["controllo"] = self.controlla(spec, docs_f, docs_c)
            except PyMongoError as e:
                m["controllo"] = {"ok": None, "errore": pulito(e)}
        m["fine"] = iso_us()
        return m

    def controlla(self, spec, docs, docs_caldo):
        """Dopo le misure: i documenti sono quelli attesi? (filtro, ordine, quantita', nessuno saltato)."""
        out = {"ok": True, "motivi": []}

        def no(x):
            out["ok"] = False
            out["motivi"].append(x)

        ids = [d["_id"] for d in docs]
        if docs_caldo is not None and ids != [d["_id"] for d in docs_caldo]:
            no("il caldo ha restituito documenti diversi dal freddo")
        f = spec["filtro"]
        if "_id" in f:
            out["attesi"] = 1
            if len(docs) != 1 or ids[0] != f["_id"]:
                no(f"atteso 1 documento con l'_id chiesto, trovati {len(docs)}")
            return out
        for d in docs:
            for campo in ("cliente", "stato"):
                if campo in f and d[campo] != f[campo]:
                    no(f"{campo} sbagliato")
                    break
            t = d["ts"]
            cond = f.get("ts", {})
            if (("$gte" in cond and t < cond["$gte"]) or ("$gt" in cond and t <= cond["$gt"])
                    or ("$lt" in cond and t >= cond["$lt"]) or ("$lte" in cond and t > cond["$lte"])):
                no("documento fuori dalla finestra")
                break
        ts = [d["ts"] for d in docs]
        if ts != sorted(ts, reverse=True):
            no("ordine non decrescente")
        # count con limit: legge solo le voci d'indice che la find ha gia' letto (dal piu' nuovo, fino a limit).
        # Senza limit il count di una finestra Q5 leggeva tutta la finestra di 7 giorni, che contiene i 100 ordini
        # della finestra dopo (passo 5,97 giorni < 7): ne scaldava le pagine d'indice (revisore dello script).
        n_tot = self.dbc.command("count", spec["coll"], query=f, limit=spec["limit"])["n"]
        atteso = min(spec["limit"], n_tot)
        out.update({"attesi": atteso, "nel_filtro_fino_al_limit": n_tot, "trovati": len(docs)})
        if len(docs) != atteso:
            no(f"trovati {len(docs)} invece di {atteso}")
        if docs:
            f2 = dict(f)
            f2["ts"] = dict(f.get("ts", {}))
            f2["ts"]["$gt"] = min(ts)
            piu_nuovi = self.dbc.command("count", spec["coll"], query=f2, limit=len(docs))["n"]
            if piu_nuovi >= len(docs):
                no("saltati documenti piu' nuovi")
        if spec["coll"] in ("messaggi", "eventi") and spec["limit"] == 20 and len(docs) < 20:
            no("meno di 20 messaggi prima della data")
        if spec["coll"] == "events_large" and spec["limit"] == EVENTI_Q3_M31 and len(docs) < EVENTI_Q3_M31:
            no(f"meno di {EVENTI_Q3_M31} documenti prima della data (Q3 da 2 KB come M31)")
        return out

    def misura_spiega(self, fase, k, c):
        """explain executionStats completo (allPlansExecution): freddo e caldo, o solo piano se gia' letto."""
        spec = c["spec"]
        m = {"fase": fase, "k": k, "query": c["query"], "cliente": c.get("cliente"), "i": c.get("i"),
             "gruppo": c.get("gruppo"), "freddo_vero": c["freddo_vero"],
             "ricerca": {"collezione": spec["coll"], "filtro": spec["filtro"], "ordine": spec["sort"],
                         "limit": spec["limit"]}, "inizio": iso_us(), "errori": []}
        base = f"p1|spiega|{c['query']}|{c.get('cliente') or 'finestra'}|{k}"
        for tipo in (("freddo", "caldo") if c["freddo_vero"] else ("solo_piano",)):
            cmd = SON([("find", spec["coll"]), ("filter", spec["filtro"])])
            if spec["sort"]:
                cmd["sort"] = SON(spec["sort"])
            cmd["limit"] = spec["limit"]
            cmd["batchSize"] = spec["limit"]
            cmd["comment"] = f"{base}|{tipo}"
            try:
                h = self.hello_accoppiato()
                t0s = time.time_ns()
                t0 = time.perf_counter_ns()
                r = self.db.command("explain", cmd, verbosity="allPlansExecution")
                ms = ms_da(t0)
                es = r.get("executionStats", {})
                m[tipo] = {"inizio": iso_ns(t0s), "fine": iso_us(), "ms": round(ms, 4), "hello_prima": h,
                           "executionTimeMillis": es.get("executionTimeMillis"), "nReturned": es.get("nReturned"),
                           "totalKeysExamined": es.get("totalKeysExamined"),
                           "totalDocsExamined": es.get("totalDocsExamined"), "explain": r}
            except PyMongoError as e:
                m["errori"].append({"dove": tipo, "t": iso_us(), "errore": pulito(e)})
        m["fine"] = iso_us()
        return m

    # ------------------------------------------------------------------ una fase
    def esegui_fase(self, indice, fase, compiti, tipo="find", min_secondi=None):
        self.fase = fase
        rec = {"fase": fase, "versione": VERSIONE, "giro": self.nome, "n_compiti": len(compiti)}
        if self.nodo:
            rec["nodo_misurato"] = self.nodo
        stampa(f"Fase {fase}: istantanea prima (hello, replSetGetStatus, serverStatus, dbStats, collStats)...")
        rec["prima"] = self.istantanea(f"prima di {fase}")
        rec["profiler_acceso"] = self.profiler_acceso()
        da = (self.reg.lunghezze(), self.reg_c.lunghezze())
        c0 = len(self.campioni)
        t0 = time.time_ns()
        rec["inizio"] = iso_ns(t0)
        stampa(f"FASE {fase} INIZIO {dt.datetime.now().strftime('%H:%M:%S')} ora del Mac "
               f"({iso_ns(t0)[:19]}Z), {len(compiti)} ricerche")
        misure, visti, profilo, errori_profilo = [], set(), [], []
        intervallo = (self.a.min_secondi_fase if min_secondi is None else min_secondi) / max(1, len(compiti))
        p0 = time.perf_counter()
        di_fila = 0  # p1-1.6: misure senza risposta di fila (rete caduta, cluster fermo): oltre il tetto la fase si ferma
        try:
            for k, c in enumerate(compiti):
                if STOP.is_set():
                    break
                if self.prenota:  # p1-1.6.1: il cliente si segna su file PRIMA della lettura fredda
                    self.prenota([c])
                try:
                    m = self.misura_find(fase, k, c) if tipo == "find" else self.misura_spiega(fase, k, c)
                except Exception as e:  # errore inatteso (non del server): si scrive e si va avanti, la fase resta
                    m = {"fase": fase, "k": k, "cliente": c.get("cliente"), "i": c.get("i"), "gruppo": c.get("gruppo"),
                         "query": c.get("query"), "freddo_vero": c.get("freddo_vero"), "caldo": [],
                         "errori": [{"dove": "script", "t": iso_us(), "errore": pulito(e),
                                     "tipo_errore": type(e).__name__}]}
                    stampa(f"  {fase}: errore inatteso alla ricerca {k} ({type(e).__name__}), vado avanti")
                misure.append(m)
                if self.segna:
                    self.segna(m)
                riuscita = m.get("freddo") or any(x in m for x in ("caldo_explain", "freddo", "solo_piano"))
                di_fila = 0 if riuscita else di_fila + 1
                if di_fila >= self.a.tetto_errori_di_fila:
                    rec["fermata"] = f"{di_fila} misure di fila senza risposta: fase fermata"
                    stampa(f"ATTENZIONE: {rec['fermata']} (ultimo errore: {m['errori'][-1:] if m.get('errori') else '-'})")
                    break
                if (k + 1) % 25 == 0:
                    if self.profilo_acceso:
                        e = self.leggi_profilo(visti, profilo)
                        if e:
                            errori_profilo.append(e)
                    if tipo == "find":
                        b = misure[-25:]
                        dif = [x["freddo"]["ms"] - x["caldo"][0]["ms"] for x in b if x.get("freddo") and x["caldo"]]
                        fh = [x["freddo"]["ms"] - x["hello_prima"]["ms"] for x in b if x.get("freddo")]
                        stampa(f"  {fase}: {k + 1}/{len(compiti)}; ultimi 25: freddo meno caldo p50 "
                               f"{percentile(dif, 50) or 0:.2f} ms, freddo meno hello p50 {percentile(fh, 50) or 0:.2f} ms")
                attesa = p0 + (k + 1) * intervallo - time.perf_counter()
                if attesa > 0:
                    STOP.wait(attesa)
        finally:
            if di_fila and not rec.get("fermata") and not STOP.is_set():
                # p1-1.6: la fase finisce con misure senza risposta (rete caduta verso la fine): non e' 'fatta'
                rec["fermata"] = f"le ultime {di_fila} misure senza risposta"
                stampa(f"ATTENZIONE: {rec['fermata']}: la fase va rifatta (riga di ripresa alla fine)")
            t1 = time.time_ns()
            rec["fine"] = iso_ns(t1)
            stampa(f"FASE {fase} FINE {dt.datetime.now().strftime('%H:%M:%S')} ora del Mac ({iso_ns(t1)[:19]}Z), "
                   f"{len(misure)} ricerche")
            rec["profiler_spento"] = self.profiler_spento()
            if rec["profiler_acceso"].get("acceso"):
                e = self.leggi_profilo(visti, profilo)
                if e:
                    errori_profilo.append(e)
        rec["dopo"] = self.istantanea(f"dopo {fase}")
        rec["misure"] = misure
        rec["profiler"] = profilo
        rec["profiler_errori_lettura"] = errori_profilo
        rec["eventi_driver"] = [self.reg.esporta(da[0]), self.reg_c.esporta(da[1])]
        rec["serverStatus_ogni_30s"] = self.campioni[c0:]
        rec["clienti"] = [m.get("cliente") for m in misure if m.get("cliente")]
        rec["rip_caldo"] = self.a.rip_caldo
        rec["riassunto"] = riassumi_fase(rec)
        rec["file"] = file_fase(self.cartella, indice, fase)
        scrivi_json(os.path.join(self.cartella, rec["file"]), rec)
        stampa_fase(rec)
        return rec

    def chiudi(self):
        self.ferma_campioni()
        for c in (self.client, self.mon):
            try:
                c.close()
            except Exception:
                pass


# ----------------------------------------------------------------------------- le ricerche

def finestra_q3(rng, i, riservato):
    fine = FINE_DORMIENTI if riservato else 1.0
    a = T_INIZIO.timestamp() + rng.uniform(0.05, fine - 0.05) * T_SPAN_S
    return (dt.datetime.fromtimestamp(a, dt.timezone.utc),
            dt.datetime.fromtimestamp(a + 14 * 86400, dt.timezone.utc))


def _ts(frazione):
    return dt.datetime.fromtimestamp(T_INIZIO.timestamp() + frazione * T_SPAN_S, dt.timezone.utc)


EVENTI_Q3_M31 = 85  # M31 da 2 KB: Q3 = esattamente 85 eventi del cliente (EVENTI_Q3, main.rs di M31; revisore A, errore 7)


def compito(query, i, gruppo, nome_giro, q2_id=None, a=None, k=0, n=1, scarto=0.0, byte_cliente=None,
            riservato=False, coll=None, prefisso=None, stato=None):
    """Una ricerca, uguale a M31 (RAPPORTO-2 sezione 1): Q2 un ordine per _id; Q3 eventi in 14 giorni (limit 100);
    Q1d i 20 messaggi piu' recenti prima di una data a caso; Q4 finestra a caso di 60 giorni dentro la storia
    dei messaggi del cliente (limit 500, dal piu' nuovo); Q5 ordini di tutti con uno stato in 7 giorni (i 100
    piu' nuovi), finestre all'indietro dalla fine della storia con passo min(7 giorni, storia/n)."""
    cli = (f"{prefisso}{i:05d}" if prefisso else nome_cliente(i)) if i is not None else None
    rng = random.Random(zlib.crc32(f"{VERSIONE}|{nome_giro}|{query}|{i}|{k}".encode()))
    # p1-1.6: i dormienti hanno scritto solo nel primo 60% della storia: le date e le finestre stanno li' dentro
    fine_storia = FINE_DORMIENTI if riservato else 1.0
    if query == "Q1":  # gli ultimi 20 messaggi del cliente (come i giri 0h e 2h e M31)
        spec = {"coll": coll or "messaggi", "filtro": {"cliente": cli}, "sort": [("ts", -1)], "limit": 20}
    elif query == "Q2":
        spec = {"coll": coll or "ordini", "filtro": {"_id": q2_id}, "sort": None, "limit": 1}
    elif query == "Q3" and coll == "events_large":
        # p1-1.6.1 (errore 7 di A): come M31 da 2 KB, esattamente 85 documenti consecutivi del cliente: i piu'
        # recenti prima di una data a caso. Con circa 512 documenti l'anno per cliente (1 GiB, 1.080 clienti,
        # STIMATO) 85 documenti sono circa il 17% dell'anno: la data sta tra il 25% e il 98% dell'anno.
        spec = {"coll": coll, "filtro": {"cliente": cli, "ts": {"$lt": _ts(rng.uniform(0.25, 0.98))}},
                "sort": [("ts", -1)], "limit": EVENTI_Q3_M31}
    elif query == "Q3":
        x, y = finestra_q3(rng, i, riservato)
        spec = {"coll": coll or "eventi", "filtro": {"cliente": cli, "ts": {"$gte": x, "$lt": y}},
                "sort": [("ts", -1)], "limit": 100}
    elif query == "Q1d":
        # data a caso nella storia del cliente attivo; lontana dalla fine (gli ultimi messaggi di tutti stanno
        # nello stesso pezzo di collezione: A2 S1) e dall'inizio (almeno 20 messaggi prima della data)
        # inizio: dopo circa 30 messaggi del cliente, stimati dai byte scritti nel caricamento (meta' messaggi,
        # circa 320 byte l'uno), cosi' prima della data ci sono almeno 20 messaggi (come M31: tra il 20esimo e il
        # piu' nuovo)
        da = 0.05
        if byte_cliente:
            da = min(0.5, max(0.05, 30 / max(1.0, byte_cliente * 0.5 / 320)))
        da = da * fine_storia
        spec = {"coll": coll or "messaggi",
                "filtro": {"cliente": cli, "ts": {"$lt": _ts(rng.uniform(da, 0.98 * fine_storia))}},
                "sort": [("ts", -1)], "limit": 20}
    elif query == "Q4":
        g60 = 60 * 86400 / T_SPAN_S
        lo = rng.uniform(0.02, 0.98 * fine_storia - g60)
        spec = {"coll": coll or "messaggi", "filtro": {"cliente": cli, "ts": {"$gte": _ts(lo), "$lt": _ts(lo + g60)}},
                "sort": [("ts", -1)], "limit": 500}
    elif query == "Q5":
        sette = 7 * 86400
        ts_max = T_INIZIO.timestamp() + T_SPAN_S + 600
        passo = min(sette, (T_SPAN_S - sette) / max(1, n))
        hi = ts_max - (k + scarto) * passo
        spec = {"coll": "ordini", "filtro": {"stato": stato or a.q5_stato, "ts": {
            "$gte": dt.datetime.fromtimestamp(hi - sette, dt.timezone.utc),
            "$lt": dt.datetime.fromtimestamp(hi, dt.timezone.utc)}}, "sort": [("ts", -1)], "limit": 100}
    else:
        raise ValueError(query)
    return {"query": query, "cliente": cli, "i": i, "gruppo": gruppo, "spec": spec}


def caldi_dei_giri_vecchi(st, n_caldi=20):
    """I clienti attivi letti a caldo nei giri vecchi (p1-1.0/1.4: 20 per giro, rng.sample con il seme crc32 del
    nome del giro, prima di ogni altro uso di rng). Il file di stato non li elenca: li ricostruisco con lo stesso
    codice (revisione/prova_infinite_v1.0_usata_al_mattino.py, righe 527 e 554-555), cosi' non tornano tra i
    'mai letti' (revisore dello script p1-1.5)."""
    cl = st.d["clienti"]
    ris = set(cl["riservati"])
    liberi = [i for i in range(cl["n"]) if i not in ris and cl["q2_id"][i] is not None]
    out = set()
    for g in st.d.get("giri", []):
        rng = random.Random(zlib.crc32(str(g.get("nome")).encode()))
        out |= set(rng.sample(liberi, min(n_caldi, len(liberi))))
    return out


def piano_del_giro(st, nome, a, fasi=None):
    """Sceglie i clienti: attivi MAI letti (ne' nei giri 0h e 2h, ne' come caldi, ne' nelle fasi prima), ognuno in
    una sola fase."""
    cl = st.d["clienti"]
    n = cl["n"]
    ris = set(cl["riservati"])
    q2 = [ObjectId(x) if x else None for x in cl["q2_id"]]
    usati = set(st.d.get("usati", [])) | caldi_dei_giri_vecchi(st)
    rng = random.Random(zlib.crc32(f"{VERSIONE}|{nome}".encode()))
    attivi = [i for i in range(n) if i not in ris and i not in usati and q2[i] is not None]
    rng.shuffle(attivi)
    dorm = [i for i in sorted(ris) if i not in usati and q2[i] is not None]
    rng.shuffle(dorm)
    pos = [0]
    avvisi = []

    fasi = set(fasi or FASI_DEFAULT.split(","))

    def prendi(k, per):
        if per.split()[-1] not in fasi and not (per.startswith("spiega") and "spiega" in fasi):
            return []  # fase non scelta: i suoi clienti restano liberi
        x = attivi[pos[0]:pos[0] + k]
        pos[0] += len(x)
        if len(x) < k:
            avvisi.append(f"{per}: chiesti {k} clienti attivi mai letti, disponibili {len(x)}")
        return x

    p = {}
    p["Q2"] = [compito("Q2", i, "attivi", nome, q2[i]) for i in prendi(a.n_q2, "Q2")]
    if a.q2_dormienti:
        p["Q2"] += [compito("Q2", i, "dormienti", nome, q2[i]) for i in dorm]
    p["Q3"] = [compito("Q3", i, "attivi", nome) for i in prendi(a.n_q3, "Q3")]
    bc = cl.get("byte") or [None] * n
    p["Q1d"] = [compito("Q1d", i, "attivi", nome, byte_cliente=bc[i]) for i in prendi(a.n_q1d, "Q1d")]
    p["Q4"] = [compito("Q4", i, "attivi", nome) for i in prendi(a.n_q4, "Q4")]
    p["Q5"] = [compito("Q5", None, "finestre", nome, a=a, k=k, n=a.n_q5) for k in range(a.n_q5)]
    sp = []
    for q in ("Q1d", "Q3", "Q4"):  # clienti nuovi: explain freddo e poi caldo
        for i in prendi(a.n_spiega, f"spiega {q}"):
            c = compito(q, i, "attivi", nome + "|spiega", byte_cliente=bc[i])
            c["freddo_vero"] = True
            sp.append(c)
    for q in ("Q2", "Q5"):  # Q2 (correzione E di B) e Q5 (finestre gia' lette): solo il piano, gia' caldo
        for c0 in p[q][:a.n_spiega]:
            c = dict(c0)
            c["freddo_vero"] = False
            sp.append(c)
    p["spiega"] = sp
    return p, avvisi, {"attivi_mai_letti": len(attivi), "dormienti_mai_letti": len(dorm), "attivi_usati": pos[0]}


# ----------------------------------------------------------------------------- riassunti (dai dati grezzi)

NOMI_Q = {"Q1": "gli ultimi 20 messaggi del cliente", "Q2": "un ordine per _id", "Q3": "eventi in 14 giorni, limit 100",
          "grandi_Q1": "documenti da 2 KB: gli ultimi 20", "grandi_Q3": "documenti da 2 KB: gli 85 prima di una data a caso (come M31)",
          "grandi_Q4": "documenti da 2 KB in 60 giorni, limit 500", "freddo_Q1": "collezione fredda: gli ultimi 20",
          "freddo_Q3": "collezione fredda: 14 giorni, limit 100",
          "Q1d": "20 messaggi piu' recenti prima di una data a caso", "Q4": "finestra di 60 giorni, limit 500",
          "Q5": "ordini di tutti con uno stato in 7 giorni, i 100 piu' nuovi", "spiega": "explain a campione",
          "fredda": "collezione fredda archive_events: 1 documento o 100 di fila per _id",
          # p1-1.6.1: le celle nuove della matrice
          "condizioni_L49_Q1_secondario": "L49: gli ultimi 20 messaggi letti dal secondario",
          "condizioni_L50_Q2_secondario": "L50: un ordine per _id letto dal secondario",
          "condizioni_L51_Q2_majority": "L51: un ordine per _id con readConcern majority",
          "mentre_scrivi_M1_Q1_w1": "M1: gli ultimi 20 mentre si scrive con w=1",
          "mentre_scrivi_M2_Q2_w1": "M2: un ordine per _id mentre si scrive con w=1",
          "mentre_scrivi_M3_Q1_majority": "M3: gli ultimi 20 mentre si scrive con w=majority",
          "mentre_scrivi_M4_Q2_majority": "M4: un ordine per _id mentre si scrive con w=majority",
          "grandi_Q2": "documenti da 2 KB: uno per _id"}


def _get(d, *chiavi):
    for k in chiavi:
        if not isinstance(d, dict) or k not in d:
            return None
        d = d[k]
    return d


def indice_profilo(profilo):
    idx = defaultdict(list)
    for d in profilo:
        c = _get(d, "command", "comment") or _get(d, "originatingCommand", "comment")
        if c:
            idx[str(c)].append(d)
    return idx


def numeri_profilo(docs, zero_se_assente=False):
    """Somma sui documenti del profiler di una ricerca (find ed eventuali getMore). None = non riportato.
    p1-1.6.1 (errore 6 di A): quando una find non legge niente dalla dispensa il profiler scrive 'storage: {}'
    (niente storage.data). Con zero_se_assente=True (lo si passa quando almeno una ricerca della stessa fase
    riporta storage.data, cioe' Atlas il campo lo da') quella find conta 0 byte e 0 microsecondi, non 'non
    riportato': cosi' le letture 'fredde' che non hanno letto niente restano nei riassunti."""
    if not docs:
        return None

    def somma(*ch):
        v = [_get(d, *ch) for d in docs]
        v = [x for x in v if x is not None]
        return sum(v) if v else None
    br, tr = somma("storage", "data", "bytesRead"), somma("storage", "data", "timeReadingMicros")
    if zero_se_assente:
        br = 0 if br is None else br
        tr = 0 if tr is None else tr
    return {"millis": somma("millis"), "docsExamined": somma("docsExamined"), "keysExamined": somma("keysExamined"),
            "nreturned": somma("nreturned"), "bytesRead": br, "timeReadingMicros": tr,
            "cpuNanos": somma("cpuNanos"), "workingMillis": somma("workingMillis"),
            "planSummary": docs[0].get("planSummary")}


def riporta_storage(prof):
    """True se almeno un documento del profiler (indice o lista) ha storage.data: Atlas il campo lo da'."""
    docs = (d for lst in prof.values() for d in lst) if isinstance(prof, dict) else prof
    return any(isinstance(_get(d, "storage", "data"), dict) for d in docs)


def conta_zero(nums):
    """Quante find (trovate nel profiler, con storage riportato) non hanno letto niente dalla dispensa."""
    con = [x for x in nums if x and x.get("bytesRead") is not None]
    return {"find_a_0_byte": sum(1 for x in con if x["bytesRead"] == 0), "su": len(con)}


def riassumi_misure(ms, prof):
    fr = [m for m in ms if m.get("freddo")]
    f = [m["freddo"]["ms"] for m in fr]
    c1 = [m["caldo"][0]["ms"] for m in fr if m["caldo"]]
    fh = [m["freddo"]["ms"] - m["hello_prima"]["ms"] for m in fr]
    ch = [m["caldo"][0]["ms"] - m["caldo"][0]["hello_prima"]["ms"] for m in fr if m["caldo"]]
    dif = [m["freddo"]["ms"] - m["caldo"][0]["ms"] for m in fr if m["caldo"]]
    r = {"n": len(ms), "con_freddo": len(fr), "errori": sum(len(m["errori"]) for m in ms),
         "controlli_ok": sum(1 for m in ms if _get(m, "controllo", "ok") is True),
         "controlli_falliti": sum(1 for m in ms if _get(m, "controllo", "ok") is False),
         "freddo_ms": stat(f), "caldo_ms": stat(c1), "freddo_meno_hello_accoppiato": stat(fh),
         "caldo_meno_hello_accoppiato": stat(ch), "freddo_meno_caldo_stessa_ricerca": stat(dif),
         "p50_freddo_meno_p50_caldo": round(percentile(f, 50) - percentile(c1, 50), 3) if f and c1 else None,
         "documenti_freddo": stat([m["freddo"]["doc"] for m in fr]),
         "documenti_caldo": stat([m["caldo"][0]["doc"] for m in fr if m["caldo"]]),
         "byte_freddo": stat([m["freddo"]["byte"] for m in fr]),
         "getmore_freddo_totale": sum(m["freddo"]["getmore"] for m in fr)}
    r["blocchi_25"] = []
    for s in range(0, len(fr), 25):
        b = fr[s:s + 25]
        r["blocchi_25"].append({
            "da": s, "a": s + len(b),
            "freddo_meno_caldo_p50": percentile([m["freddo"]["ms"] - m["caldo"][0]["ms"] for m in b if m["caldo"]], 50),
            "freddo_meno_hello_p50": percentile([m["freddo"]["ms"] - m["hello_prima"]["ms"] for m in b], 50)})
    zero = riporta_storage(prof)
    for nome, lst in (("freddo", [prof.get(m["freddo"]["commento"]) for m in fr]),
                      ("caldo", [prof.get(m["caldo"][0]["commento"]) for m in fr if m["caldo"]])):
        nums = [numeri_profilo(x, zero) for x in lst]
        trovati = [x for x in nums if x]
        pos = [x for x in trovati if x["bytesRead"]]
        r[f"profiler_{nome}"] = {
            "ricerche_trovate_nel_profiler": len(trovati), "ricerche": len(nums),
            "millis": stat([x["millis"] for x in trovati]),
            "storage_byte_letti": stat([x["bytesRead"] for x in trovati]),
            "storage_ms_di_lettura": stat([x["timeReadingMicros"] / 1000 for x in trovati
                                           if x["timeReadingMicros"] is not None]),
            # p1-1.6.1: le find che non hanno letto niente dalla dispensa, e le statistiche solo su quelle che hanno
            # letto (il confronto con M31 si fa su queste: vedi GIRO-FINALE, per il coordinatore)
            **conta_zero(trovati),
            "storage_byte_letti_solo_maggiori_di_0": stat([x["bytesRead"] for x in pos]),
            "storage_ms_di_lettura_solo_maggiori_di_0": stat([x["timeReadingMicros"] / 1000 for x in pos
                                                              if x["timeReadingMicros"] is not None]),
            "millis_solo_maggiori_di_0": stat([x["millis"] for x in pos]),
            "storage_non_riportato_da_atlas": sum(1 for x in trovati if x["bytesRead"] is None),
            "docsExamined": stat([x["docsExamined"] for x in trovati]),
            "keysExamined": stat([x["keysExamined"] for x in trovati]),
            "piani": sorted({str(x["planSummary"]) for x in trovati})}
    return r


def riassumi_fase(rec):
    prof = indice_profilo(rec.get("profiler", []))
    ms = rec.get("misure", [])
    out = {"fase": rec["fase"], "inizio": rec.get("inizio"), "fine": rec.get("fine")}
    if rec["fase"] == "spiega":
        out["per_query"] = {}
        for q in ("Q1", "Q1d", "Q3", "Q4", "Q2", "Q5"):  # p1-1.6.1: c'era tutto tranne Q1 (che pure si misurava)
            x = [m for m in ms if m.get("query") == q]
            if not x:
                continue
            d = {"n": len(x), "freddo_vero": x[0]["freddo_vero"]}
            for tipo in ("freddo", "caldo", "solo_piano"):
                y = [m[tipo] for m in x if tipo in m]
                if y:
                    d[tipo] = {"server_ms": stat([z["executionTimeMillis"] for z in y]),
                               "cliente_ms": stat([z["ms"] for z in y]),
                               "chiavi": stat([z["totalKeysExamined"] for z in y]),
                               "documenti_esaminati": stat([z["totalDocsExamined"] for z in y]),
                               "restituiti": stat([z["nReturned"] for z in y])}
            out["per_query"][q] = d
        out["errori"] = sum(len(m["errori"]) for m in ms)
        out["clienti"] = [m.get("cliente") for m in ms if m.get("cliente")]
        return out
    out.update(riassumi_misure(ms, prof))
    gruppi = sorted({m.get("gruppo") for m in ms})
    if len(gruppi) > 1:
        out["per_gruppo"] = {g: riassumi_misure([m for m in ms if m.get("gruppo") == g], prof) for g in gruppi}
    if rec["fase"] == "Q3":
        out["primi_50_freddo_vero"] = riassumi_misure(ms[:50], prof)
    out["clienti"] = [m.get("cliente") for m in ms if m.get("cliente")]
    return out


def _r(s, k="p50"):
    v = s.get(k) if isinstance(s, dict) else None
    return "-" if v is None else f"{v:.2f}"


def _riga(nome, s):
    return (f"   {nome}: p50 {_r(s)}  p95 {_r(s, 'p95')}  p99 {_r(s, 'p99')} ms (n={s.get('n', 0)})")


def stampa_riassunto_misure(r, rientro=""):
    print(rientro + _riga("freddo                        ", r["freddo_ms"]))
    print(rientro + _riga("caldo (stessa ricerca, subito)", r["caldo_ms"]))
    print(rientro + _riga("freddo meno hello accoppiato  ", r["freddo_meno_hello_accoppiato"]))
    print(rientro + _riga("caldo meno hello accoppiato   ", r["caldo_meno_hello_accoppiato"]))
    print(rientro + _riga("freddo meno caldo (accoppiato)", r["freddo_meno_caldo_stessa_ricerca"]))
    print(rientro + f"   p50 freddo meno p50 caldo: {r['p50_freddo_meno_p50_caldo']} ms; documenti freddo p50 "
          f"{_r(r['documenti_freddo'])}, caldo p50 {_r(r['documenti_caldo'])}; byte freddo p50 "
          f"{_r(r['byte_freddo'])}; getMore {r['getmore_freddo_totale']}")
    bl = ", ".join(f"{b['freddo_meno_caldo_p50']:.1f}" for b in r["blocchi_25"] if b["freddo_meno_caldo_p50"] is not None)
    print(rientro + f"   freddo meno caldo p50 a blocchi di 25: {bl}")
    for nome in ("freddo", "caldo"):
        p = r[f"profiler_{nome}"]
        print(rientro + f"   profiler {nome}: trovate {p['ricerche_trovate_nel_profiler']} su {p['ricerche']}; millis p50 "
              f"{_r(p['millis'])} p99 {_r(p['millis'], 'p99')}; letti dallo storage p50 {_r(p['storage_byte_letti'])} "
              f"byte; tempo di lettura dallo storage p50 {_r(p['storage_ms_di_lettura'])} p95 "
              f"{_r(p['storage_ms_di_lettura'], 'p95')} p99 {_r(p['storage_ms_di_lettura'], 'p99')} ms "
              + (f"(storage.data assente in tutta la fase, {p['storage_non_riportato_da_atlas']} find: o Atlas non lo da' "
                 f"o tutte a 0 byte, NON SO) " if p['storage_non_riportato_da_atlas'] else "") + f"piani {p['piani']}")
        print(rientro + f"   profiler {nome}: find a 0 byte dalla dispensa {p['find_a_0_byte']} su {p['su']}; solo quelle "
              f"con byte > 0: lettura dallo storage p50 {_r(p['storage_ms_di_lettura_solo_maggiori_di_0'])} p99 "
              f"{_r(p['storage_ms_di_lettura_solo_maggiori_di_0'], 'p99')} ms, byte p50 "
              f"{_r(p['storage_byte_letti_solo_maggiori_di_0'])}")


def stampa_fase(rec):
    r = rec["riassunto"]
    print()
    print(f"=== FASE {rec['fase']} ({NOMI_Q.get(rec['fase'], '')}): {r.get('inizio')} - {r.get('fine')} UTC ===")
    if rec["fase"] == "spiega":
        for q, d in r["per_query"].items():
            for tipo in ("freddo", "caldo", "solo_piano"):
                if tipo in d:
                    x = d[tipo]
                    print(f"   {q} {tipo}: server p50 {_r(x['server_ms'])} p99 {_r(x['server_ms'], 'p99')} ms; chiavi "
                          f"{_r(x['chiavi'])}, documenti esaminati {_r(x['documenti_esaminati'])}, "
                          f"restituiti {_r(x['restituiti'])} (n={x['server_ms'].get('n', 0)})")
        print(f"   errori: {r['errori']}")
        print()
        return
    print(f"   ricerche {r['n']}, errori {r['errori']}, controlli ok {r['controlli_ok']}, "
          f"falliti {r['controlli_falliti']}")
    stampa_riassunto_misure(r)
    if "primi_50_freddo_vero" in r:
        print("   --- primi 50 clienti (freddo vero):")
        stampa_riassunto_misure(r["primi_50_freddo_vero"], "   ")
    for g, x in r.get("per_gruppo", {}).items():
        print(f"   --- gruppo {g}:")
        stampa_riassunto_misure(x, "   ")
    print()


# ----------------------------------------------------------------------------- il giro (comando misura)

FASI_DEFAULT = "Q2,Q3,Q1d,Q4,Q5,spiega"


def cmd_misura(a, nome=None, stato=None):
    signal.signal(signal.SIGINT, ferma)
    signal.signal(signal.SIGTERM, ferma)
    st = stato or Stato(a.file)
    if "clienti" not in st.d:
        print(f"Nel file {a.file} non c'e' un caricamento: serve il file di stato di ieri (risultati_p1.json).")
        sys.exit(2)
    nome = nome or a.giro or iso(ora())
    fasi = [x.strip() for x in a.fasi.split(",") if x.strip()]
    piano, avvisi, conti = piano_del_giro(st, nome, a, fasi)
    stampa(f"Giro '{nome}' (versione {VERSIONE}): clienti attivi mai letti {conti['attivi_mai_letti']}, dormienti "
           f"mai letti {conti['dormienti_mai_letti']}. Fasi: {', '.join(f'{f} ({len(piano[f])})' for f in fasi)}.")
    for x in avvisi:
        stampa("AVVISO:", x)
    if a.a_secco:
        for f in fasi:
            print(f"{f}: {len(piano[f])} ricerche; prime 3: " +
                  "; ".join(f"{c.get('cliente') or 'finestra'} {c['spec']['filtro']}" for c in piano[f][:3]))
        print("A secco: nessuna connessione, niente scritto.")
        return st
    copia = a.file + f".prima-del-giro-{re.sub(r'[^A-Za-z0-9_-]', '_', nome)}.json"
    if os.path.exists(a.file) and not os.path.exists(copia):
        shutil.copyfile(a.file, copia)
    g = Giro(a, "giro", nome)
    stampa(f"Dati grezzi in: {os.path.abspath(g.cartella)}")
    rec = {"versione": VERSIONE, "giro": nome, "cartella": g.cartella, "inizio": iso_us(), "fasi": [], "pause": [],
           "piano": {f: [{"cliente": c.get("cliente"), "gruppo": c.get("gruppo"), "filtro": c["spec"]["filtro"]}
                         for c in piano[f]] for f in fasi}, "avvisi": avvisi, "conti_clienti": conti}
    usati = set(st.d.get("usati", []))
    try:
        rec["ambiente"] = g.ambiente()
        if not a.senza_atlas:
            rec["atlas_inizio"] = _prova(lambda: raccolta_atlas(os.path.join(g.cartella, "atlas_inizio"), a, g))
        g.avvia_campioni()
        rec["inizio_giornata"] = g.istantanea("inizio del giro")
        for idx, f in enumerate(fasi):
            if STOP.is_set():
                break
            g.fase = f"pausa prima di {f}"
            p0 = iso_us()
            stampa(f"Pausa di {a.pausa_s:g} s prima della fase {f} (i grafici di Atlas si separano per fase).")
            dorme(a.pausa_s)
            rec["pause"].append({"prima_di": f, "inizio": p0, "fine": iso_us()})
            if STOP.is_set():
                break
            fr = g.esegui_fase(idx + 1, f, piano[f], tipo="spiega" if f == "spiega" else "find")
            rec["fasi"].append(fr)
            usati.update(m["i"] for m in fr["misure"] if m.get("i") is not None)
            st.d["usati"] = sorted(usati)
            st.salva()
        g.fase = "fine"
        rec["fine_giornata"] = g.istantanea("fine del giro")
    finally:
        g.ferma_campioni()
        if g.profilo_acceso:
            g.profiler_spento()
        rec["campioni_serverStatus"] = g.campioni
        rec["eventi_driver_completi"] = [g.reg.esporta(), g.reg_c.esporta()]
        st.d["usati"] = sorted(usati)
        st.d.setdefault("giri_p15", []).append({"nome": nome, "cartella": g.cartella, "inizio": rec["inizio"],
                                                 "fine": iso_us(), "fasi": [x["fase"] for x in rec["fasi"]]})
        st.salva()
        rec["fine"] = iso_us()
        scrivi_json(os.path.join(g.cartella, "tutto.json"), rec)
        stampa(f"Salvato: {os.path.abspath(os.path.join(g.cartella, 'tutto.json'))}")
    if not a.senza_atlas and not STOP.is_set():
        rec["atlas_fine"] = _prova(lambda: raccolta_atlas(os.path.join(g.cartella, "atlas_fine"), a, g))
        scrivi_json(os.path.join(g.cartella, "tutto.json"), rec)
    g.chiudi()
    print()
    print(f"=== RIASSUNTO DEL GIRO '{nome}' (calcolato dai dati grezzi; si rifa' con: riassunto --cartella "
          f"{g.cartella}) ===")
    for fr in rec["fasi"]:
        stampa_fase(fr)
    return st


# ----------------------------------------------------------------------------- latenza delle scritture

def cmd_latenza(a):
    signal.signal(signal.SIGINT, ferma)
    signal.signal(signal.SIGTERM, ferma)
    g = Giro(a, "latenza", a.giro or "latenza")
    stampa(f"Dati grezzi in: {os.path.abspath(g.cartella)}")
    rec = {"versione": VERSIONE, "comando": "latenza", "inizio": iso_us(), "collezione": "scratch_writes"}
    g.fase = "latenza"
    col = g.db["scratch_writes"]
    scritture = []
    try:
        rec["ambiente"] = g.ambiente()
        g.avvia_campioni()
        rec["prima"] = g.istantanea("prima della latenza")
        rec["profiler_acceso"] = g.profiler_acceso()
        coll = {"w1": col.with_options(write_concern=WriteConcern(w=1)),
                "majority": col.with_options(write_concern=WriteConcern(w="majority"))}
        rng = random.Random(a.seme + 777)
        tot = 2 * a.n_latenza + 10
        rec["inizio_scritture"] = iso_us()
        stampa(f"LATENZA INIZIO {dt.datetime.now().strftime('%H:%M:%S')}: 10 di riscaldamento, poi {a.n_latenza} con w=1 e "
               f"{a.n_latenza} con w=majority, alternate, un hello prima di ognuna.")
        for k in range(tot):
            if STOP.is_set():
                break
            tipo = "riscaldamento" if k < 10 else ("w1" if (k - 10) % 2 == 0 else "majority")
            d = doc_2kb(rng, f"u{rng.randrange(5000):05d}")
            d["_id"] = ObjectId()
            w = {"k": k, "tipo": tipo, "byte": len(bson.encode(d))}
            try:
                w["hello_prima"] = g.hello_accoppiato()
                t0s = time.time_ns()
                t0 = time.perf_counter_ns()
                coll["majority" if tipo == "majority" else "w1"].insert_one(d, comment=f"p1|latenza|{tipo}|{k}")
                w["ms"] = round(ms_da(t0), 4)
                w["inizio"], w["fine"] = iso_ns(t0s), iso_us()
            except PyMongoError as e:
                w["errore"] = pulito(e)
                w["t"] = iso_us()
            scritture.append(w)
            if (k + 1) % 100 == 0:
                stampa(f"  scritture {k + 1}/{tot}")
        rec["fine_scritture"] = iso_us()
        stampa(f"LATENZA FINE {dt.datetime.now().strftime('%H:%M:%S')}")
    finally:
        rec["profiler_spento"] = g.profiler_spento()
        prof = []
        if rec.get("profiler_acceso", {}).get("acceso"):
            rec["profiler_errori_lettura"] = g.leggi_profilo(set(), prof)
        rec["profiler"] = prof
        rec["scritture"] = scritture
        rec["dopo"] = g.istantanea("dopo la latenza")
        g.ferma_campioni()
        rec["serverStatus_ogni_30s"] = g.campioni
        rec["eventi_driver"] = [g.reg.esporta(), g.reg_c.esporta()]
        rec["drop"] = _prova(lambda: col.drop() or "collezione scratch_writes cancellata")
        rec["fine"] = iso_us()
        rec["riassunto"] = riassumi_latenza(rec)
        scrivi_json(os.path.join(g.cartella, "latenza.json"), rec)
        g.chiudi()
    stampa_latenza(rec)


def riassumi_latenza(rec):
    r = {}
    prof = indice_profilo(rec.get("profiler", []))
    for tipo in ("w1", "majority"):
        x = [w for w in rec.get("scritture", []) if w["tipo"] == tipo]
        ok = [w for w in x if "ms" in w]
        p = [numeri_profilo(prof.get(f"p1|latenza|{tipo}|{w['k']}")) for w in ok]
        r[tipo] = {"n": len(x), "errori": sum(1 for w in x if "errore" in w), "ms": stat([w["ms"] for w in ok]),
                   "meno_hello_accoppiato": stat([w["ms"] - w["hello_prima"]["ms"] for w in ok]),
                   "byte_medi": round(statistics.fmean([w["byte"] for w in x]), 1) if x else None,
                   "profiler_millis": stat([q["millis"] for q in p if q]),
                   "profiler_trovate": sum(1 for q in p if q)}
    return r


def stampa_latenza(rec):
    r = rec["riassunto"]
    print()
    print(f"=== LATENZA DELLE SCRITTURE ({rec.get('inizio_scritture')} - {rec.get('fine_scritture')} UTC) ===")
    for tipo in ("w1", "majority"):
        x = r[tipo]
        print(f"insert_one da {x['byte_medi']} byte, w={'1' if tipo == 'w1' else 'majority'}: errori {x['errori']}")
        print(_riga("tempo dal cliente        ", x["ms"]))
        print(_riga("meno hello accoppiato    ", x["meno_hello_accoppiato"]))
        print(_riga("profiler (millis, server)", x["profiler_millis"]))
    print()


# ----------------------------------------------------------------------------- collezione fredda (punto 7 del fondatore)

NOME_FREDDA = "archive_events"  # nome neutro (RULES)


def _errore_completo(e):
    """Errore del server salvato per intero (codice, nome, messaggio, risposta), senza segreti."""
    out = {"errore": pulito(e), "tipo_errore": type(e).__name__}
    for k in ("code", "details"):
        v = getattr(e, k, None)
        if v is not None:
            out[k] = v
    return out


def copia_fredda(g, a, rec):
    """Copia circa a.fredda_gb GiB di eventi (i primi per _id, cioe' in ordine di arrivo) nella collezione fredda.
    Prima lato server ($merge, niente rete), a lotti; se $merge e' rifiutato, dal Mac (find + insert_many).
    Si ferma a a.fredda_gb GiB (dimensione 'size' di collStats) o dopo a.fredda_max_min minuti."""
    db, raw_src = g.db, g.raw("eventi")
    dst_raw = g.raw(NOME_FREDDA)
    cs = _prova(lambda: db.command("collStats", "eventi"))
    medio = (cs.get("avgObjSize") if isinstance(cs, dict) else None) or 300
    per_lotto = max(1000, int(a.fredda_lotto_mb * MB / medio))
    obiettivo = a.fredda_gb * GB
    scadenza = time.time() + a.fredda_max_min * 60
    modo = "merge"
    ultimo, copiati, k = None, 0, 0
    rec["copia"] = {"documento_medio_eventi": medio, "documenti_per_lotto": per_lotto, "obiettivo_byte": obiettivo,
                    "inizio": iso_us(), "lotti": [], "primo_id": None, "ultimo_id": None}
    cp = rec["copia"]
    stampa(f"COPIA INIZIO {dt.datetime.now().strftime('%H:%M:%S')}: circa {a.fredda_gb:g} GiB di eventi in "
           f"{NOME_FREDDA}, lotti da {per_lotto:,} documenti (al massimo {a.fredda_max_min:g} minuti).")
    while not STOP.is_set() and copiati < obiettivo and time.time() < scadenza:
        filtro = {"_id": {"$gt": ultimo}} if ultimo is not None else {}
        lot = {"k": k, "modo": modo, "inizio": iso_us()}
        try:
            confine = next(iter(db["eventi"].find(filtro, {"_id": 1}).sort("_id", 1).skip(per_lotto - 1).limit(1)),
                           None)
            f = {"_id": dict(filtro.get("_id", {}))}
            if confine is not None:
                f["_id"]["$lte"] = confine["_id"]
            if not f["_id"]:
                f = {}
            if k == 0:
                primo = next(iter(db["eventi"].find(f, {"_id": 1}).sort("_id", 1).limit(1)), None)
                cp["primo_id"] = primo["_id"] if primo else None
            t0 = time.perf_counter_ns()
            if modo == "merge":
                try:
                    list(db["eventi"].aggregate(
                        [{"$match": f}, {"$sort": {"_id": 1}},
                         {"$merge": {"into": NOME_FREDDA, "on": "_id", "whenMatched": "fail",
                                     "whenNotMatched": "insert"}}],
                        maxTimeMS=int(a.fredda_max_min * 60000), comment=f"p1|fredda|copia|{k}"))
                except PyMongoError as e:
                    if k == 0 and copiati == 0:
                        cp["merge_rifiutato"] = _errore_completo(e)
                        stampa(f"  $merge non riuscito ({pulito(e)[:120]}): copio dal Mac.")
                        modo = lot["modo"] = "dal_mac"
                    else:
                        raise
            if modo == "dal_mac":
                docs = list(raw_src.find(f).sort("_id", 1))
                if docs:
                    dst_raw.insert_many(docs, ordered=False, comment=f"p1|fredda|copia|{k}")
                lot["documenti_letti"] = len(docs)
                lot["byte_letti"] = sum(len(d.raw) for d in docs)
            lot["ms"] = round(ms_da(t0), 1)
            s = _prova(lambda: db.command("collStats", NOME_FREDDA))
            lot["collStats_size"] = s.get("size") if isinstance(s, dict) else None
            lot["collStats_count"] = s.get("count") if isinstance(s, dict) else None
            copiati = lot["collStats_size"] or (copiati + (lot.get("byte_letti") or per_lotto * medio))
            lot["fine"] = iso_us()
            cp["lotti"].append(lot)
            stampa(f"  lotto {k}: {copiati / GB:.3f} GiB copiati ({lot['ms'] / 1000:.1f} s)")
            if confine is None:
                cp["ultimo_id"] = None
                break
            ultimo = confine["_id"]
            cp["ultimo_id"] = ultimo
            k += 1
        except PyMongoError as e:
            lot.update({"errore": _errore_completo(e), "fine": iso_us()})
            cp["lotti"].append(lot)
            stampa(f"  copia ferma: {pulito(e)[:200]}")
            break
    if cp["ultimo_id"] is None and ultimo is not None:
        cp["ultimo_id"] = ultimo
    cp.update({"fine": iso_us(), "byte_copiati": copiati, "modo": modo,
               "motivo_fine": ("fermata a mano" if STOP.is_set() else "obiettivo raggiunto" if copiati >= obiettivo
                               else "tempo massimo" if time.time() >= scadenza else "errore o fine dei dati")})
    stampa(f"COPIA FINE {dt.datetime.now().strftime('%H:%M:%S')}: {copiati / GB:.3f} GiB ({cp['motivo_fine']}).")
    return copiati


def carica_fredda_dal_mac(g, a, rec, st):
    """p1-1.6.1: riempie la collezione fredda con circa a.fredda_gb GiB di documenti nuovi da circa 2 KB scritti dal
    Mac (come events_large, in ordine di tempo; insert soltanto, che le collezioni fredde accettano). Niente letture
    delle collezioni misurate. Riprende se interrotto."""
    car = carica_2kb(g, a, NOME_FREDDA, "arch-", a.fredda_clienti, a.fredda_gb, "fredda_carico", st)
    cp = {"modo": "documenti nuovi dal Mac (niente letture delle collezioni misurate)", "caricamento": car,
          "inizio": car.get("inizio"), "fine": car.get("fine"), "byte_copiati": car.get("byte_bson")}
    primo = _prova(lambda: next(iter(g.db[NOME_FREDDA].find({}, {"_id": 1}).sort("_id", 1).limit(1)), None))
    ultimo = _prova(lambda: next(iter(g.db[NOME_FREDDA].find({}, {"_id": 1}).sort("_id", -1).limit(1)), None))
    cp["primo_id"] = primo.get("_id") if isinstance(primo, dict) else None
    cp["ultimo_id"] = ultimo.get("_id") if isinstance(ultimo, dict) else None
    rec["copia"] = cp
    return car.get("byte_bson") or 0


def compiti_freddi(cp, a):
    """Letture per _id nella collezione fredda: il primo documento con _id >= un punto a caso tra il primo e l'ultimo
    copiato (n_fredda letture da 1 documento), e n_fredda_100 letture di 100 documenti di fila."""
    p, u = cp.get("primo_id"), cp.get("ultimo_id")
    if not isinstance(p, ObjectId) or not isinstance(u, ObjectId):
        return []
    t0, t1 = int(p.generation_time.timestamp()), int(u.generation_time.timestamp())
    rng = random.Random(zlib.crc32(f"{VERSIONE}|fredda".encode()))
    tot = a.n_fredda + a.n_fredda_100
    # un secondo diverso per ogni lettura (l'_id cambia secondo per secondo; nel carico di ieri circa 2.800 eventi al
    # secondo, circa 28 pagine): nessuna lettura tocca le pagine di un'altra
    secondi = list(range(t0, t1 + 1))
    scelti = rng.sample(secondi, tot) if len(secondi) >= tot else [rng.choice(secondi) for _ in range(tot)]
    out = []
    for k in range(tot):
        lim = 1 if k < a.n_fredda else 100
        oid = ObjectId.from_datetime(dt.datetime.fromtimestamp(scelti[k], dt.timezone.utc))
        out.append({"query": "fredda", "cliente": None, "i": None, "gruppo": "punto" if lim == 1 else "100_di_fila",
                    "senza_controllo": True,
                    "spec": {"coll": NOME_FREDDA, "filtro": {"_id": {"$gte": oid}}, "sort": [("_id", 1)], "limit": lim}})
    return out


def _oid(x):
    try:
        return ObjectId(x) if isinstance(x, str) else x
    except Exception:
        return x


def esegui_fredda(g, a, rec, indice=1, st=None):
    """Il cuore di 'fredda' (revisore dello script), usato anche dalla fase 'freddo' del comando 'giro' (p1-1.6).
    p1-1.6.1 (E9 di B): con a.fredda_solo_carica fa solo creazione e copia e la lascia (su M10, prima del cambio);
    il giro su M30 la ritrova (file di stato, chiave 'fredda'), salta creazione, copia e attesa se il primario e'
    ripartito dopo la copia, controlla il bancone (correzione 13 di A), legge, prova la modifica e la cancella."""
    db = g.db
    rec.setdefault("fasi", [])
    rec["collezione"] = NOME_FREDDA
    rec["opzione"] = {"storageTier": {"collection": "cold"}}
    creata = False
    stf = (st.d.get("fredda") if st is not None else None) or {}
    riuso = False
    try:
        if NOME_FREDDA in db.list_collection_names():
            rec["gia_esistente"] = _prova(lambda: list(db.list_collections(filter={"name": NOME_FREDDA})))
            if stf.get("copiata") and not stf.get("cancellata"):
                riuso, creata = True, True
                rec["riuso"] = {"nota": "creata e copiata in un'esecuzione prima (su M10)", "stato": dict(stf)}
                rec["create"] = stf.get("create") or {"accettata": True}
                cp = dict(stf.get("copia") or {})
                cp["primo_id"], cp["ultimo_id"] = _oid(cp.get("primo_id")), _oid(cp.get("ultimo_id"))
                rec["copia"] = cp
                stampa(f"La collezione {NOME_FREDDA} c'e' gia' (creata e copiata prima, {stf.get('fine_copia')}): "
                       f"la uso.")
            else:
                # p1-1.6.2 (revisore finale): per esempio caricamento su M10 interrotto con Ctrl+C. La fase NON e'
                # 'fatta' (prima risultava 'fatta' senza aver letto niente); la collezione la cancella la fase 'fine'.
                rec["fermata"] = (f"{NOME_FREDDA} esiste gia' ma non risulta caricata per intero (caricamento "
                                  f"interrotto?): niente letture, cancellata dalla fase 'fine'")
                stampa(f"La collezione {NOME_FREDDA} esiste gia' ma non risulta caricata per intero (caricamento su "
                       f"M10 interrotto?): non la tocco, la cancella la fase 'fine'. Fase freddo NON fatta. Scritto "
                       f"nel file.")
                return rec
        if not riuso:
            g.fase = "fredda: create"
            comando = SON([("create", NOME_FREDDA), ("storageTier", {"collection": "cold"})])
            t0 = time.perf_counter_ns()
            try:
                r = db.command(comando)
                rec["create"] = {"accettata": True, "t": iso_us(), "ms": round(ms_da(t0), 2), "risposta": r}
                creata = True
                stampa("createCollection con storageTier freddo: ACCETTATA.")
            except PyMongoError as e:
                rec["create"] = {"accettata": False, "t": iso_us(), "ms": round(ms_da(t0), 2), **_errore_completo(e)}
                stampa(f"createCollection con storageTier freddo: RIFIUTATA ({pulito(e)[:200]}). Salvato, vado avanti.")
                if st is not None:
                    st.d["fredda"] = {"rifiutata": True, "create": rec["create"]}
                    st.salva()
            rec["listCollections_dopo_create"] = _prova(lambda: list(db.list_collections(filter={"name": NOME_FREDDA})))
            if NOME_FREDDA in (db.list_collection_names() if not creata else [NOME_FREDDA]):
                creata = True  # rifiutata ma creata lo stesso: si cancella alla fine
            if not rec["create"]["accettata"] or STOP.is_set():
                return rec
            rec["collStats_dopo_create"] = _prova(lambda: db.command("collStats", NOME_FREDDA))
            g.fase = "fredda: copia"
            if a.fredda_solo_carica and st is not None:
                # p1-1.6.1: su M10, prima delle misure, NON si copia da 'eventi' (si leggerebbe circa 1 GiB degli
                # eventi che Q3 misura dopo: se Infinite ha una cache delle pagine che sopravvive al cambio di
                # taglia, NON SO, quelle letture sarebbero meno fredde). Si scrivono documenti nuovi dal Mac.
                copiati = carica_fredda_dal_mac(g, a, rec, st)
            else:  # nel giro su M30 la copia da 'eventi' viene dopo tutte le letture misurate
                copiati = copia_fredda(g, a, rec)
            rec["collStats_dopo_copia"] = _prova(lambda: db.command("collStats", NOME_FREDDA))
            rec["listCollections_dopo_copia"] = _prova(lambda: list(db.list_collections(filter={"name": NOME_FREDDA})))
            if not copiati or STOP.is_set():
                stampa("Niente copiato: salto le letture.")
                return rec
            if st is not None:
                st.d["fredda"] = {"copiata": True, "fine_copia": iso_us(), "create": rec["create"],
                                  "copia": {k: (str(v) if isinstance(v, ObjectId) else v) for k, v in rec["copia"].items()
                                            if k != "lotti"}}
                st.salva()
            if a.fredda_solo_carica:
                creata = False  # resta: letture, modifica e cancellazione nel giro su M30
                rec["nota"] = "solo creazione e copia (prima del cambio a M30): il resto nel giro su M30"
                stampa(f"{NOME_FREDDA} creata e copiata: la lascio per il giro su M30 (letture, modifica, cancellazione).")
                return rec
        fine_copia = stf.get("fine_copia") if riuso else None
        rip = nodi_ripartiti_dopo(g, fine_copia) if fine_copia else None
        rec["riavvio"] = rip
        g.fase = "fredda: attesa"
        if (rip or {}).get("ripartito_dopo"):
            rec["attesa"] = {"saltata": "il primario e' ripartito dopo la copia (cambio di taglia): fredda per costruzione"}
            stampa("Attesa saltata: il primario e' ripartito dopo la copia (cambio di taglia).")
        else:
            rec["attesa"] = {"inizio": iso_us(), "secondi": a.fredda_attesa_s}
            stampa(f"Attesa di {a.fredda_attesa_s / 60:g} minuti (fino alle "
                   f"{(dt.datetime.now() + dt.timedelta(seconds=a.fredda_attesa_s)).strftime('%H:%M:%S')}).")
            dorme(a.fredda_attesa_s)
            rec["attesa"]["fine"] = iso_us()
        if STOP.is_set():
            return rec
        # p1-1.6.1 (correzione 13 di A): se la collezione e' ancora nel bancone, lavaggio prima delle letture
        g.fase = "fredda: controllo del bancone"
        rec["lavaggio"] = lavaggio(g, a, NOME_FREDDA, fine_copia or (rec.get("copia") or {}).get("fine"), {})
        if STOP.is_set():
            return rec
        compiti = compiti_freddi(rec["copia"], a)
        rec["fasi"].append(g.esegui_fase(indice, "fredda", compiti))
        g.fase = "fredda: aggiornamento"
        bersaglio = rec["copia"].get("primo_id")
        t0 = time.perf_counter_ns()
        try:
            r = db[NOME_FREDDA].update_one({"_id": bersaglio}, {"$set": {"nota_modificata": "x"}},
                                           comment="p1|fredda|aggiornamento")
            rec["aggiornamento"] = {"riuscito": True, "t": iso_us(), "ms": round(ms_da(t0), 2), "_id": bersaglio,
                                    "matched": r.matched_count, "modified": r.modified_count, "risposta": r.raw_result}
            stampa(f"Aggiornamento nella collezione fredda: RIUSCITO (matched {r.matched_count}, modified "
                   f"{r.modified_count}).")
        except PyMongoError as e:
            rec["aggiornamento"] = {"riuscito": False, "t": iso_us(), "ms": round(ms_da(t0), 2), "_id": bersaglio,
                                    **_errore_completo(e)}
            stampa(f"Aggiornamento nella collezione fredda: ERRORE (atteso): {pulito(e)[:200]}")
    finally:
        if creata:
            g.fase = "fredda: drop"
            rec["drop"] = _prova(lambda: db.drop_collection(NOME_FREDDA))
            rec["listCollections_dopo_drop"] = _prova(lambda: sorted(db.list_collection_names()))
            if st is not None and isinstance(rec["listCollections_dopo_drop"], list) \
                    and NOME_FREDDA not in rec["listCollections_dopo_drop"]:
                st.d.setdefault("fredda", {})["cancellata"] = True
                st.salva()
            stampa(f"Collezione {NOME_FREDDA} cancellata." if NOME_FREDDA not in (rec["listCollections_dopo_drop"] or [])
                   else f"ATTENZIONE: {NOME_FREDDA} c'e' ancora: cancellala (vedi GIRO-FINALE).")
    return rec


def cmd_fredda(a):
    """Punto 7 del fondatore (gratis, non decisivo): createCollection con storageTier freddo. Se rifiutata, salva la
    risposta e finisce. Se accettata: copia circa 1 GiB di eventi, aspetta 10 minuti, letture fredde con il profiler
    (come le altre fasi), prova un aggiornamento (atteso un errore), poi cancella la collezione."""
    signal.signal(signal.SIGINT, ferma)
    signal.signal(signal.SIGTERM, ferma)
    g = Giro(a, "fredda", a.giro or "fredda")
    stampa(f"Dati grezzi in: {os.path.abspath(g.cartella)}")
    rec = {"versione": VERSIONE, "comando": "fredda", "inizio": iso_us(), "fasi": []}
    g.fase = "fredda"
    try:
        rec["ambiente"] = g.ambiente()
        g.avvia_campioni()
        rec["prima"] = g.istantanea("prima della collezione fredda")
        esegui_fredda(g, a, rec)
    finally:
        g.fase = "fredda: fine"
        rec["dopo"] = g.istantanea("dopo la collezione fredda")
        g.ferma_campioni()
        if g.profilo_acceso:
            g.profiler_spento()
        rec["serverStatus_ogni_30s"] = g.campioni
        rec["eventi_driver_completi"] = [g.reg.esporta(), g.reg_c.esporta()]
        rec["fine"] = iso_us()
        scrivi_json(os.path.join(g.cartella, "fredda.json"), rec)
        stampa(f"Salvato: {os.path.abspath(os.path.join(g.cartella, 'fredda.json'))}")
        g.chiudi()
    for fr in rec["fasi"]:
        stampa_fase(fr)


# ----------------------------------------------------------------------------- Atlas Admin API (sola lettura)

ATLAS_BASE = os.environ.get("ATLAS_API_BASE", "https://cloud.mongodb.com").rstrip("/")  # cambiato solo nelle prove
VERSIONI_API = ("2023-01-01", "2023-02-01", "2024-08-05", "2024-11-13", "2025-03-12")
INIZIO_CARICO = "2026-11-04T08:35:00Z"


def _dt(s):
    return dt.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc)


class AtlasAPI:
    """Solo richieste GET, con digest auth. Le chiavi si leggono solo dall'ambiente e non si scrivono mai."""

    def __init__(self, cartella, pausa=0.7):
        self.cartella = cartella
        pub, priv = os.environ.get("ATLAS_PUBLIC_KEY"), os.environ.get("ATLAS_PRIVATE_KEY")
        self.ok = bool(pub and priv)
        self.indice = []
        self.n = 0
        self.pausa = pausa
        self._ultimo = 0.0
        os.makedirs(cartella, exist_ok=True)
        if self.ok:
            mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
            mgr.add_password(None, ATLAS_BASE, pub, priv)
            self.digest = urllib.request.HTTPDigestAuthHandler(mgr)
            self.opener = urllib.request.build_opener(self.digest)

    def get(self, percorso, params=None, binario=False):
        if STOP.is_set():  # p1-1.6: Ctrl+C durante una raccolta: niente altre richieste, si salva quello che c'e'
            return None, None
        q = urllib.parse.urlencode(params or {}, doseq=True)
        url = f"{ATLAS_BASE}/api/atlas/v2{percorso}" + (f"?{q}" if q else "")
        tentativi = []
        stato, corpo, accept = None, b"", None
        for v in VERSIONI_API:
            accept = f"application/vnd.atlas.{v}+{'gzip' if binario else 'json'}"
            for rip in range(4):
                attesa = self.pausa - (time.time() - self._ultimo)
                if attesa > 0:
                    time.sleep(attesa)
                t0 = iso_us()
                # urllib non azzera mai il contatore dei tentativi digest: senza questa riga, dopo circa 6
                # richieste tutte le chiamate fallirebbero con 401 (trovato nella prova locale)
                self.digest.reset_retry_count()
                try:
                    req = urllib.request.Request(url, headers={"Accept": accept, "User-Agent": "p1-raccolta/1.5"})
                    with self.opener.open(req, timeout=180) as r:
                        stato, corpo = r.status, r.read()
                except urllib.error.HTTPError as e:
                    stato = e.code
                    try:
                        corpo = e.read()
                    except Exception:
                        corpo = b""
                except Exception as e:
                    stato, corpo = None, pulito(e).encode()
                self._ultimo = time.time()
                tentativi.append({"accept": accept, "inizio": t0, "fine": iso_us(), "stato_http": stato,
                                  "byte": len(corpo or b"")})
                if stato == 429 or stato is None or stato >= 500:
                    time.sleep(min(60, 5 * 2 ** rip))
                    continue
                break
            if stato == 200:
                break
            t = (corpo or b"")[:3000].decode("utf-8", "replace").lower()
            if stato in (406, 415) or (stato == 400 and "version" in t):
                continue  # versione dell'API non accettata da questo endpoint: provo la successiva
            break
        self.n += 1
        base = f"{self.n:04d}_" + re.sub(r"[^A-Za-z0-9]+", "_", percorso).strip("_")[:110]
        rec = {"url": url, "metodo": "GET", "accept": accept, "stato_http": stato, "tentativi": tentativi}
        dati = None
        if stato == 200 and binario:
            with open(os.path.join(self.cartella, base + ".gz"), "wb") as f:
                f.write(corpo)
            rec["file_binario"] = base + ".gz"
            rec["byte"] = len(corpo)
        else:
            try:
                dati = json.loads(corpo.decode("utf-8"))
                rec["risposta"] = dati
            except Exception:
                rec["risposta_testo"] = (corpo or b"").decode("utf-8", "replace")
        scrivi_json(os.path.join(self.cartella, base + ".json"), rec)
        self.indice.append({"n": self.n, "url": url, "stato_http": stato, "file": base + ".json"})
        return stato, dati

    def tutte(self, percorso, params=None, per_pagina=500, max_pagine=40):
        out = []
        for pg in range(1, max_pagine + 1):
            pr = dict(params or {})
            pr.update({"itemsPerPage": per_pagina, "pageNum": pg})
            s, d = self.get(percorso, pr)
            if s != 200 or not isinstance(d, dict):
                break
            res = d.get("results") or []
            out.extend(res)
            tot = d.get("totalCount")
            if not res or len(res) < per_pagina or (tot is not None and len(out) >= tot):
                break
        return out

    def misure(self, base, da, fino):
        """Prima l'elenco delle misure, poi tutte, a finestre di 6 ore, alla granularita' piu' fine permessa."""
        nomi, trovato = [], False
        for g in ("PT1M", "PT5M", "PT1H"):
            s, d = self.get(base + "/measurements", {"granularity": g, "period": "PT1H"})
            if s == 200 and isinstance(d, dict):
                nomi = [m.get("name") for m in d.get("measurements", []) if m.get("name")]
                trovato = True
                break
        if not trovato:
            return False  # processo inesistente o misure non leggibili: gli errori sono salvati
        t = da
        while t < fino:
            t2 = min(fino, t + dt.timedelta(hours=6))
            grane = (("PT10S",) if t >= ora() - dt.timedelta(hours=8) else ()) + ("PT1M", "PT5M", "PT1H")
            for g in grane:
                pr = {"granularity": g, "start": iso(t), "end": iso(t2)}
                if nomi:
                    pr["m"] = nomi
                s, _ = self.get(base + "/measurements", pr)
                if s == 400 and nomi:
                    pr.pop("m")
                    s, _ = self.get(base + "/measurements", pr)
                if s == 200:
                    break
            t = t2
        return trovato


def raccolta_atlas(cartella, a, giro=None):
    """Tutto quello che l'Atlas Admin API espone, salvato grezzo. Se manca la chiave, lo dice e va avanti."""
    at = AtlasAPI(cartella)
    if not at.ok:
        msg = ("Mancano ATLAS_PUBLIC_KEY e ATLAS_PRIVATE_KEY nell'ambiente: salto la raccolta dall'Atlas Admin API "
               "(si puo' rifare dopo con: scarica_atlas).")
        stampa(msg)
        scrivi_json(os.path.join(cartella, "NOTA.json"), {"fatto": False, "motivo": msg, "t": iso_us()})
        return {"fatto": False, "motivo": msg}
    stampa(f"Atlas Admin API: raccolta in {cartella} (qualche minuto)...")
    t_inizio = iso_us()
    da = _dt(a.atlas_da)
    fino = ora().replace(microsecond=0)  # mai nel futuro: un 'end' futuro puo' dare 400 e far perdere l'ultima finestra
    gids = [os.environ["ATLAS_PROJECT_ID"]] if os.environ.get("ATLAS_PROJECT_ID") else \
        [p.get("id") for p in at.tutte("/groups") if p.get("id")]
    if not gids:
        stampa("Atlas Admin API: nessun progetto leggibile con questa chiave (vedi i file in", cartella, ").")
    extra = set()
    if giro is not None:  # i nodi visti dal driver (anche quelli che entrano ed escono nel cambio di taglia)
        for x in giro.reg.topologia + giro.reg_c.topologia:
            if x[2] and re.match(r"^[\w.-]+:\d+$", str(x[2])):
                extra.add(str(x[2]))
    for gid in gids:
        _, prog = at.get(f"/groups/{gid}")
        org = prog.get("orgId") if isinstance(prog, dict) else None
        clusters = at.tutte(f"/groups/{gid}/clusters")
        at.tutte(f"/groups/{gid}/flexClusters")
        nomi_c = sorted({c.get("name") for c in clusters if c.get("name")} |
                        ({os.environ["ATLAS_CLUSTER"]} if os.environ.get("ATLAS_CLUSTER") else set()))
        sec_da, sec_a = int(da.timestamp()), int(fino.timestamp())
        for nc in nomi_c:
            for suf in ("", "/processArgs", "/status", "/backup/snapshots", "/backup/schedule",
                        "/autoScalingConfiguration"):
                at.get(f"/groups/{gid}/clusters/{nc}{suf}")
            at.get(f"/groups/{gid}/clusters/{nc}/queryShapeInsights/summaries",
                   {"since": sec_da * 1000, "until": sec_a * 1000})
            for vista in ("PRIMARY", "SECONDARY"):
                at.get(f"/groups/{gid}/clusters/{nc}/{vista}/collStats/namespaces", {"start": iso(da), "end": iso(fino)})
        procs = at.tutte(f"/groups/{gid}/processes")
        candidati = {p.get("id") for p in procs if p.get("id")} | extra
        for p in list(candidati):
            host, _, porta = p.rpartition(":")
            mm = re.search(r"shard-(\d\d)-(\d\d)", host)
            if mm:
                for k in range(5):
                    candidati.add(host.replace(mm.group(0), f"shard-{mm.group(1)}-{k:02d}") + ":" + porta)
        for pid in sorted(candidati):
            pb = f"/groups/{gid}/processes/{pid}"
            at.get(pb)
            if not at.misure(pb, da, fino):
                continue
            for dsk in at.tutte(pb + "/disks"):
                if dsk.get("partitionName"):
                    at.misure(f"{pb}/disks/{dsk['partitionName']}", da, fino)
            for dbx in at.tutte(pb + "/databases"):
                if dbx.get("databaseName"):
                    at.misure(f"{pb}/databases/{dbx['databaseName']}", da, fino)
            pa = pb + "/performanceAdvisor"
            at.get(pa + "/namespaces", {"since": sec_da * 1000})
            at.get(pa + "/suggestedIndexes", {"since": sec_da * 1000})
            at.get(pa + "/slowQueryLogs", {"since": sec_da * 1000, "nLogs": 20000})
            at.get(pb + "/collStats/namespaces", {"start": iso(da), "end": iso(fino)})
        for h in sorted({c.rpartition(":")[0] for c in candidati}):
            for log in ("mongodb", "mongodb-audit-log"):
                at.get(f"/groups/{gid}/clusters/{h}/logs/{log}.gz", {"startDate": sec_da, "endDate": sec_a},
                       binario=True)
        at.tutte(f"/groups/{gid}/events", {"minDate": iso(da), "maxDate": iso(fino)})
        at.tutte(f"/groups/{gid}/alerts")
        at.tutte(f"/groups/{gid}/alertConfigs")
        at.get(f"/groups/{gid}/limits")
        if org:
            at.get(f"/orgs/{org}")
            fatture = at.tutte(f"/orgs/{org}/invoices")
            at.get(f"/orgs/{org}/invoices/pending")
            for f in fatture:
                if f.get("id") and (str(f.get("startDate", "")) >= "2026-11" or f.get("statusName") == "PENDING"):
                    at.get(f"/orgs/{org}/invoices/{f['id']}")
            at.tutte(f"/orgs/{org}/events", {"minDate": iso(da), "maxDate": iso(fino)})
    ok = sum(1 for x in at.indice if x["stato_http"] == 200)
    res = {"fatto": True, "inizio": t_inizio, "fine": iso_us(), "chiamate": len(at.indice), "riuscite": ok,
           "non_riuscite": len(at.indice) - ok, "da": iso(da), "fino": iso(fino)}
    scrivi_json(os.path.join(cartella, "indice.json"), {"riassunto": res, "chiamate": at.indice})
    stampa(f"Atlas Admin API: {ok} chiamate riuscite su {len(at.indice)} (gli errori sono salvati: alcuni "
           f"endpoint non esistono per Infinite o chiedono ruoli in piu').")
    return res


def cmd_scarica_atlas(a):
    cartella = os.path.join(a.cartella_dati, f"atlas_{ora().strftime('%Y%m%dT%H%M%SZ')}")
    raccolta_atlas(cartella, a)
    print("Salvato in", os.path.abspath(cartella))


# ----------------------------------------------------------------------------- giro p1-1.6: il comando unico

# L'ordine delle fasi del giro finale su M30 e i minuti STIMATI di ognuna (pausa di 2 minuti compresa dove c'e').
# Le stime vengono dai tempi delle prove di ieri (rete del Mac circa 37 ms, MISURATO) e dal numero di ricerche;
# sono scritte anche in GIRO-FINALE.md.
# cancellazione prima di grandi_lavaggio: la scansione intera del lavaggio scalderebbe le pagine dei clienti da cancellare
# p1-1.6.1: condizioni (L49-L51) e mentre_scrivi (M1-M4) dopo spiega e prima di parallelo e carico_massimo, che
# scaldano; grandi_carica e la parte 'carica' di freddo si fanno su M10 prima del cambio (GIRO-FINALE, passo D):
# nel giro su M30 grandi_carica dice "gia' caricata" e il lavaggio non scansiona se i nodi sono ripartiti dopo.
FASI_GIRO = ("atlas_inizio", "inizio", "Q2", "Q3", "Q1", "Q1d", "Q4", "Q5", "spiega", "condizioni", "mentre_scrivi",
             "parallelo", "carico_massimo", "scritture", "cancellazione", "grandi_carica", "grandi_lavaggio",
             "grandi_Q2", "grandi_Q1", "grandi_Q3", "grandi_Q4", "freddo", "fine", "atlas_fine")
MINUTI_STIMATI = {"atlas_inizio": 10, "inizio": 3, "Q2": 5, "Q3": 5, "Q1": 5, "Q1d": 5, "Q4": 5, "Q5": 4.5,
                  "spiega": 5.5, "condizioni": 9, "mentre_scrivi": 11, "parallelo": 16, "carico_massimo": 15,
                  "scritture": 27, "grandi_carica": 0.5, "grandi_lavaggio": 0.5, "grandi_Q2": 4.5, "grandi_Q1": 4.5,
                  "grandi_Q3": 4.5, "grandi_Q4": 4.5, "freddo": 2, "cancellazione": 4, "fine": 3, "atlas_fine": 10}
# se grandi_carica NON e' stata fatta su M10: carico circa 9 minuti e lavaggio circa 6 (dentro il giro su M30)
MINUTI_GRANDI_SU_M30 = 15
MINUTI_FREDDO_ACCETTATA = 8  # accettata e copiata su M10: letture, modifica, cancellazione (33 se tutto su M30)
PAUSA_PRIMA = {"Q2", "Q3", "Q1", "Q1d", "Q4", "Q5", "spiega", "condizioni", "mentre_scrivi", "parallelo",
               "carico_massimo", "scritture", "grandi_carica", "grandi_Q2", "grandi_Q1", "grandi_Q3", "grandi_Q4",
               "freddo", "cancellazione"}
FASI_LETTURA = ("Q2", "Q3", "Q1", "Q1d", "Q4", "Q5", "grandi_Q2", "grandi_Q1", "grandi_Q3", "grandi_Q4")
COLL_GRANDI, PREF_GRANDI = "events_large", "acct-"  # nomi neutri (RULES)
COLL_SCRITTURE = {"normale": "scratch_writes", "per_cliente": "scratch_keyed"}
COLL_SCRITTURE_SFONDO = "scratch_stream"  # p1-1.6.1: le scritture continue di mentre_scrivi (nome neutro, cancellata)


def file_fase(cartella, indice, fase):
    """Nome del file di una fase; se c'e' gia' (fase rifatta dopo una ripresa) aggiunge _r2, _r3..."""
    nome = f"fase_{indice:02d}_{fase}.json"
    k = 2
    while os.path.exists(os.path.join(cartella, nome)):
        nome = f"fase_{indice:02d}_{fase}_r{k}.json"
        k += 1
    return nome


def _lista_int(s):
    return [int(x) for x in str(s).split(",") if x.strip()]


def _intercala(attivi, dormienti):
    """Mette i dormienti in mezzo agli attivi a intervalli regolari: i due gruppi vedono lo stesso bancone."""
    if not dormienti:
        return list(attivi)
    out, passo, j = [], (len(attivi) + len(dormienti)) / len(dormienti), 0
    for k in range(len(attivi) + len(dormienti)):
        if j < len(dormienti) and k >= int(j * passo + passo / 2):
            out.append(dormienti[j])
            j += 1
        else:
            out.append(attivi[k - j] if k - j < len(attivi) else dormienti[j])
    return out


def piano_giro16(st, a, nome, fasi):
    """I clienti di ogni fase: attivi e dormienti MAI letti (ne' nei giri 0h e 2h ne' prima in questo giro), ognuno
    in una sola fase. Su una ripresa l'elenco dei clienti usati si rilegge dal file di stato."""
    cl = st.d["clienti"]
    n = cl["n"]
    ris = set(cl["riservati"])
    q2 = [ObjectId(x) if x else None for x in cl["q2_id"]]
    usati = set(st.d.get("usati", [])) | caldi_dei_giri_vecchi(st)  # E2 del revisore: i 40 caldi di ieri
    rng = random.Random(zlib.crc32(f"{VERSIONE}|{nome}|{len(usati)}".encode()))
    attivi = [i for i in range(n) if i not in ris and i not in usati and q2[i] is not None]
    rng.shuffle(attivi)
    dorm = [i for i in sorted(ris) if i not in usati and q2[i] is not None]
    rng.shuffle(dorm)
    bc = cl.get("byte") or [None] * n
    avvisi, pa, pd = [], [0], [0]
    conti = {"attivi_mai_letti": len(attivi), "dormienti_mai_letti": len(dorm)}

    def prendi(lista, pos, k, per):
        x = lista[pos[0]:pos[0] + k]
        pos[0] += len(x)
        if len(x) < k:
            avvisi.append(f"{per}: chiesti {k} clienti mai letti, disponibili {len(x)}")
        return x

    p = {}
    for q, na, nd in (("Q2", a.n_q2, a.dorm_q2), ("Q3", a.n_q3, a.dorm_q3), ("Q1", a.n_q1, a.dorm_q1),
                      ("Q1d", a.n_q1d, a.dorm_q1d), ("Q4", a.n_q4, a.dorm_q4)):
        if q not in fasi:
            continue
        att = [compito(q, i, "attivi", nome, q2_id=q2[i], byte_cliente=bc[i]) for i in prendi(attivi, pa, na, q)]
        dor = [compito(q, i, "dormienti", nome, q2_id=q2[i], byte_cliente=bc[i], riservato=True)
               for i in prendi(dorm, pd, nd, f"{q} dormienti")]
        p[q] = _intercala(att, dor)
    if "Q5" in fasi:
        p["Q5"] = [compito("Q5", None, "finestre", nome, a=a, k=k, n=a.n_q5) for k in range(a.n_q5)]
    if "spiega" in fasi:
        sp = []
        for q in ("Q1", "Q1d", "Q3", "Q4", "Q2"):  # clienti nuovi: explain freddo e poi caldo
            # p1-1.6.1 (L45, errore 1 di A, E6 di B): anche Q2 su ordini mai letti, freddo e caldo
            for i in prendi(attivi, pa, a.n_spiega, f"spiega {q}"):
                c = compito(q, i, "attivi", nome + "|spiega", q2_id=q2[i], byte_cliente=bc[i])
                c["freddo_vero"] = True
                sp.append(c)
        # p1-1.6.1 (L48, E6 di B): Q5 freddo e caldo su finestre di uno stato MAI letto (le 60 finestre della fase Q5
        # coprono quasi tutto l'anno dello stato a.q5_stato: li' una finestra mai letta non c'e')
        st5 = stato_spiega_q5(a)
        for k in range(min(a.n_spiega, a.n_q5)):
            c = compito("Q5", None, "finestre", nome + "|spiega", a=a, k=k, n=a.n_q5, stato=st5)
            c["freddo_vero"] = True
            sp.append(c)
        p["spiega"] = sp
    if "condizioni" in fasi:
        # p1-1.6.1 (L49, L50, L51; errore 1 di A, E1 di B): 50 clienti attivi mai letti per cella
        p["condizioni"] = {
            "L49_Q1_secondario": [compito("Q1", i, "attivi", nome + "|cond", byte_cliente=bc[i])
                                  for i in prendi(attivi, pa, a.n_condizione, "condizioni L49 Q1 dal secondario")],
            "L50_Q2_secondario": [compito("Q2", i, "attivi", nome + "|cond", q2_id=q2[i])
                                  for i in prendi(attivi, pa, a.n_condizione, "condizioni L50 Q2 dal secondario")],
            "L51_Q2_majority": [compito("Q2", i, "attivi", nome + "|cond", q2_id=q2[i])
                                for i in prendi(attivi, pa, a.n_condizione, "condizioni L51 Q2 readConcern majority")]}
        for c in p["condizioni"]["L51_Q2_majority"]:
            c["spec"]["rc"] = "majority"
    if "mentre_scrivi" in fasi:
        # p1-1.6.1 (M1-M4; errore 1 di A, E2 di B): 50 clienti attivi mai letti per cella
        p["mentre_scrivi"] = {
            cella: [compito(q, i, "attivi", nome + "|mentre", q2_id=q2[i], byte_cliente=bc[i])
                    for i in prendi(attivi, pa, a.n_condizione, f"mentre_scrivi {cella}")]
            for cella, q in (("M1_Q1_w1", "Q1"), ("M2_Q2_w1", "Q2"), ("M3_Q1_majority", "Q1"),
                             ("M4_Q2_majority", "Q2"))}
    if "parallelo" in fasi:
        # per ogni livello (1, 10, 50, 100 insieme) i lotti girano Q2, Q3, Q1: ogni lotto sono 'liv' clienti diversi,
        # mai letti, che partono insieme; i lotti di un livello si distribuiscono su almeno a.par_secondi secondi
        p["parallelo"] = {}
        qs = [x.strip() for x in a.par_query.split(",") if x.strip()]
        # p1-1.6.1 (rischio di B): su una ripresa i livelli gia' finiti non si rifanno (non consumano altri clienti)
        fatti = set((st.d.get("giro_finale") or {}).get("parallelo_livelli_fatti", []))
        for liv, nl in zip(_lista_int(a.livelli), _lista_int(a.par_lotti)):
            if f"L{liv}" in fatti:
                conti.setdefault("parallelo_livelli_gia_fatti", []).append(f"L{liv}")
                continue
            lotti = []
            for b in range(nl):
                for q in qs:
                    sc = prendi(attivi, pa, liv, f"parallelo {liv} insieme {q}")
                    if sc:
                        lotti.append({"query": q, "compiti": [compito(q, i, "attivi", nome + "|parallelo", q2_id=q2[i],
                                                                      byte_cliente=bc[i]) for i in sc]})
            p["parallelo"][f"L{liv}"] = lotti
    if "cancellazione" in fasi:
        p["cancellazione"] = [compito("Q1", i, "attivi", nome + "|cancellazione") for i in
                              prendi(attivi, pa, a.n_cancella, "cancellazione")]
    conti["attivi_usati_dal_piano"] = pa[0]
    conti["dormienti_usati_dal_piano"] = pd[0]
    gr = [f for f in fasi if f.startswith("grandi_Q")]
    if gr:
        ng = (st.d.get("grandi") or {}).get("clienti") or a.grandi_clienti
        ug = set(st.d.get("usati_grandi", []))
        lib = [i for i in range(ng) if i not in ug]
        rng.shuffle(lib)
        pg = [0]
        q2g = (st.d.get("grandi") or {}).get("q2_id") or []
        for q, nn in (("Q2", a.grandi_n_q2), ("Q1", a.grandi_n_q1), ("Q3", a.grandi_n_q3), ("Q4", a.grandi_n_q4)):
            if f"grandi_{q}" not in fasi:
                continue
            sc = prendi(lib, pg, nn, f"grandi_{q}") if q != "Q2" else \
                [i for i in lib[pg[0]:] if i < len(q2g) and q2g[i]][:nn]
            if q == "Q2":
                lib_set = set(sc)
                lib[:] = [i for i in lib if i not in lib_set]
                if len(sc) < nn:
                    avvisi.append(f"grandi_Q2: chiesti {nn} clienti con un _id noto, disponibili {len(sc)}"
                                  + ("" if q2g else " (gli _id si scrivono durante grandi_carica)"))
            p[f"grandi_{q}"] = [compito(q, i, "grandi", nome + "|grandi", coll=COLL_GRANDI, prefisso=PREF_GRANDI,
                                        q2_id=ObjectId(q2g[i]) if q == "Q2" else None) for i in sc]
        conti["grandi_mai_letti"] = len(lib)
    return p, avvisi, conti


def quanti_nel_piano(n):
    """Quante ricerche (o clienti) ha una voce del piano: lista, lotti del parallelo o celle."""
    if n is None:
        return None
    if isinstance(n, dict):
        return sum(sum(len(lo["compiti"]) for lo in v) if v and "compiti" in v[0] else len(v) for v in n.values())
    return len(n)


def stato_spiega_q5(a):
    """Lo stato degli ordini per l'explain freddo di Q5: il primo dopo 'spedito' diverso da quello della fase Q5."""
    for x in ("spedito", "pagato", "consegnato", "reso", "nuovo"):
        if x != a.q5_stato:
            return x


def stima_minuti(fasi):
    return sum(MINUTI_STIMATI.get(f, 0) for f in fasi)


# ------------------------------------------------------------------ letture in parallelo (1, 10, 50 insieme)

def riassumi_parallelo(rec):
    out = {"fase": "parallelo", "inizio": rec.get("inizio"), "fine": rec.get("fine"), "livelli": []}
    for lv in rec.get("livelli", []):
        prof = indice_profilo(lv.get("profiler", []))
        zero = riporta_storage(prof)
        r = {"insieme": lv.get("livello"), "clienti": len({m.get("i") for m in lv.get("misure", []) if m.get("i") is not None}),
             "clienti_previsti": lv.get("clienti"), "lotti": lv.get("lotti_fatti"),
             "connessioni_aperte_prima": lv.get("connessioni_pronte"), "inizio": lv.get("inizio"),
             "fine": lv.get("fine"), "errore": lv.get("errore"), "per_query": {},
             "serverStatus_cpu_e_operazioni": cpu_per_operazione(lv.get("serverStatus_prima"),
                                                                  lv.get("serverStatus_dopo"))}
        for q in sorted({m.get("query") for m in lv.get("misure", []) if m.get("query")}):
            d = {}
            for tipo in ("freddo", "caldo"):
                x = [m for m in lv.get("misure", []) if m.get("tipo") == tipo and m.get("query") == q]
                ok = [m for m in x if "ms" in m]
                nums = [numeri_profilo(prof.get(m["commento"]), zero) for m in ok]
                tr = [y for y in nums if y]
                pos = [y for y in tr if y["bytesRead"]]
                d[tipo] = {"n": len(x), "errori": len(x) - len(ok),
                           "server_profiler_millis": stat([y["millis"] for y in tr]),
                           "server_ms_di_lettura_storage": stat([y["timeReadingMicros"] / 1000 for y in tr
                                                                 if y["timeReadingMicros"] is not None]),
                           "server_byte_letti_storage": stat([y["bytesRead"] for y in tr]),
                           **conta_zero(tr),  # p1-1.6.1: quota di find a 0 byte dalla dispensa (A, correzione 12)
                           "server_profiler_millis_solo_byte_maggiori_di_0": stat([y["millis"] for y in pos]),
                           "server_ms_di_lettura_storage_solo_byte_maggiori_di_0": stat(
                               [y["timeReadingMicros"] / 1000 for y in pos if y["timeReadingMicros"] is not None]),
                           "trovate_nel_profiler": len(tr),
                           "cliente_ms_RETE_DEL_MAC": stat([m["ms"] for m in ok]),
                           "documenti": stat([m["doc"] for m in ok])}
            d["controlli_falliti"] = sum(1 for m in lv.get("misure", []) if m.get("query") == q
                                         and m.get("controllo_ok") is False)
            r["per_query"][q] = d
        out["livelli"].append(r)
    return out


def stampa_parallelo(rec):
    r = riassumi_parallelo(rec)
    print()
    print(f"=== FASE parallelo (letture fredde di clienti diversi che partono insieme): {r.get('inizio')} - "
          f"{r.get('fine')} UTC ===")
    print("   Il giudizio si fa sul tempo lato server (profiler). Il tempo lato cliente e' la rete del Mac.")
    for lv in r["livelli"]:
        print(f"   {lv['insieme']} insieme: {lv['clienti']} clienti, {lv['lotti']} lotti, connessioni aperte prima "
              f"{lv['connessioni_aperte_prima']}; {lv['inizio']} - {lv['fine']}" +
              (f"; ERRORE {lv['errore']}" if lv.get("errore") else ""))
        for q, d in lv["per_query"].items():
            for tipo in ("freddo", "caldo"):
                x = d[tipo]
                print(f"     {q} {tipo}: server millis p50 {_r(x['server_profiler_millis'])} p95 "
                      f"{_r(x['server_profiler_millis'], 'p95')} p99 {_r(x['server_profiler_millis'], 'p99')}; "
                      f"lettura storage p50 {_r(x['server_ms_di_lettura_storage'])} p99 "
                      f"{_r(x['server_ms_di_lettura_storage'], 'p99')} ms; profiler {x['trovate_nel_profiler']}/{x['n']}; "
                      f"a 0 byte dalla dispensa {x['find_a_0_byte']}/{x['su']}; solo byte>0: millis p50 "
                      f"{_r(x['server_profiler_millis_solo_byte_maggiori_di_0'])}; "
                      f"errori {x['errori']}; [rete del Mac: p50 {_r(x['cliente_ms_RETE_DEL_MAC'])} p99 "
                      f"{_r(x['cliente_ms_RETE_DEL_MAC'], 'p99')} ms]")
            if d["controlli_falliti"]:
                print(f"     {q}: controlli falliti {d['controlli_falliti']}")
    print()


def livello_parallelo(g, a, liv, lotti):
    """Ogni lotto: 'liv' clienti diversi mai letti partono insieme (barriera) a freddo, poi le stesse 'liv' ricerche
    di nuovo insieme (caldo). I lotti girano Q2, Q3, Q1 e si distribuiscono su almeno a.par_secondi secondi.
    Prima: connessioni aperte e scaldate, con un client a parte (pool = liv) e i suoi eventi del pool."""
    from concurrent.futures import ThreadPoolExecutor
    out = {"livello": liv, "clienti": sum(len(x["compiti"]) for x in lotti), "lotti_previsti": len(lotti),
           "inizio_preparazione": iso_us(), "misure": [], "lotti_fatti": 0}
    reg = Registro(f"parallelo_{liv}")
    # p1-1.6.1 (correzione 14 di A): pool di liv + 10 connessioni (il prompt chiede almeno 110 al livello 100)
    cl = MongoClient(os.environ[ENV_URI], appname=APPNAME, compressors=compressori(), maxPoolSize=liv + 10,
                     minPoolSize=liv, serverSelectionTimeoutMS=30000, connectTimeoutMS=20000,
                     socketTimeoutMS=600000, w=1, event_listeners=reg.ascoltatori)
    try:
        bar = threading.Barrier(liv)

        def scalda():
            try:
                bar.wait(timeout=60)
            except threading.BrokenBarrierError:
                pass
            for _ in range(5):
                cl.admin.command("ping")
        ths = [threading.Thread(target=scalda, daemon=True) for _ in range(liv)]
        for t in ths:
            t.start()
        for t in ths:
            t.join(timeout=120)
        time.sleep(1)
        out["connessioni_pronte"] = sum(1 for x in reg.pool if x[0] == "connessione_pronta")
        stampa(f"  {liv} insieme: connessioni aperte e scaldate {out['connessioni_pronte']} (servono {liv})")
        out["serverStatus_prima"] = _prova(lambda: g.mon.admin.command("serverStatus"))
        out["profiler_acceso"] = g.profiler_acceso()
        intervallo = a.par_secondi / max(1, len(lotti))
        out["inizio"] = iso_us()
        stampa(f"  PARALLELO {liv} INSIEME INIZIO {dt.datetime.now().strftime('%H:%M:%S')} ora del Mac "
               f"({out['inizio'][:19]}Z): {out['clienti']} clienti in {len(lotti)} lotti (Q2, Q3, Q1 a turno), "
               f"uno ogni {intervallo:.1f} s")

        def una(c, b, tipo, barriera):
            spec, q = c["spec"], c["query"]
            com = f"p1|par|{liv}|{q}|{c.get('cliente')}|{b}|{tipo}"
            kw = {"limit": spec["limit"], "batch_size": spec["limit"], "comment": com}
            if spec["sort"]:
                kw["sort"] = spec["sort"]
            coll = cl[a.db].get_collection(spec["coll"], codec_options=RAWOPT)
            try:
                barriera.wait(timeout=60)
            except threading.BrokenBarrierError:
                pass
            r = {"query": q, "cliente": c.get("cliente"), "i": c.get("i"), "lotto": b, "tipo": tipo,
                 "commento": com, "thread": threading.current_thread().name,
                 "ricerca": {"collezione": spec["coll"], "filtro": spec["filtro"], "ordine": spec["sort"],
                             "limit": spec["limit"]}}
            t0s = time.time_ns()
            t0 = time.perf_counter_ns()
            try:
                docs = list(coll.find(spec["filtro"], **kw))
                r["ms"] = round(ms_da(t0), 4)
                r["doc"] = len(docs)
                r["byte"] = sum(len(d.raw) for d in docs)
                r["primo_id"] = docs[0]["_id"] if docs else None
            except Exception as e:
                r["errore"] = pulito(e)
            r["inizio"], r["fine"] = iso_ns(t0s), iso_us()
            return r

        p0 = time.perf_counter()
        with ThreadPoolExecutor(max_workers=liv, thread_name_prefix=f"par{liv}") as ex:
            for b, lotto in enumerate(lotti):
                if STOP.is_set():
                    break
                comp = lotto["compiti"]
                if g.prenota:  # p1-1.6.1: i clienti del lotto si segnano su file PRIMA delle letture fredde
                    g.prenota(comp)
                ris = {}
                for tipo in ("freddo", "caldo"):
                    barriera = threading.Barrier(len(comp))
                    ris[tipo] = [x.result() for x in [ex.submit(una, c, b, tipo, barriera) for c in comp]]
                for rf, rc in zip(ris["freddo"], ris["caldo"]):
                    # controllo: Q2 un documento; Q1 20 messaggi; il caldo uguale al freddo
                    ok = "ms" in rf and "ms" in rc and rf["doc"] == rc["doc"] and rf["primo_id"] == rc["primo_id"]
                    if rf["query"] == "Q2":
                        ok = ok and rf["doc"] == 1
                    if rf["query"] == "Q1":
                        ok = ok and rf["doc"] == 20
                    rf["controllo_ok"] = ok
                out["misure"] += ris["freddo"] + ris["caldo"]
                out["lotti_fatti"] = b + 1
                if all("errore" in x for x in ris["freddo"]):
                    out["fermato"] = "tutte le letture fredde del lotto in errore (rete o cluster): livello fermato"
                    stampa(f"  ATTENZIONE: {out['fermato']} ({ris['freddo'][0].get('errore')})")
                    break
                for c in comp:
                    if g.segna:
                        g.segna(c)
                attesa = p0 + (b + 1) * intervallo - time.perf_counter()
                if attesa > 0:
                    STOP.wait(attesa)
        out["fine"] = iso_us()
        stampa(f"  PARALLELO {liv} INSIEME FINE {dt.datetime.now().strftime('%H:%M:%S')} ora del Mac "
               f"({out['fine'][:19]}Z), {out['lotti_fatti']} lotti")
    finally:
        out["profiler_spento"] = g.profiler_spento()
        prof = []
        if (out.get("profiler_acceso") or {}).get("acceso"):
            out["profiler_errori_lettura"] = g.leggi_profilo(set(), prof)
        out["profiler"] = prof
        out["serverStatus_dopo"] = _prova(lambda: g.mon.admin.command("serverStatus"))
        out["eventi_driver"] = reg.esporta()
        try:
            cl.close()
        except Exception:
            pass
    return out


def fase_parallelo(g, a, st, piano, nome, indice):
    rec = {"fase": "parallelo", "tipo": "parallelo", "versione": VERSIONE, "giro": g.nome, "livelli": [],
           "nota": "giudizio sui tempi lato server (profiler); il tempo lato cliente e' la rete del Mac"}
    rec["prima"] = g.istantanea("prima di parallelo")
    gia = (st.d.get("giro_finale") or {}).get("parallelo_livelli_fatti", [])
    if gia:
        rec["livelli_gia_fatti_in_un_file_prima"] = list(gia)
        stampa(f"  Ripresa: livelli gia' finiti prima, non li rifaccio: {', '.join(gia)}")
    da_c, c0 = g.reg_c.lunghezze(), len(g.campioni)
    rec["inizio"] = iso_us()
    rec["file"] = file_fase(g.cartella, indice, "parallelo")
    try:
        for k, (chiave, lotti) in enumerate(piano.get("parallelo", {}).items()):
            if STOP.is_set():
                break
            liv = int(chiave[1:])
            if k > 0:
                p0 = iso_us()
                g.fase = f"pausa prima di parallelo {chiave}"
                stampa(f"  Pausa di {a.pausa_s:g} s prima del livello {liv} insieme.")
                dorme(a.pausa_s)
                rec.setdefault("pause", []).append({"prima_di": chiave, "inizio": p0, "fine": iso_us()})
                if STOP.is_set():
                    break
            g.fase = f"parallelo {chiave}"
            try:
                rec["livelli"].append(livello_parallelo(g, a, liv, lotti))
                if rec["livelli"][-1].get("fermato"):
                    rec["fermata"] = rec["livelli"][-1]["fermato"]
                    scrivi_json(os.path.join(g.cartella, rec["file"]), rec)
                    break
                lv = rec["livelli"][-1]
                if not STOP.is_set() and lv.get("lotti_fatti") == lv.get("lotti_previsti"):
                    # p1-1.6.1 (rischio di B): livello finito, segnato: una ripresa non lo rifa' e non spende clienti
                    gfx = st.d.setdefault("giro_finale", {})
                    gfx.setdefault("parallelo_livelli_fatti", [])
                    if chiave not in gfx["parallelo_livelli_fatti"]:
                        gfx["parallelo_livelli_fatti"].append(chiave)
                    lv["file_del_livello"] = rec["file"]
            except Exception as e:  # un livello che fallisce non ferma gli altri
                rec["livelli"].append({"livello": liv, "errore": pulito(e),
                                       "traccia": scrub(traceback.format_exc())[-3000:], "misure": []})
                stampa(f"  ERRORE nel livello {liv} insieme: {pulito(e)} (vado avanti)")
            scrivi_json(os.path.join(g.cartella, rec["file"]), rec)  # salvato dopo ogni livello
    finally:
        rec["fine"] = iso_us()
        rec["dopo"] = g.istantanea("dopo parallelo")
        rec["eventi_driver"] = [g.reg_c.esporta(da_c)]
        rec["serverStatus_ogni_30s"] = g.campioni[c0:]
        rec["riassunto"] = riassumi_parallelo(rec)
        scrivi_json(os.path.join(g.cartella, rec["file"]), rec)
    stampa_parallelo(rec)
    return rec


# ------------------------------------------------------------------ p1-1.6.1: letture in altre condizioni (L49-L51)

def secondari_di(g):
    """I secondari dal hello del primario (hosts e passives, tolto il primario)."""
    he = _prova(lambda: g.mon.admin.command("hello"))
    if not isinstance(he, dict) or "errore" in he:
        return [], he
    prim = he.get("primary")
    return [h for h in list(he.get("hosts") or []) + list(he.get("passives") or []) if h != prim], he


def fase_condizioni(g, a, st, piano, nome, indice):
    """L49 Q1 freddo e caldo dal secondario, L50 Q2 dal secondario, L51 Q2 con readConcern majority (primario).
    Il profiler e' per nodo: per L49 e L50 un client diretto a UN secondario (directConnection, credenziali dalla
    stringa di connessione, mai scritte), con profiler, hello e serverStatus di quel nodo. Se il client diretto non
    si apre: le stesse letture con readPreference secondary dal replica set (solo i tempi dal Mac, scritto). Se
    Atlas rifiuta: risposta salvata, cella NON POSSIBILE. Ogni cella salva il suo file fase_NN_condizioni_*.json."""
    blocchi = piano.get("condizioni", {})
    rec = {"fase": "condizioni", "tipo": "condizioni", "versione": VERSIONE, "giro": g.nome, "celle": {},
           "file": file_fase(g.cartella, indice, "condizioni")}
    rec["inizio"] = iso_us()
    sec, he = secondari_di(g)
    rec["hello_del_primario"] = he
    rec["secondari"] = sec
    stampa(f"FASE condizioni INIZIO {dt.datetime.now().strftime('%H:%M:%S')} ora del Mac; secondari visti: "
           f"{len(sec)}")
    gs = None
    try:
        sec_celle = [k for k in ("L49_Q1_secondario", "L50_Q2_secondario") if blocchi.get(k)]
        if sec_celle and sec:
            try:
                gs = Giro(a, "giro_finale", g.nome, cartella=g.cartella, nodo=sec[0])
                rec["secondario_scelto"] = sec[0]
                rec["hello_del_secondario"] = _prova(lambda: gs.mon.admin.command("hello"))
                gs.segna, gs.prenota = g.segna, g.prenota
                gs.avvia_campioni()
            except Exception as e:  # il client diretto non si apre: si legge dal secondario via il replica set
                rec["client_diretto_errore"] = _errore_completo(e) if isinstance(e, PyMongoError) else pulito(e)
                stampa(f"  Client diretto al secondario non riuscito ({pulito(e)[:150]}): leggo con readPreference "
                       f"secondary dal replica set (profiler del secondario non disponibile).")
                gs = None
        elif sec_celle:
            rec["nota_secondario"] = "nessun secondario nel hello del primario: readPreference secondary dal replica set"
        for cella in ("L49_Q1_secondario", "L50_Q2_secondario", "L51_Q2_majority"):
            comp = blocchi.get(cella) or []
            if STOP.is_set() or not comp:
                continue
            chi = g
            if cella.endswith("secondario"):
                if gs is not None:
                    chi = gs
                else:
                    for c in comp:
                        c["spec"]["rp"] = "secondary"
            try:
                r = chi.esegui_fase(indice, f"condizioni_{cella}", comp, min_secondi=a.min_secondi_condizione)
                ok = sum(1 for m in r.get("misure", []) if m.get("freddo"))
                rec["celle"][cella] = {"file": r.get("file"), "nodo": chi.nodo, "fredde_riuscite": ok,
                                       "ricerche": len(r.get("misure", [])), "fermata": r.get("fermata"),
                                       "primo_errore": next((m["errori"][0] for m in r.get("misure", [])
                                                             if m.get("errori")), None)}
                rec["celle"][cella]["esito"] = "fatta" if ok else \
                    "NON POSSIBILE: nessuna lettura riuscita (risposta di Atlas nel file della cella)"
                if r.get("fermata") and ok:
                    rec["fermata"] = r["fermata"]
            except Exception as e:
                rec["celle"][cella] = {"errore": pulito(e), "traccia": scrub(traceback.format_exc())[-3000:]}
                stampa(f"  ERRORE nella cella {cella}: {pulito(e)} (vado avanti)")
            scrivi_json(os.path.join(g.cartella, rec["file"]), rec)
    finally:
        if gs is not None:
            gs.ferma_campioni()
            if gs.profilo_acceso:
                gs.profiler_spento()
            rec["eventi_driver_secondario"] = [gs.reg.esporta(), gs.reg_c.esporta()]
            rec["serverStatus_secondario_ogni_30s"] = gs.campioni
            gs.chiudi()
        rec["fine"] = iso_us()
        scrivi_json(os.path.join(g.cartella, rec["file"]), rec)
    for cella, x in rec["celle"].items():
        stampa(f"  {cella}: {x.get('esito') or x.get('errore')} ({x.get('fredde_riuscite', 0)} fredde su "
               f"{x.get('ricerche', 0)}; nodo {x.get('nodo') or 'replica set'})")
    return rec


# ------------------------------------------------------------------ p1-1.6.1: letture mentre si scrive (M1-M4)

class ScrittoreDiFondo:
    """Insert a lotti di documenti da circa 340 byte (doc_messaggio, come il carico P1) a velocita' costante in una
    collezione a parte, con un write concern fisso. Ogni lotto con l'ora, il tempo e l'esito."""

    def __init__(self, a, w):
        self.a, self.w = a, w
        self.cl = connetti(pool=a.sfondo_thread + 2, esci=False)
        self.col = self.cl[a.db][COLL_SCRITTURE_SFONDO].with_options(
            write_concern=WriteConcern(w=1 if w == "w1" else "majority"))
        self.lotti, self.lock, self.fermo = [], threading.Lock(), threading.Event()
        self.k = 0
        self.ths = []

    def avvia(self):
        self.t0 = time.perf_counter()
        self.inizio = iso_us()
        rate = self.a.sfondo_lotti_s

        def lavora(n):
            r = random.Random(self.a.seme * 104729 + n + (0 if self.w == "w1" else 7))
            while not self.fermo.is_set() and not STOP.is_set():
                with self.lock:
                    k = self.k
                    self.k += 1
                attesa = self.t0 + k / rate - time.perf_counter()
                if attesa < -1.0:  # in ritardo di oltre 1 s (Atlas o la rete piu' lenti): niente raffica per recuperare
                    with self.lock:
                        self.k = max(self.k, int((time.perf_counter() - self.t0) * rate))
                if attesa > 0:
                    self.fermo.wait(attesa)
                    if self.fermo.is_set():
                        return
                docs = [doc_messaggio(r, f"w{r.randrange(100000):06d}", ora()) for _ in range(self.a.lotto_scritture)]
                b = sum(len(bson.encode(d)) for d in docs)
                x = {"k": k, "inizio": iso_us(), "documenti": len(docs), "byte": b}
                t0 = time.perf_counter_ns()
                try:
                    self.col.insert_many(docs, ordered=False, comment=f"p1|sfondo|{self.w}|{k}")
                    x["ms"] = round(ms_da(t0), 3)
                except Exception as e:
                    x["errore"] = pulito(e)
                    time.sleep(1)
                x["fine"] = iso_us()
                with self.lock:
                    self.lotti.append(x)
        self.ths = [threading.Thread(target=lavora, args=(n,), daemon=True, name=f"sfondo{n}")
                    for n in range(self.a.sfondo_thread)]
        for t in self.ths:
            t.start()

    def ferma(self):
        self.fermo.set()
        for t in self.ths:
            t.join(timeout=60)
        self.fine = iso_us()
        try:
            self.cl.close()
        except Exception:
            pass

    def riassunto(self, inizio=None, fine=None):
        with self.lock:
            xs = list(self.lotti)
        if inizio and fine:
            xs = [x for x in xs if inizio <= x["inizio"] <= fine]
            sec = (_dt_us_fine(fine) - _dt_us_fine(inizio)).total_seconds()
        else:
            sec = time.perf_counter() - self.t0
        ok = [x for x in xs if "ms" in x]
        return {"w": self.w, "lotti": len(xs), "errori": len(xs) - len(ok), "secondi": round(sec, 1),
                "lotti_al_secondo_ottenuti": round(len(ok) / sec, 2) if sec else None,
                "lotti_al_secondo_chiesti": self.a.sfondo_lotti_s,
                "documenti_al_secondo": round(sum(x["documenti"] for x in ok) / sec, 1) if sec else None,
                "mb_al_secondo": round(sum(x["byte"] for x in ok) / MB / sec, 4) if sec else None,
                "byte_per_documento": round(sum(x["byte"] for x in ok) / max(1, sum(x["documenti"] for x in ok)), 1),
                "ms_per_lotto_dal_mac": stat([x["ms"] for x in ok])}


def _dt_us_fine(s):
    return dt.datetime.strptime(str(s)[:26].rstrip("Z"), "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=dt.timezone.utc) \
        if "." in str(s)[:26] else _dt_us(s)


def fase_mentre_scrivi(g, a, st, piano, nome, indice):
    """M1 Q1 e M2 Q2 freddo e caldo mentre un filo a parte fa insert a lotti continui w=1 da circa 340 byte; M3 Q1 e
    M4 Q2 con insert w="majority". Velocita' di scrittura costante (a.sfondo_lotti_s lotti da a.lotto_scritture
    documenti al secondo), stampata e salvata, anche per ogni cella. Collezione scratch_stream, cancellata."""
    blocchi = piano.get("mentre_scrivi", {})
    rec = {"fase": "mentre_scrivi", "tipo": "mentre_scrivi", "versione": VERSIONE, "giro": g.nome, "blocchi": [],
           "collezione": COLL_SCRITTURE_SFONDO, "file": file_fase(g.cartella, indice, "mentre_scrivi")}
    rec["inizio"] = iso_us()
    rec["pulizia_iniziale"] = _prova(lambda: g.db.drop_collection(COLL_SCRITTURE_SFONDO) or "fatto")
    try:
        for kk, (w, celle) in enumerate((("w1", ("M1_Q1_w1", "M2_Q2_w1")),
                                         ("majority", ("M3_Q1_majority", "M4_Q2_majority")))):
            if STOP.is_set():
                break
            if kk:
                p0 = iso_us()
                g.fase = "pausa tra w=1 e majority"
                stampa(f"  Pausa di {a.pausa_volume_s:g} s (scritture ferme) prima del blocco w={w}.")
                dorme(a.pausa_volume_s)
                rec["pausa"] = {"inizio": p0, "fine": iso_us()}
                if STOP.is_set():
                    break
            b = {"w": w, "celle": {}}
            b["serverStatus_prima"] = _prova(lambda: g.mon.admin.command("serverStatus"))
            sc = ScrittoreDiFondo(a, w)
            sc.avvia()
            b["scritture_inizio"] = sc.inizio
            stampa(f"  SCRITTURE DI FONDO w={w} INIZIO {dt.datetime.now().strftime('%H:%M:%S')}: "
                   f"{a.sfondo_lotti_s:g} lotti da {a.lotto_scritture} documenti da circa 340 byte al secondo, "
                   f"{a.sfondo_thread} fili; letture fredde tra {a.sfondo_riscaldamento_s:g} s")
            try:
                dorme(a.sfondo_riscaldamento_s)
                for cella in celle:
                    comp = blocchi.get(cella) or []
                    if STOP.is_set() or not comp:
                        continue
                    r = g.esegui_fase(indice, f"mentre_scrivi_{cella}", comp, min_secondi=a.min_secondi_condizione)
                    v = sc.riassunto(r.get("inizio"), r.get("fine"))
                    b["celle"][cella] = {"file": r.get("file"), "scritture_durante_la_cella": v,
                                         "fermata": r.get("fermata"),
                                         "fredde_riuscite": sum(1 for m in r.get("misure", []) if m.get("freddo"))}
                    stampa(f"  {cella}: scritture durante la cella {v['lotti_al_secondo_ottenuti']} lotti/s "
                           f"(chiesti {a.sfondo_lotti_s:g}), {v['documenti_al_secondo']} documenti/s, "
                           f"{v['mb_al_secondo']} MB/s, errori {v['errori']}")
                    if r.get("fermata"):
                        rec["fermata"] = r["fermata"]
                        break
            finally:
                sc.ferma()
                b["scritture_fine"] = sc.fine
                b["scritture_riassunto"] = sc.riassunto()
                b["lotti"] = sc.lotti
                b["serverStatus_dopo"] = _prova(lambda: g.mon.admin.command("serverStatus"))
                b["cpu"] = cpu_per_operazione(b["serverStatus_prima"], b["serverStatus_dopo"])
                b["cancellata"] = _prova(lambda: g.db.drop_collection(COLL_SCRITTURE_SFONDO) or "cancellata")
                rec["blocchi"].append(b)
                scrivi_json(os.path.join(g.cartella, rec["file"]), rec)
            if rec.get("fermata"):
                break
    finally:
        rec["fine"] = iso_us()
        rec["cancellata_alla_fine"] = _prova(lambda: g.db.drop_collection(COLL_SCRITTURE_SFONDO) or "cancellata")
        scrivi_json(os.path.join(g.cartella, rec["file"]), rec)
    return rec


# ------------------------------------------------------------------ scritture: tutte le combinazioni

def riassumi_scritture(rec):
    out = {"fase": "scritture", "inizio": rec.get("inizio"), "fine": rec.get("fine"), "latenza": {}, "volume": []}
    for ps in rec.get("passi", []):
        prof = indice_profilo(ps.get("profiler", []))
        gruppi = defaultdict(list)
        for op in ps.get("operazioni", []):
            gruppi[(op["collezione"], op["w"], op.get("taglia", "2k"))].append(op)
        for (kind, w, t), ops in sorted(gruppi.items()):
            ok = [o for o in ops if "ms" in o]
            pr = [numeri_profilo(prof.get(o["commento"])) for o in ok]
            out["latenza"][f"{ps['passo']}|{kind}|{w}|{t}"] = {
                "n": len(ops), "errori": len(ops) - len(ok), "ms": stat([o["ms"] for o in ok]),
                "meno_hello_accoppiato": stat([o["ms"] - o["hello_prima"]["ms"] for o in ok if o.get("hello_prima")]),
                "profiler_millis": stat([x["millis"] for x in pr if x]), "profiler_trovate": sum(1 for x in pr if x),
                "documenti_per_operazione": stat([o.get("documenti") for o in ok]),
                "byte_per_operazione": stat([o.get("byte") for o in ok])}
    for v in rec.get("volume", []):
        out["volume"].append({k: v.get(k) for k in ("passo", "collezione", "inizio", "fine", "operazioni", "byte",
                                                    "mb_s", "errori", "thread")})
    return out


def stampa_scritture(rec):
    r = rec.get("riassunto") or riassumi_scritture(rec)
    print()
    print(f"=== FASE scritture: {r.get('inizio')} - {r.get('fine')} UTC ===")
    for k, x in r["latenza"].items():
        print(f"   {k}: n {x['n']}, errori {x['errori']}; dal cliente p50 {_r(x['ms'])} p99 {_r(x['ms'], 'p99')} ms; "
              f"meno hello p50 {_r(x['meno_hello_accoppiato'])}; profiler millis p50 {_r(x['profiler_millis'])} "
              f"p99 {_r(x['profiler_millis'], 'p99')} ({x['profiler_trovate']} trovate)")
    for v in r["volume"]:
        print(f"   volume {v['passo']} su {v['collezione']}: {v['inizio']} - {v['fine']}, {v['operazioni']} operazioni, "
              f"{(v['byte'] or 0) / MB:.1f} MB, {v['mb_s']} MB/s, errori {v['errori']}")
    print()


def fase_scritture(g, a, st, piano, nome, indice):
    """Insert singoli e a lotti, con w=1 e w="majority", con _id normale e con _id per cliente (collezione
    clustered), e $set su documenti esistenti: ogni operazione con l'ora, il tempo, un hello prima, il profiler,
    serverStatus prima e dopo. Poi tre passi di volume (profiler spento) per i Bytes Written di Atlas.
    Collezioni a parte, cancellate alla fine."""
    rec = {"fase": "scritture", "tipo": "scritture", "versione": VERSIONE, "giro": g.nome, "passi": [], "volume": [],
           "collezioni": dict(COLL_SCRITTURE)}
    rec["file"] = file_fase(g.cartella, indice, "scritture")
    rec["prima"] = g.istantanea("prima delle scritture")
    da_c, c0 = (g.reg.lunghezze(), g.reg_c.lunghezze()), len(g.campioni)
    rec["inizio"] = iso_us()
    db = g.db
    rec["pulizia_iniziale"] = {k: _prova(lambda k=k: db.drop_collection(k) or "fatto") for k in COLL_SCRITTURE.values()}
    rec["crea"] = {"normale": _prova(lambda: (db.create_collection(COLL_SCRITTURE["normale"]), "creata")[1]),
                   "indice_normale": _prova(lambda: db[COLL_SCRITTURE["normale"]].create_index(
                       [("cliente", 1), ("ts", -1)], name="cliente_ts")),
                   "per_cliente": _prova(lambda: (db.create_collection(
                       COLL_SCRITTURE["per_cliente"], clusteredIndex={"key": {"_id": 1}, "unique": True}),
                       "clustered su _id")[1])}
    tipi = ["normale"] + (["per_cliente"] if not isinstance(rec["crea"]["per_cliente"], dict) else [])
    if "per_cliente" not in tipi:
        stampa(f"La collezione clustered non si crea ({rec['crea']['per_cliente']}): salvato, vado avanti senza.")
    rng = random.Random(a.seme + 1606)
    # p1-1.6.1 (S1-S4, S9-S12, S17-S18; errore 1 di A, E3 di B): anche la taglia del documento, circa 340 byte
    # (doc_messaggio, come il carico P1) e circa 2 KB; gli _id scritti si tengono per collezione e taglia
    taglie = ("340", "2k")
    ids = {(k, t): [] for k in tipi for t in taglie}
    lock = threading.Lock()

    def nuovo_doc(kind, r=rng, taglia="2k"):
        cli = f"u{r.randrange(a.clienti_scrivi):07d}"
        d = doc_2kb(r, cli) if taglia == "2k" else doc_messaggio(r, cli, ora())
        d["_id"] = {"c": d["cliente"], "t": d["ts"], "k": r.getrandbits(48)} if kind == "per_cliente" else ObjectId()
        return d

    combos = [(k, w, t) for t in taglie for k in tipi for w in ("w1", "majority")]
    wc = {"w1": WriteConcern(w=1), "majority": WriteConcern(w="majority")}
    coll = {(k, w): db[COLL_SCRITTURE[k]].with_options(write_concern=wc[w]) for k in tipi for w in wc}

    def singolo(kind, w, t, com):
        d = nuovo_doc(kind, taglia=t)
        b = len(bson.encode(d))
        t0s, t0 = time.time_ns(), time.perf_counter_ns()
        coll[(kind, w)].insert_one(d, comment=com)
        ms = ms_da(t0)
        with lock:
            ids[(kind, t)].append(d["_id"])
        return {"ms": round(ms, 4), "inizio": iso_ns(t0s), "fine": iso_us(), "documenti": 1, "byte": b}

    def lotto(kind, w, t, com):
        docs = [nuovo_doc(kind, taglia=t) for _ in range(a.lotto_scritture)]
        b = sum(len(bson.encode(d)) for d in docs)
        t0s, t0 = time.time_ns(), time.perf_counter_ns()
        coll[(kind, w)].insert_many(docs, ordered=True, comment=com)
        ms = ms_da(t0)
        with lock:
            ids[(kind, t)].extend(d["_id"] for d in docs)
        return {"ms": round(ms, 4), "inizio": iso_ns(t0s), "fine": iso_us(), "documenti": len(docs), "byte": b}

    def aggiorna(kind, w, t, com):
        with lock:
            _id = rng.choice(ids[(kind, t)])
        t0s, t0 = time.time_ns(), time.perf_counter_ns()
        r = coll[(kind, w)].update_one({"_id": _id}, {"$set": {"stato_agg": rng.randrange(1000), "agg_ts": ora()}},
                                        comment=com)
        ms = ms_da(t0)
        return {"ms": round(ms, 4), "inizio": iso_ns(t0s), "fine": iso_us(), "documenti": 1,
                "trovati": r.matched_count, "modificati": r.modified_count, "_id": _id}

    def passo(nome_p, n_per_combo, fai):
        ps = {"passo": nome_p, "inizio": iso_us(), "operazioni": []}
        ps["serverStatus_prima"] = _prova(lambda: g.mon.admin.command("serverStatus"))
        ps["profiler_acceso"] = g.profiler_acceso()
        g.fase = f"scritture {nome_p}"
        stampa(f"  SCRITTURE {nome_p} INIZIO {dt.datetime.now().strftime('%H:%M:%S')} ora del Mac: {n_per_combo} per "
               f"combinazione ({', '.join(f'{k} w={w} {t}' for k, w, t in combos)}), alternate, un hello prima di ognuna")
        di_fila = 0
        try:
            for k in range(n_per_combo * len(combos)):
                if STOP.is_set():
                    break
                kind, w, t = combos[k % len(combos)]
                com = f"p1|scritture|{nome_p}|{kind}|{w}|{t}|{k}"
                op = {"passo": nome_p, "k": k, "collezione": kind, "w": w, "taglia": t, "commento": com}
                try:
                    op["hello_prima"] = g.hello_accoppiato()
                    op.update(fai(kind, w, t, com))
                    di_fila = 0
                except Exception as e:
                    op["errore"], op["t"] = pulito(e), iso_us()
                    di_fila += 1
                ps["operazioni"].append(op)
                if di_fila >= a.tetto_errori_di_fila:
                    ps["fermato"] = f"{di_fila} errori di fila"
                    stampa(f"  ATTENZIONE: {ps['fermato']} ({op.get('errore')}): passo fermato")
                    break
        finally:
            ps["fine"] = iso_us()
            ps["profiler_spento"] = g.profiler_spento()
            prof = []
            if (ps.get("profiler_acceso") or {}).get("acceso"):
                ps["profiler_errori_lettura"] = g.leggi_profilo(set(), prof)
            ps["profiler"] = prof
            ps["serverStatus_dopo"] = _prova(lambda: g.mon.admin.command("serverStatus"))
            rec["passi"].append(ps)
            stampa(f"  SCRITTURE {nome_p} FINE {dt.datetime.now().strftime('%H:%M:%S')}: "
                   f"{sum(1 for o in ps['operazioni'] if 'ms' in o)} riuscite su {len(ps['operazioni'])}")
            rec["riassunto"] = riassumi_scritture(rec)
            scrivi_json(os.path.join(g.cartella, rec["file"]), rec)

    def volume(nome_p, kind, aggiornamenti, pausa_prima=True):
        v = {"passo": nome_p, "collezione": COLL_SCRITTURE[kind], "thread": a.thread_volume,
             "profiler": "spento (scriverebbe anche lui nella dispensa)"}
        p0 = iso_us()
        g.fase = f"pausa prima di volume {nome_p}"
        if pausa_prima:  # p1-1.6.1: dal secondo passo la pausa 'dopo' del passo prima fa gia' da pausa 'prima'
            dorme(a.pausa_volume_s)
        v["pausa"] = {"inizio": p0, "fine": iso_us()}
        if STOP.is_set():
            return
        g.fase = f"scritture volume {nome_p}"
        v["serverStatus_prima"] = _prova(lambda: g.mon.admin.command("serverStatus"))
        v["collStats_prima"] = _prova(lambda: g.dbc.command("collStats", COLL_SCRITTURE[kind]))
        cl = connetti(pool=a.thread_volume + 2, esci=False)
        col = cl[a.db][COLL_SCRITTURE[kind]]
        fine = time.time() + a.minuti_volume * 60
        tot = {"op": 0, "byte": 0, "errori": 0, "ultimo_errore": None}
        with lock:
            bersagli = list(ids[(kind, "2k")]) + list(ids[(kind, "340")])

        def lavora(k):
            r = random.Random(a.seme * 7919 + k)
            while not STOP.is_set() and time.time() < fine:
                try:
                    if aggiornamenti:
                        col.update_one({"_id": r.choice(bersagli)}, {"$set": {"stato_agg": r.randrange(1000),
                                                                              "agg_ts": ora()}})
                        n = 40  # byte nuovi del $set, circa
                    else:
                        d = nuovo_doc(kind, r)
                        n = len(bson.encode(d))
                        col.insert_one(d)
                        if kind == "per_cliente" and len(bersagli) < 2_000_000:
                            bersagli.append(d["_id"])
                    with lock:
                        tot["op"] += 1
                        tot["byte"] += n
                except Exception as e:
                    with lock:
                        tot["errori"] += 1
                        tot["ultimo_errore"] = pulito(e)
                    time.sleep(1)

        t0 = time.time()
        v["inizio"] = iso_us()
        stampa(f"  VOLUME {nome_p} INIZIO {dt.datetime.now().strftime('%H:%M:%S')} ora del Mac: "
               f"{'$set' if aggiornamenti else 'insert'} su {COLL_SCRITTURE[kind]}, {a.thread_volume} thread, "
               f"{a.minuti_volume:g} minuti")
        ths = [threading.Thread(target=lavora, args=(k,), daemon=True) for k in range(a.thread_volume)]
        for t in ths:
            t.start()
        camp, prima = [], (t0, 0)
        while any(t.is_alive() for t in ths):
            time.sleep(1)
            if time.time() - prima[0] >= 30:
                with lock:
                    b, nop = tot["byte"], tot["op"]
                camp.append({"t": iso_us(), "operazioni": nop, "byte": b})
                stampa(f"    {nop:,} operazioni, ultimi 30 s {(b - prima[1]) / MB / (time.time() - prima[0]):.2f} MB/s")
                prima = (time.time(), b)
        dur = time.time() - t0
        v.update({"fine": iso_us(), "secondi": round(dur, 1), "operazioni": tot["op"], "byte": tot["byte"],
                  "mb_s": round(tot["byte"] / MB / max(dur, 1e-9), 3), "errori": tot["errori"],
                  "ultimo_errore": tot["ultimo_errore"], "campioni_30s": camp})
        if kind == "per_cliente" and not aggiornamenti:
            with lock:
                ids[(kind, "2k")] = bersagli
                ids[(kind, "340")] = []
        v["serverStatus_dopo"] = _prova(lambda: g.mon.admin.command("serverStatus"))
        v["collStats_dopo"] = _prova(lambda: g.dbc.command("collStats", COLL_SCRITTURE[kind]))
        cl.close()
        # p1-1.6.1 (errore 4 di A): un minuto quieto anche DOPO il passo, cosi' la somma dei minuti che toccano il
        # passo (misure dell'API) non prende scritture di altri passi
        p1 = iso_us()
        g.fase = f"pausa dopo volume {nome_p}"
        dorme(a.pausa_volume_s)
        v["pausa_dopo"] = {"inizio": p1, "fine": iso_us()}
        stampa(f"  VOLUME {nome_p} FINE {dt.datetime.now().strftime('%H:%M:%S')}: {tot['op']:,} operazioni, "
               f"{v['mb_s']} MB/s, errori {tot['errori']}")
        rec["volume"].append(v)
        rec["riassunto"] = riassumi_scritture(rec)
        scrivi_json(os.path.join(g.cartella, rec["file"]), rec)

    def fermo():  # un passo fermato per errori di fila (rete caduta): niente altri passi
        return STOP.is_set() or any(p.get("fermato") for p in rec["passi"])

    try:
        passo("riscaldamento", 3, singolo)
        if not fermo():
            passo("singoli", a.n_scritture, singolo)
        if not fermo():
            passo("lotti", a.n_lotti_scritture, lotto)
        if not fermo():
            passo("set", a.n_scritture, aggiorna)
        if a.minuti_volume > 0 and not fermo():
            for kv, (nome_p, kind, agg) in enumerate((("insert_id_normale", "normale", False),
                                                      ("insert_id_per_cliente", "per_cliente", False),
                                                      ("set_id_per_cliente", "per_cliente", True))):
                if STOP.is_set():
                    break
                if kind not in tipi:
                    kind = "normale"
                try:
                    volume(nome_p, kind, agg, pausa_prima=(kv == 0))
                except Exception as e:
                    rec["volume"].append({"passo": nome_p, "collezione": COLL_SCRITTURE[kind], "errore": pulito(e)})
    finally:
        if any(p.get("fermato") for p in rec["passi"]):
            rec["fermata"] = "un passo fermato per errori di fila: passi successivi saltati"
        rec["fine"] = iso_us()
        rec["dopo"] = g.istantanea("dopo le scritture")
        rec["cancellate"] = {k: _prova(lambda k=k: db.drop_collection(k) or "cancellata") for k in COLL_SCRITTURE.values()}
        rec["eventi_driver"] = [g.reg.esporta(da_c[0]), g.reg_c.esporta(da_c[1])]
        rec["serverStatus_ogni_30s"] = g.campioni[c0:]
        rec["riassunto"] = riassumi_scritture(rec)
        scrivi_json(os.path.join(g.cartella, rec["file"]), rec)
    stampa_scritture(rec)
    return rec


# ------------------------------------------------------------------ documenti da 2 KB e collezione fredda

def bancone_di(g, nome_coll):
    """Byte della collezione nel bancone (cache WiredTiger) e taglia dei dati, se Atlas li riporta."""
    cs = _prova(lambda: g.dbc.command("collStats", nome_coll))
    cache = _get(cs, "wiredTiger", "cache", "bytes currently in the cache")
    return {"t": iso_us(), "byte_nel_bancone": cache, "dati": cs.get("size") if isinstance(cs, dict) else None,
            "bancone_totale": _get(_prova(lambda: g.mon.admin.command("serverStatus")), "wiredTiger", "cache",
                                   "bytes currently in the cache")}


def carica_2kb(g, a, nome_coll, prefisso, n_clienti, gib, chiave_stato, st):
    """Carica circa gib GiB di documenti da 1,9-2 KB in nome_coll, in ordine di tempo (un anno simulato), clienti a
    caso tra n_clienti: le pagine mescolano clienti diversi, come nel carico di ieri. Riprende se interrotto."""
    out = {"collezione": nome_coll, "clienti": n_clienti, "gib_obiettivo": gib, "thread": a.grandi_thread,
           "lotto": a.grandi_lotto, "inizio": iso_us()}
    cl = connetti(pool=a.grandi_thread + 2, esci=False)
    col = cl[a.db][nome_coll]
    out["indice"] = _prova(lambda: col.create_index([("cliente", 1), ("ts", -1)], name="cliente_ts"))
    gia = _prova(lambda: cl[a.db].command("collStats", nome_coll))
    partenza = int(gia.get("size") or 0) if isinstance(gia, dict) and "errore" not in gia else 0
    obiettivo = int(gib * GB)
    out["byte_gia_presenti"] = partenza
    lo, hi = gib * 1024 / 2.9 / 60, gib * 1024 / 2.4 / 60
    stampa(f"  Stima del tempo di caricamento dal Mac: {lo:.1f}-{hi:.1f} minuti per {gib:g} GiB (STIMATO da 2,4-2,9 "
           f"MiB/s, MISURATO ieri su M30 con 4 thread e documenti da 340 byte; con documenti da 2 KB e "
           f"{a.grandi_thread} thread puo' essere piu' veloce). Gia' presenti: {partenza / MB:.0f} MB.")
    lock = threading.Lock()
    prog = {"byte": partenza, "doc": 0, "errori": 0, "bloccato": None}
    span_lotto = T_SPAN_S * a.grandi_lotto * 2000 / max(1, obiettivo)
    # per Q2 sui documenti da 2 KB: per ogni cliente l'_id di un documento scelto a caso (come i biglietti di ieri)
    vecchi = (st.d.get(chiave_stato) or {}).get("q2_id") or []
    q2 = [ObjectId(x) if x else None for x in vecchi][:n_clienti] + [None] * max(0, n_clienti - len(vecchi))
    n_cli = [1 if x else 0 for x in q2]

    def stato_q2():
        return [str(x) if x else None for x in q2]
    t0 = time.time()

    def lavora(k):
        r = random.Random(zlib.crc32(f"{nome_coll}|{k}|{partenza}".encode()))
        di_fila = 0
        while not STOP.is_set():
            with lock:
                if prog["byte"] >= obiettivo or prog["bloccato"]:
                    return
                frac = prog["byte"] / obiettivo
            base = T_INIZIO.timestamp() + min(frac, 0.9999) * T_SPAN_S
            docs, nb = [], 0
            meta = []
            for _ in range(a.grandi_lotto):
                ic = r.randrange(n_clienti)
                d = doc_2kb(r, f"{prefisso}{ic:05d}")
                d["ts"] = dt.datetime.fromtimestamp(base + r.uniform(0, span_lotto), dt.timezone.utc)
                d["_id"] = ObjectId()
                raw = bson.encode(d)
                nb += len(raw)
                docs.append(RawBSONDocument(raw))
                meta.append((ic, d["_id"]))
            try:
                col.insert_many(docs, ordered=False)
                di_fila = 0
            except BulkWriteError as e:
                errs = (e.details or {}).get("writeErrors", [])
                if not (errs and all(x.get("code") == 11000 for x in errs)):
                    di_fila += 1
            except PyMongoError as e:
                di_fila += 1
                with lock:
                    prog["errori"] += 1
                    out["ultimo_errore"] = pulito(e)
            if di_fila:
                if di_fila >= 5:
                    with lock:
                        prog["bloccato"] = prog["bloccato"] or f"5 errori di fila: {out.get('ultimo_errore')}"
                    return
                time.sleep(5 * di_fila)
                continue
            with lock:
                prog["byte"] += nb
                prog["doc"] += len(docs)
                for ic, _id in meta:
                    n_cli[ic] += 1
                    if r.random() < 1.0 / n_cli[ic]:
                        q2[ic] = _id

    ths = [threading.Thread(target=lavora, args=(k,), daemon=True) for k in range(a.grandi_thread)]
    for t in ths:
        t.start()
    camp, ultimo = [], (time.time(), partenza)
    while any(t.is_alive() for t in ths):
        time.sleep(1)
        if time.time() - ultimo[0] >= 30:
            with lock:
                b, nd = prog["byte"], prog["doc"]
            vel = (b - partenza) / MB / max(1e-9, time.time() - t0)
            eta = (obiettivo - b) / MB / vel / 60 if vel > 0 else float("inf")
            camp.append({"t": iso_us(), "byte": b, "documenti_nuovi": nd, "mib_s_medi": round(vel, 3)})
            stampa(f"    {nome_coll}: {b / GB:.3f} di {gib:g} GiB, {vel:.2f} MiB/s di media, fine tra circa {eta:.1f} "
                   f"minuti (MISURATO adesso)")
            with lock:
                ids = stato_q2()
            st.d[chiave_stato] = {"collezione": nome_coll, "clienti": n_clienti, "obiettivo_byte": obiettivo,
                                  "byte": b, "finito": False, "prefisso": prefisso, "q2_id": ids}
            st.salva()
            ultimo = (time.time(), b)
    dur = time.time() - t0
    out.update({"fine": iso_us(), "secondi": round(dur, 1), "byte_bson": prog["byte"], "documenti_nuovi": prog["doc"],
                "mib_s": round((prog["byte"] - partenza) / MB / max(dur, 1e-9), 3), "errori": prog["errori"],
                "bloccato": prog["bloccato"], "campioni_30s": camp,
                "finito": prog["byte"] >= obiettivo})
    out["documento_medio_byte"] = round((prog["byte"] - partenza) / max(1, prog["doc"]))
    st.d[chiave_stato] = {"collezione": nome_coll, "clienti": n_clienti, "obiettivo_byte": obiettivo,
                          "byte": prog["byte"], "finito": out["finito"], "prefisso": prefisso, "q2_id": stato_q2(),
                          "fine": out["fine"]}  # p1-1.6.1: per sapere se i nodi sono ripartiti dopo (cambio di taglia)
    st.salva()
    out["clienti_con_id_per_q2"] = sum(1 for x in q2 if x)
    out["collStats_dopo"] = _prova(lambda: cl[a.db].command("collStats", nome_coll))
    cl.close()
    stampa(f"  Caricati {prog['byte'] / GB:.3f} GiB in {nome_coll} in {dur / 60:.1f} minuti ({out['mib_s']} MiB/s, "
           f"MISURATO), documento medio {out['documento_medio_byte']} byte, errori {prog['errori']}"
           + (f", FERMATO: {prog['bloccato']}" if prog["bloccato"] else ""))
    return out


def _fase_semplice(g, a, indice, fase, corpo):
    """Una fase che non e' una serie di find: istantanea prima e dopo, eventi, serverStatus, file salvato."""
    rec = {"fase": fase, "tipo": fase, "versione": VERSIONE, "giro": g.nome}
    rec["file"] = file_fase(g.cartella, indice, fase)
    rec["prima"] = g.istantanea(f"prima di {fase}")
    da, c0 = (g.reg.lunghezze(), g.reg_c.lunghezze()), len(g.campioni)
    rec["inizio"] = iso_us()
    stampa(f"FASE {fase} INIZIO {dt.datetime.now().strftime('%H:%M:%S')} ora del Mac ({rec['inizio'][:19]}Z)")
    try:
        corpo(rec)
    finally:
        rec["fine"] = iso_us()
        stampa(f"FASE {fase} FINE {dt.datetime.now().strftime('%H:%M:%S')} ora del Mac ({rec['fine'][:19]}Z)")
        rec["dopo"] = g.istantanea(f"dopo {fase}")
        rec["eventi_driver"] = [g.reg.esporta(da[0]), g.reg_c.esporta(da[1])]
        rec["serverStatus_ogni_30s"] = g.campioni[c0:]
        scrivi_json(os.path.join(g.cartella, rec["file"]), rec)
    return rec


def fase_grandi_carica(g, a, st, piano, nome, indice):
    def corpo(rec):
        stato = st.d.get("grandi") or {}
        if stato.get("finito") and not a.ricarica_grandi:
            rec["nota"] = "gia' caricata in un giro precedente: non la ricarico"
            stampa(f"  {COLL_GRANDI} gia' caricata ({stato.get('byte', 0) / GB:.3f} GiB): salto il caricamento.")
            return
        rec["caricamento"] = carica_2kb(g, a, COLL_GRANDI, PREF_GRANDI, a.grandi_clienti, a.grandi_gib, "grandi", st)
    return _fase_semplice(g, a, indice, "grandi_carica", corpo)


def nodi_ripartiti_dopo(g, t_iso):
    """p1-1.6.1: il primario e' ripartito (cambio di taglia) dopo t_iso? Da serverStatus: localTime meno uptime."""
    ss = _prova(lambda: g.mon.admin.command("serverStatus"))
    up, lt = _get(ss, "uptime"), _get(ss, "localTime")
    out = {"nodo": _get(ss, "host"), "uptime_s": up, "fine_caricamento": t_iso}
    if isinstance(up, (int, float)) and isinstance(lt, dt.datetime) and t_iso:
        if lt.tzinfo is None:  # pymongo restituisce le date senza fuso (sono UTC)
            lt = lt.replace(tzinfo=dt.timezone.utc)
        avvio = lt - dt.timedelta(seconds=up)
        out["avvio_del_nodo"] = iso_us(avvio)
        out["ripartito_dopo"] = avvio > _dt_us_fine(t_iso)
    return out


def lavaggio(g, a, nome_coll, fine_caricamento, rec):
    """Fa uscire dal bancone una collezione appena scritta leggendo per intero, lato server, collezioni gia'
    misurate (messaggi, poi eventi). p1-1.6.1 (E9 di B, correzione 13 di A): prima guarda se serve. Se il bancone
    della collezione e' gia' sotto la soglia, niente scansioni; se Atlas non riporta il campo ma il primario e'
    ripartito dopo il caricamento (caricata su M10 prima del cambio a M30), e' fredda per costruzione."""
    rec["bancone_prima"] = bancone_di(g, nome_coll)
    rec["riavvio"] = nodi_ripartiti_dopo(g, fine_caricamento) if fine_caricamento else None
    rec["scansioni"] = []
    b0 = rec["bancone_prima"]
    stampa(f"  {nome_coll} nel bancone prima: {b0['byte_nel_bancone']} byte (dati {b0['dati']})")
    if b0["byte_nel_bancone"] is not None and b0["dati"] and b0["byte_nel_bancone"] < a.lavaggio_soglia * b0["dati"]:
        rec["esito"] = "gia' fredda: meno della soglia nel bancone, nessuna scansione"
    elif b0["byte_nel_bancone"] is None and (rec["riavvio"] or {}).get("ripartito_dopo"):
        rec["esito"] = ("fredda per costruzione: il primario e' ripartito dopo il caricamento (cambio di taglia), "
                        "nessuna scansione (Atlas non riporta i byte della collezione nel bancone)")
    if rec.get("esito"):
        stampa(f"  esito del lavaggio: {rec['esito']}")
        return rec
    for c in [x for x in a.lavaggio_collezioni.split(",") if x]:
        if STOP.is_set():
            break
        t0s, t0 = iso_us(), time.perf_counter()
        r = _prova(lambda: list(g.dbc[c].aggregate([{"$match": {"_campo_assente_": 1}}, {"$count": "n"}],
                                                   maxTimeMS=int(a.lavaggio_max_s * 1000),
                                                   comment=f"p1|lavaggio|{c}")))
        b = bancone_di(g, nome_coll)
        rec["scansioni"].append({"collezione": c, "inizio": t0s, "fine": iso_us(),
                                 "secondi": round(time.perf_counter() - t0, 2), "risposta": r, "bancone_dopo": b})
        stampa(f"  scansione di {c}: {time.perf_counter() - t0:.0f} s; {nome_coll} nel bancone adesso: "
               f"{b['byte_nel_bancone']} byte")
        if b["byte_nel_bancone"] is not None and b["dati"] and b["byte_nel_bancone"] < a.lavaggio_soglia * b["dati"]:
            break
    fine = rec["scansioni"][-1]["bancone_dopo"] if rec["scansioni"] else rec["bancone_prima"]
    if fine["byte_nel_bancone"] is None:
        rec["esito"] = "NON SO: Atlas non riporta i byte della collezione nel bancone"
    elif fine["dati"] and fine["byte_nel_bancone"] < a.lavaggio_soglia * fine["dati"]:
        rec["esito"] = "lavato: meno della soglia nel bancone"
    else:
        rec["esito"] = "ATTENZIONE: la collezione e' ancora in buona parte nel bancone (letture non del tutto fredde)"
    stampa(f"  esito del lavaggio: {rec['esito']}")
    return rec


def fase_grandi_lavaggio(g, a, st, piano, nome, indice):
    """Il bancone e' pieno dei documenti da 2 KB appena scritti (se caricati su M30): per leggerli a freddo li faccio
    uscire leggendo per intero, lato server, collezioni gia' misurate. Nessun documento torna al Mac. Se caricati
    su M10 prima del cambio (GIRO-FINALE, passo D), niente scansioni."""
    def corpo(rec):
        lavaggio(g, a, COLL_GRANDI, (st.d.get("grandi") or {}).get("fine"), rec)
    return _fase_semplice(g, a, indice, "grandi_lavaggio", corpo)


def fase_freddo(g, a, st, piano, nome, indice):
    """Extra, non decisiva: la collezione fredda di Atlas (storageTier cold), con il codice del comando 'fredda' del
    revisore dello script: se rifiutata salva la risposta; se accettata copia circa 1 GiB lato server, aspetta 10
    minuti, letture fredde con il profiler, un tentativo di modifica (errore atteso), cancellazione."""
    def corpo(rec):
        esegui_fredda(g, a, rec, indice, st=st)
        acc = _get(rec, "create", "accettata")
        rec["accettata"] = acc
        rec["risposta_create"] = rec.get("create")
    return _fase_semplice(g, a, indice, "freddo", corpo)


# ------------------------------------------------------------------ CPU per operazione, misure dell'API, indice

def cpu_per_operazione(ss0, ss1):
    """Dalla differenza di due serverStatus dello stesso nodo: CPU del processo (extra_info, Linux), operazioni,
    documenti, pagine e byte letti nel bancone. None = non riportato o nodo diverso."""
    if not isinstance(ss0, dict) or not isinstance(ss1, dict) or "errore" in ss0 or "errore" in ss1:
        return None
    if ss0.get("host") != ss1.get("host"):
        return {"nota": "serverStatus di due nodi diversi: niente differenza", "prima": ss0.get("host"),
                "dopo": ss1.get("host")}

    def d(*ch):
        a, b = _get(ss0, *ch), _get(ss1, *ch)
        return (b - a) if isinstance(a, (int, float)) and isinstance(b, (int, float)) else None
    t0, t1 = ss0.get("localTime"), ss1.get("localTime")
    sec = (t1 - t0).total_seconds() if isinstance(t0, dt.datetime) and isinstance(t1, dt.datetime) else None
    cpu = [d("extra_info", "user_time_us"), d("extra_info", "system_time_us")]
    cpu_us = sum(x for x in cpu if x is not None) if any(x is not None for x in cpu) else None
    op = {k: d("opcounters", k) for k in ("query", "getmore", "insert", "update", "delete", "command")}
    doc = {k: d("metrics", "document", k) for k in ("returned", "inserted", "updated", "deleted")}
    letture = sum(op[k] or 0 for k in ("query", "getmore"))
    scritture = sum(op[k] or 0 for k in ("insert", "update", "delete"))
    out = {"nodo": ss1.get("host"), "secondi": sec, "cpu_us": cpu_us, "operazioni": op, "documenti": doc,
           "pagine_lette_nel_bancone": d("wiredTiger", "cache", "pages read into cache"),
           "byte_letti_nel_bancone": d("wiredTiger", "cache", "bytes read into cache"),
           "byte_scritti_dal_bancone": d("wiredTiger", "cache", "bytes written from cache"),
           "bancone_byte_dopo": _get(ss1, "wiredTiger", "cache", "bytes currently in the cache")}
    if cpu_us is not None:
        out["cpu_us_per_lettura"] = round(cpu_us / letture, 2) if letture else None
        out["cpu_us_per_scrittura"] = round(cpu_us / scritture, 2) if scritture else None
        nd = sum(v or 0 for v in doc.values())
        out["cpu_us_per_documento"] = round(cpu_us / nd, 2) if nd else None
        out["cpu_percento_di_un_core"] = round(100 * cpu_us / 1e6 / sec, 1) if sec else None
    if out["pagine_lette_nel_bancone"] is not None and letture:
        out["pagine_lette_per_lettura"] = round(out["pagine_lette_nel_bancone"] / letture, 3)
    out["nota"] = ("STIMATO: la differenza comprende anche i comandi di controllo dello script (istantanee, "
                   "profiler); CPU del solo nodo a cui e' collegato il client di controllo (il primario)")
    return out


NOMI_API_CHIAVE = ("DISK_PARTITION_IOPS_READ", "DISK_PARTITION_IOPS_WRITE", "DISK_PARTITION_THROUGHPUT_READ",
                   "DISK_PARTITION_THROUGHPUT_WRITE", "DISK_PARTITION_LATENCY_READ", "DISK_PARTITION_LATENCY_WRITE",
                   "PROCESS_CPU_USER", "PROCESS_CPU_KERNEL", "PROCESS_NORMALIZED_CPU_USER",
                   "PROCESS_NORMALIZED_CPU_KERNEL", "SYSTEM_NORMALIZED_CPU_USER", "OPCOUNTER_QUERY",
                   "OPCOUNTER_GETMORE", "OPCOUNTER_INSERT", "OPCOUNTER_UPDATE", "OPCOUNTER_DELETE",
                   "CACHE_BYTES_READ_INTO", "CACHE_BYTES_WRITTEN_FROM", "CACHE_USED_BYTES", "CACHE_DIRTY_BYTES",
                   "OP_EXECUTION_TIME_READS", "OP_EXECUTION_TIME_WRITES", "DB_DATA_SIZE_TOTAL", "DB_STORAGE_TOTAL",
                   "OPLOG_MASTER_TIME", "OPLOG_RATE_GB_PER_HOUR")


def serie_api(cartelle):
    """Tutte le misure scaricate dall'Atlas Admin API: {origine: {nome: {ts: valore}}}. origine = processo
    (host:porta), con /disks/X o /databases/Y se la misura e' di un disco o di un database."""
    serie = defaultdict(lambda: defaultdict(dict))
    for c in cartelle:
        for radice, _, files in os.walk(c):
            for nf in files:
                if not nf.endswith(".json") or "measurements" not in nf:
                    continue
                try:
                    r = leggi_json(os.path.join(radice, nf))
                except Exception:
                    continue
                url, risp = r.get("url", ""), r.get("risposta")
                if r.get("stato_http") != 200 or not isinstance(risp, dict):
                    continue
                m = re.search(r"/processes/([^/?]+)(/(disks|databases)/([^/?]+))?/measurements", url)
                if not m:
                    continue
                origine = urllib.parse.unquote(m.group(1)) + (f"/{m.group(3)}/{m.group(4)}" if m.group(2) else "")
                for mis in risp.get("measurements", []) or []:
                    for p in mis.get("dataPoints", []) or []:
                        if p.get("value") is not None and p.get("timestamp"):
                            serie[origine][mis.get("name")][str(p["timestamp"])[:19] + "Z"] = p["value"]
    return serie


def media_finestra(punti, inizio, fine, passo_s=60):
    """Media dei punti al minuto che si sovrappongono alla finestra [inizio, fine] (ogni punto porta l'ora d'inizio
    del suo minuto). Nei minuti di confine c'e' anche un pezzo della fase prima o dopo: per questo le fasi durano
    almeno 150 secondi e hanno 2 minuti di pausa prima."""
    a, b = _dt_us(inizio), _dt_us(fine)
    v = []
    for t, x in punti.items():
        try:
            tt = _dt_us(t)
        except ValueError:
            continue
        if tt <= b and tt + dt.timedelta(seconds=passo_s) > a:
            v.append(x)
    return (round(statistics.fmean(v), 4), len(v)) if v else (None, 0)


def somma_finestra(punti, inizio, fine, passo_s=60):
    """p1-1.6.1 (errore 4 di A): la SOMMA sui minuti che toccano la finestra di valore x 60 s (per misure 'al
    secondo', come i byte scritti o le letture del disco al secondo): e' il totale della fase, senza la diluizione
    della media (una fase da 180 s tocca 4 minuti). Giusta se i minuti di confine sono quieti (pause prima e dopo).
    Restituisce (somma, punti)."""
    a, b = _dt_us(inizio), _dt_us(fine)
    v = []
    for t, x in punti.items():
        try:
            tt = _dt_us(t)
        except ValueError:
            continue
        if tt <= b and tt + dt.timedelta(seconds=passo_s) > a:
            v.append(x)
    return (round(sum(v) * passo_s, 1), len(v)) if v else (None, 0)


def analisi_giro(cartella):
    """Il riassunto della lista di controllo (voce 24), calcolato solo dai dati grezzi della cartella: per ogni fase
    di lettura pagine lette dallo storage per find e microsecondi per pagina (profiler, con i byte per lettura delle
    misure dell'API); per le scritture byte scritti nello storage per byte inserito; CPU per operazione
    (serverStatus); le misure chiave dell'API del primario mediate sui minuti della fase. Tutto STIMATO."""
    def fasi_una_per_volta():  # niente lista in memoria: sul giro vero i file possono essere grandi
        for nf in sorted(os.listdir(cartella)):
            if nf.startswith("fase_") and nf.endswith(".json"):
                try:
                    yield leggi_json(os.path.join(cartella, nf))
                except Exception:
                    continue
    fasi = fasi_una_per_volta()
    atl = [os.path.join(cartella, x) for x in os.listdir(cartella) if x.startswith("atlas_") and
           os.path.isdir(os.path.join(cartella, x))]
    serie = serie_api(atl)
    out = {"nota": "STIMATO: numeri derivati dai dati grezzi; le misure dell'API sono medie al minuto",
           "origini_api": sorted(serie), "fasi": []}

    def primario(rec):
        if rec.get("nodo_misurato"):  # p1-1.6.1: le letture dal secondario si confrontano con le misure del secondario
            return rec["nodo_misurato"]
        for k in ("prima", "dopo"):
            h = _get(rec, k, "hello")
            if isinstance(h, dict) and h.get("primary"):
                return h["primary"]
        return None

    def api(rec, inizio, fine):
        p = primario(rec)
        r = {"primario": p}
        if not p:
            return r
        for orig, nomi in serie.items():
            if orig.split("/")[0] != p:
                continue
            for nome in NOMI_API_CHIAVE:
                if nome in nomi:
                    v, n = media_finestra(nomi[nome], inizio, fine)
                    if v is not None:
                        r[f"{orig[len(p):] or ''}|{nome}".lstrip("|")] = {
                            "media": v, "punti": n, "somma_x60": somma_finestra(nomi[nome], inizio, fine)[0]}
        return r

    def prendi(dic, nome, chiave="media"):
        for k, v in dic.items():
            if k.endswith(nome) and isinstance(v, dict):
                return v.get(chiave)
        return None

    for rec in fasi:
        f = rec.get("fase")
        ini, fin = rec.get("inizio"), rec.get("fine")
        r = {"fase": f, "inizio": ini, "fine": fin}
        r["cpu_serverStatus"] = cpu_per_operazione(_get(rec, "prima", "serverStatus"), _get(rec, "dopo", "serverStatus"))
        if ini and fin:
            r["api_primario"] = api(rec, ini, fin)
        misure = rec.get("misure") if isinstance(rec.get("misure"), list) else []
        if misure and rec.get("tipo") in (None, "spiega") and f != "spiega":
            prof = indice_profilo(rec.get("profiler", []))
            zero = riporta_storage(prof)
            fr = [m for m in misure if m.get("freddo")]
            nums = [numeri_profilo(prof.get(m["freddo"]["commento"]), zero) for m in fr]
            nums = [x for x in nums if x and x.get("bytesRead") is not None]  # p1-1.6.1: le find a 0 byte restano
            sec = (_dt_us(fin) - _dt_us(ini)).total_seconds() if ini and fin else None
            n_find = len(fr) * (1 + rec.get("rip_caldo", 2))
            iops = prendi(r.get("api_primario", {}), "DISK_PARTITION_IOPS_READ")
            thr = prendi(r.get("api_primario", {}), "DISK_PARTITION_THROUGHPUT_READ")
            io_fase = prendi(r.get("api_primario", {}), "DISK_PARTITION_IOPS_READ", "somma_x60")
            byte_io = thr / iops if iops and thr else None
            br = [x["bytesRead"] for x in nums]
            tr = [x["timeReadingMicros"] for x in nums if x.get("timeReadingMicros") is not None]
            r["letture"] = {
                "find_fredde": len(fr), "find_al_secondo": round(n_find / sec, 3) if sec else None,
                "profiler_byte_letti_storage_per_find_fredda": stat(br),
                "profiler_us_di_lettura_storage_per_find_fredda": stat(tr),
                **conta_zero(nums),
                "api_byte_per_lettura_storage": round(byte_io, 1) if byte_io else None,
                "pagine_per_find_fredda_profiler": round(statistics.fmean(br) / byte_io, 2) if br and byte_io else None,
                "us_per_pagina": round(sum(tr) / (sum(br) / byte_io), 1) if br and tr and byte_io and sum(br) else None,
                # p1-1.6.1 (errore 5 di A): letture del disco della fase (somma dei minuti) divise per le sole find
                # fredde (le calde non leggono pagine). Comprende anche le letture del disco di istantanee e count di
                # controllo, e i minuti di confine: e' un controllo d'ordine di grandezza, non il numero buono.
                "api_letture_disco_nella_fase": io_fase,
                "pagine_per_find_api_controllo": round(io_fase / len(fr), 2) if io_fase and fr else None,
                "nota_controllo_api": "letture del disco della fase (somma al minuto) / find fredde; comprende "
                                      "istantanee, count di controllo e minuti di confine"}
        if rec.get("tipo") == "scritture":
            vol = []
            for v in rec.get("volume", []):
                if v.get("inizio") and v.get("fine") and v.get("byte"):
                    a2 = api(rec, v["inizio"], v["fine"])
                    w = prendi(a2, "DISK_PARTITION_THROUGHPUT_WRITE")
                    # p1-1.6.1 (errore 4 di A): somma dei minuti che toccano il passo (pause quiete prima e dopo),
                    # non media x secondi (che toglieva circa un quarto)
                    tot_w = prendi(a2, "DISK_PARTITION_THROUGHPUT_WRITE", "somma_x60")
                    vol.append({"passo": v.get("passo"), "byte_inseriti": v["byte"], "secondi": v.get("secondi"),
                                "api_byte_scritti_al_secondo_media": w, "api_byte_scritti_nei_minuti_del_passo": tot_w,
                                "byte_scritti_per_byte_inserito": round(tot_w / v["byte"], 2) if tot_w else None,
                                "nota": ("per i $set i 'byte inseriti' sono circa 40 per operazione (i campi nuovi): "
                                         "conta di piu' byte_scritti_per_operazione") if "set" in str(v.get("passo")) else
                                "solo i passi di volume (profiler spento); i passi di latenza non si usano per questo numero",
                                "byte_scritti_per_operazione": round(tot_w / v["operazioni"], 1)
                                if tot_w and v.get("operazioni") else None,
                                "cpu_serverStatus": cpu_per_operazione(v.get("serverStatus_prima"),
                                                                       v.get("serverStatus_dopo"))})
            r["volume"] = vol
            r["passi_cpu"] = [{"passo": p.get("passo"), "cpu": cpu_per_operazione(p.get("serverStatus_prima"),
                                                                                   p.get("serverStatus_dopo"))}
                              for p in rec.get("passi", [])]
        if rec.get("tipo") in ("parallelo", "carico_massimo"):
            r["livelli"] = []
            for lv in rec.get("livelli", rec.get("parti", [])):
                a2 = api(rec, lv["inizio"], lv["fine"]) if lv.get("inizio") and lv.get("fine") else {}
                x = {"livello": lv.get("livello") or lv.get("parte"), "api_primario": a2,
                     "cpu": cpu_per_operazione(lv.get("serverStatus_prima"), lv.get("serverStatus_dopo"))}
                if lv.get("profiler"):  # parallelo: pagine per find fredda e microsecondi per pagina, per ricerca
                    prof = indice_profilo(lv["profiler"])
                    iops = prendi(a2, "DISK_PARTITION_IOPS_READ")
                    thr = prendi(a2, "DISK_PARTITION_THROUGHPUT_READ")
                    bio = thr / iops if iops and thr else None
                    x["per_query"] = {}
                    zero = riporta_storage(prof)
                    for q in sorted({m.get("query") for m in lv.get("misure", []) if m.get("query")}):
                        nn = [numeri_profilo(prof.get(m["commento"]), zero) for m in lv["misure"]
                              if m.get("query") == q and m.get("tipo") == "freddo"]
                        nn = [y for y in nn if y and y.get("bytesRead") is not None]
                        br = [y["bytesRead"] for y in nn]
                        tr = [y["timeReadingMicros"] for y in nn if y.get("timeReadingMicros") is not None]
                        x["per_query"][q] = {
                            **conta_zero(nn),
                            "profiler_byte_letti_storage_per_find_fredda": stat(br),
                            "profiler_us_di_lettura_storage_per_find_fredda": stat(tr),
                            "pagine_per_find_fredda": round(statistics.fmean(br) / bio, 2) if br and bio else None,
                            "us_per_pagina": round(sum(tr) / (sum(br) / bio), 1) if br and tr and bio and sum(br) else None}
                r["livelli"].append(x)
        out["fasi"].append(r)
    return out


def _dt_us(s):
    return dt.datetime.strptime(str(s)[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=dt.timezone.utc)


def scrivi_indice(cartella):
    """INDICE.json: ogni file salvato, con dimensione e SHA-256 (voce 24 della lista di controllo)."""
    righe = []
    for radice, _, files in os.walk(cartella):
        for nf in sorted(files):
            pth = os.path.join(radice, nf)
            if os.path.relpath(pth, cartella) == "INDICE.json":
                continue
            h = hashlib.sha256()
            with open(pth, "rb") as f:
                for blocco in iter(lambda: f.read(1 << 20), b""):
                    h.update(blocco)
            righe.append({"file": os.path.relpath(pth, cartella), "byte": os.path.getsize(pth), "sha256": h.hexdigest()})
    righe.sort(key=lambda x: x["file"])
    scrivi_json(os.path.join(cartella, "INDICE.json"), {"scritto": iso_us(), "versione": VERSIONE,
                                                         "file": righe, "totale_byte": sum(x["byte"] for x in righe)})
    return len(righe)


def stampa_analisi(an):
    print("=== NUMERI DERIVATI (STIMATI, solo dai dati grezzi; dettaglio in RIASSUNTO.json) ===")
    for r in an.get("fasi", []):
        riga = [f"{r['fase']}:"]
        le = r.get("letture")
        if le:
            riga.append(f"find fredde a 0 byte {le.get('find_a_0_byte')}/{le.get('su')}; "
                        f"find/s {le['find_al_secondo']}; byte letti dallo storage per find fredda p50 "
                        f"{_r(le['profiler_byte_letti_storage_per_find_fredda'])}; pagine per find fredda "
                        f"{le['pagine_per_find_fredda_profiler']} (API: {le['pagine_per_find_api_controllo']}); "
                        f"us per pagina {le['us_per_pagina']}")
        for v in r.get("volume", []):
            riga.append(f"[{v['passo']}: byte scritti per byte inserito {v['byte_scritti_per_byte_inserito']}, "
                        f"per operazione {v.get('byte_scritti_per_operazione')}]")
        c = r.get("cpu_serverStatus") or {}
        if c.get("cpu_us") is not None:
            riga.append(f"CPU {c.get('cpu_percento_di_un_core')}% di un core, us per lettura {c.get('cpu_us_per_lettura')}, "
                        f"per scrittura {c.get('cpu_us_per_scrittura')}")
        print("  " + " ".join(riga))
    print()


# ------------------------------------------------------------------ carico massimo e cancellazione di un cliente

def fase_carico_massimo(g, a, st, piano, nome, indice):
    """Voce 27: il carico piu' alto che il Mac riesce a dare, per la CPU per operazione di M30. Letture (un ordine per
    _id e gli ultimi 20 messaggi, a turno, su clienti GIA' letti: non tocca i mai letti), poi insert a lotti da circa
    340 byte con w=1 (C2) e con w="majority" (C3) in una collezione a parte (cancellata). Il limite e' quasi certamente il Mac (rete e Python): lo dice il file."""
    rec = {"fase": "carico_massimo", "tipo": "carico_massimo", "versione": VERSIONE, "giro": g.nome, "parti": [],
           "thread": a.thread_massimo, "file": file_fase(g.cartella, indice, "carico_massimo")}
    rec["prima"] = g.istantanea("prima del carico massimo")
    c0 = len(g.campioni)
    rec["inizio"] = iso_us()
    cl_st = st.d["clienti"]
    ris = set(cl_st["riservati"])
    gia = [i for i in sorted(set(st.d.get("usati", [])) | caldi_dei_giri_vecchi(st)) if cl_st["q2_id"][i]]
    gia_attivi = [i for i in gia if i not in ris] or gia
    # p1-1.6.1 (C2, C3; errore 1 di A, E5 di B): le scritture sono insert a lotti (a.lotto_scritture documenti da
    # circa 340 byte, come il carico P1) con w=1 e poi con w="majority", ognuna con la sua pausa quieta prima
    try:
        for parte in ("letture", "lotti_w1", "lotti_majority"):
            if STOP.is_set() or (parte == "letture" and not gia):
                continue
            if parte != "letture":
                rec.setdefault("pulizia", {})[parte] = _prova(
                    lambda: g.db.drop_collection(COLL_SCRITTURE["normale"]) or "fatto")
                p0 = iso_us()
                dorme(a.pausa_volume_s)
                rec.setdefault("pause", {})[parte] = {"inizio": p0, "fine": iso_us()}
                if STOP.is_set():
                    break
            cl = connetti(pool=a.thread_massimo + 2, esci=False)
            db = cl[a.db]
            wc_parte = WriteConcern(w="majority") if parte == "lotti_majority" else WriteConcern(w=1)
            fine = time.time() + a.minuti_massimo * 60
            lock = threading.Lock()
            tot = {"op": 0, "errori": 0, "doc": 0, "ultimo_errore": None, "ms": []}
            pt = {"parte": parte, "w": None if parte == "letture" else ("1" if parte == "lotti_w1" else "majority"),
                  "documenti_per_operazione": 1 if parte == "letture" else a.lotto_scritture,
                  "serverStatus_prima": _prova(lambda: g.mon.admin.command("serverStatus")),
                  "mac_cpu_prima_s": time.process_time(), "mac_carico_prima": _prova(lambda: list(os.getloadavg()))}

            def lavora(k):
                r = random.Random(a.seme * 31 + k)
                ordini, messaggi = db["ordini"], db.get_collection("messaggi", codec_options=RAWOPT)
                scr = db[COLL_SCRITTURE["normale"]].with_options(write_concern=wc_parte)
                n = 0
                while not STOP.is_set() and time.time() < fine:
                    t0 = time.perf_counter_ns()
                    try:
                        if parte == "letture":
                            i = r.choice(gia_attivi)
                            if n % 2 == 0:
                                nd = 1 if ordini.find_one({"_id": ObjectId(cl_st["q2_id"][i])}) else 0
                            else:
                                nd = len(list(messaggi.find({"cliente": nome_cliente(i)}, sort=[("ts", -1)],
                                                            limit=20, batch_size=20)))
                        else:
                            docs = [doc_messaggio(r, f"u{r.randrange(a.clienti_scrivi):07d}", ora())
                                    for _ in range(a.lotto_scritture)]
                            scr.insert_many(docs, ordered=False)
                            nd = len(docs)
                        ms = ms_da(t0)
                        with lock:
                            tot["op"] += 1
                            tot["doc"] += nd
                            if len(tot["ms"]) < 200000:
                                tot["ms"].append(round(ms, 3))
                    except Exception as e:
                        with lock:
                            tot["errori"] += 1
                            tot["ultimo_errore"] = pulito(e)
                        time.sleep(1)
                    n += 1

            pt["inizio"] = iso_us()
            stampa(f"  CARICO MASSIMO {parte} INIZIO {dt.datetime.now().strftime('%H:%M:%S')} ora del Mac: "
                   f"{a.thread_massimo} thread per {a.minuti_massimo:g} minuti")
            ths = [threading.Thread(target=lavora, args=(k,), daemon=True) for k in range(a.thread_massimo)]
            t0 = time.time()
            for t in ths:
                t.start()
            camp, prima = [], (t0, 0)
            while any(t.is_alive() for t in ths):
                time.sleep(1)
                if time.time() - prima[0] >= 30:
                    with lock:
                        nop = tot["op"]
                    camp.append({"t": iso_us(), "operazioni": nop, "mac_carico": _prova(lambda: list(os.getloadavg()))})
                    stampa(f"    {nop:,} operazioni, ultimi 30 s {(nop - prima[1]) / (time.time() - prima[0]):.0f} al secondo")
                    prima = (time.time(), nop)
            dur = time.time() - t0
            pt.update({"fine": iso_us(), "secondi": round(dur, 1), "operazioni": tot["op"], "documenti": tot["doc"],
                       "operazioni_al_secondo": round(tot["op"] / max(dur, 1e-9), 1), "errori": tot["errori"],
                       "ultimo_errore": tot["ultimo_errore"], "ms_dal_mac": stat(tot["ms"]), "campioni_30s": camp,
                       "mac_cpu_s": round(time.process_time() - pt["mac_cpu_prima_s"], 2),
                       "mac_carico_dopo": _prova(lambda: list(os.getloadavg())), "mac_core": os.cpu_count()})
            pt["mac_cpu_percento_di_un_core"] = round(100 * pt["mac_cpu_s"] / max(dur, 1e-9), 1)
            pt["limite"] = ("probabilmente il Mac: Python usa circa un core intero" if pt["mac_cpu_percento_di_un_core"] > 80
                            else "rete del Mac (37 ms per viaggio) o Atlas: NON SO quale")
            pt["serverStatus_dopo"] = _prova(lambda: g.mon.admin.command("serverStatus"))
            pt["cpu_atlas"] = cpu_per_operazione(pt["serverStatus_prima"], pt["serverStatus_dopo"])
            cl.close()
            stampa(f"  CARICO MASSIMO {parte} FINE {dt.datetime.now().strftime('%H:%M:%S')}: "
                   f"{pt['operazioni_al_secondo']} operazioni al secondo; CPU del Mac {pt['mac_cpu_percento_di_un_core']}% "
                   f"di un core; CPU di Atlas per operazione {_get(pt, 'cpu_atlas', 'cpu_us_per_lettura') if parte == 'letture' else _get(pt, 'cpu_atlas', 'cpu_us_per_scrittura')} us, "
                   f"per documento {_get(pt, 'cpu_atlas', 'cpu_us_per_documento')} us")
            rec["parti"].append(pt)
            scrivi_json(os.path.join(g.cartella, rec["file"]), rec)
    finally:
        rec["cancellata"] = _prova(lambda: g.db.drop_collection(COLL_SCRITTURE["normale"]) or "cancellata")
        rec["fine"] = iso_us()
        rec["dopo"] = g.istantanea("dopo il carico massimo")
        rec["serverStatus_ogni_30s"] = g.campioni[c0:]
        scrivi_json(os.path.join(g.cartella, rec["file"]), rec)
    return rec


def fase_cancellazione(g, a, st, piano, nome, indice):
    """Voce 29: cancellazione di clienti interi (attivi mai letti): deleteMany del cliente in tutte le collezioni,
    tempo di ogni operazione, documenti cancellati, serverStatus, dbStats (Data Size) e oplog prima e dopo."""
    def corpo(rec):
        comp = piano.get("cancellazione", [])
        rec["clienti"] = [c["cliente"] for c in comp]
        rec["dbStats_prima"] = _prova(lambda: g.dbc.command("dbStats"))
        rec["oplog_prima"] = _prova(lambda: g.mon["local"].command("collStats", "oplog.rs"))
        rec["serverStatus_prima"] = _prova(lambda: g.mon.admin.command("serverStatus"))
        ops = []
        for k, c in enumerate(comp):
            if STOP.is_set():
                break
            if len(ops) >= a.tetto_errori_di_fila and all("errore" in o for o in ops[-a.tetto_errori_di_fila:]):
                rec["fermata"] = f"{a.tetto_errori_di_fila} operazioni di fila in errore: fase fermata"
                stampa(f"  ATTENZIONE: {rec['fermata']}")
                break
            # p1-1.6.1 (S22; errore 1 di A, E4 di B): un cliente con w=1, il successivo con w="majority"
            w = "w1" if k % 2 == 0 else "majority"
            if g.prenota:
                g.prenota([c])
            for coll in ("messaggi", "eventi", "ordini"):
                op = {"k": k, "cliente": c["cliente"], "i": c["i"], "collezione": coll, "w": w,
                      "commento": f"p1|cancellazione|{w}|{c['cliente']}|{coll}"}
                try:
                    op["hello_prima"] = g.hello_accoppiato()
                    t0s, t0 = time.time_ns(), time.perf_counter_ns()
                    r = g.db[coll].with_options(write_concern=WriteConcern(w=1 if w == "w1" else "majority")) \
                        .delete_many({"cliente": c["cliente"]}, comment=op["commento"])
                    op.update({"ms": round(ms_da(t0), 3), "inizio": iso_ns(t0s), "fine": iso_us(),
                               "cancellati": r.deleted_count})
                except Exception as e:
                    op.update({"errore": pulito(e), "t": iso_us()})
                ops.append(op)
            if g.segna:
                g.segna(c)
            stampa(f"  cliente {c['cliente']} cancellato: " + ", ".join(
                f"{o['collezione']} {o.get('cancellati', 'errore')} in {o.get('ms', '-')} ms" for o in ops[-3:]))
        rec["operazioni"] = ops
        rec["serverStatus_dopo"] = _prova(lambda: g.mon.admin.command("serverStatus"))
        rec["dbStats_dopo"] = _prova(lambda: g.dbc.command("dbStats"))
        rec["oplog_dopo"] = _prova(lambda: g.mon["local"].command("collStats", "oplog.rs"))
        rec["cpu"] = cpu_per_operazione(rec["serverStatus_prima"], rec["serverStatus_dopo"])
        ok = [o for o in ops if "ms" in o]
        rec["riassunto"] = {"operazioni": len(ops), "errori": len(ops) - len(ok), "ms": stat([o["ms"] for o in ok]),
                            "documenti_cancellati": sum(o["cancellati"] for o in ok),
                            "dataSize_prima": _get(rec, "dbStats_prima", "dataSize"),
                            "dataSize_dopo": _get(rec, "dbStats_dopo", "dataSize"),
                            "oplog_size_prima": _get(rec, "oplog_prima", "size"),
                            "oplog_size_dopo": _get(rec, "oplog_dopo", "size"),
                            "per_w": {w: {"operazioni": len([o for o in ops if o.get("w") == w]),
                                          "clienti": len({o["i"] for o in ops if o.get("w") == w}),
                                          "ms": stat([o["ms"] for o in ok if o.get("w") == w]),
                                          "documenti_cancellati": sum(o["cancellati"] for o in ok if o.get("w") == w)}
                                      for w in ("w1", "majority")}}
        pw = rec["riassunto"]["per_w"]
        stampa(f"  Cancellati {rec['riassunto']['documenti_cancellati']} documenti di {len(comp)} clienti; "
               f"deleteMany p50 {_r(rec['riassunto']['ms'])} ms (dal Mac; w=1 {_r(pw['w1']['ms'])}, majority "
               f"{_r(pw['majority']['ms'])}); dataSize da "
               f"{rec['riassunto']['dataSize_prima']} a {rec['riassunto']['dataSize_dopo']}")
    return _fase_semplice(g, a, indice, "cancellazione", corpo)


def fase_inizio(g, a, st, piano, nome, indice):
    def corpo(rec):
        # voce 3: il bancone (cache WiredTiger) del primario deve essere quasi vuoto dopo il cambio a M30
        ss = _prova(lambda: g.mon.admin.command("serverStatus"))
        he = _prova(lambda: g.mon.admin.command("hello"))
        cache = _get(ss, "wiredTiger", "cache", "bytes currently in the cache")
        rec["bancone_primario"] = {"t": iso_us(), "nodo": _get(ss, "host"), "primario": _get(he, "primary"),
                                   "byte_nel_bancone": cache,
                                   "tetto_del_bancone": _get(ss, "wiredTiger", "cache", "maximum bytes configured"),
                                   "sotto_0_2_GiB": (cache < 0.2 * GB) if isinstance(cache, (int, float)) else None}
        if isinstance(cache, (int, float)):
            stampa(f"  Bancone (cache) del primario adesso: {cache / GB:.3f} GiB "
                   + ("(sotto 0,2 GiB: bene)" if cache < 0.2 * GB else
                      "ATTENZIONE: sopra 0,2 GiB. Il cambio a M30 e' finito? Se si', vado avanti lo stesso: e' salvato."))
        else:
            stampa("  Bancone del primario: non riportato da serverStatus (lo guardi sul grafico, passo 3).")
        rec["ambiente"] = g.ambiente()
        fine_car = st.d.get("caricamento", {}).get("fine")
        if fine_car:
            ore = (ora() - _dt(fine_car)).total_seconds() / 3600
            rec["ore_dalla_fine_del_caricamento"] = round(ore, 2)
            stampa(f"  Punto a 24 ore: il caricamento e' finito {ore:.1f} ore fa ({fine_car}); i dormienti hanno smesso "
                   f"di scrivere circa 25 minuti prima (RISULTATI-CORRETTI).")
        rec["composizione"] = _prova(lambda: composizione(g.mon, a, st))
    return _fase_semplice(g, a, indice, "inizio", corpo)


def fase_fine(g, a, st, piano, nome, indice):
    def corpo(rec):
        rec["composizione"] = _prova(lambda: composizione(g.mon, a, st))
        # voce 23: tutte le collezioni create dallo script si cancellano, con la prova (listCollections dopo)
        nomi = _prova(lambda: g.dbc.list_collection_names())
        create = list(COLL_SCRITTURE.values()) + [COLL_SCRITTURE_SFONDO, NOME_FREDDA] + \
            ([] if a.tieni_grandi else [COLL_GRANDI])
        rest = [c for c in create if isinstance(nomi, list) and c in nomi]
        rec["collezioni_dello_script_trovate"] = rest
        rec["cancellate"] = {c: _prova(lambda c=c: g.db.drop_collection(c) or "cancellata") for c in rest}
        if not g.profilo_acceso:  # system.profile (ricreata dallo script come capped) si cancella a profiler spento
            rec["system_profile"] = _prova(lambda: g.db.drop_collection("system.profile") or "cancellata")
        rec["collezioni_dopo"] = _prova(lambda: sorted(g.dbc.list_collection_names()))
        restano = [c for c in create + ["system.profile"] if isinstance(rec["collezioni_dopo"], list)
                   and c in rec["collezioni_dopo"]]
        rec["prova_cancellazione"] = "nessuna collezione dello script rimasta" if not restano else \
            f"ATTENZIONE: restano {restano}"
        stampa(f"  Collezioni dello script cancellate: {', '.join(rest) or 'nessuna da cancellare'}; "
               f"{rec['prova_cancellazione']}. Restano: {rec['collezioni_dopo']}")
        if a.tieni_grandi:
            rec["nota"] = f"{COLL_GRANDI} tenuta (--tieni-grandi): sparisce con il Terminate"
        else:
            st.d.setdefault("grandi", {})["finito"] = False  # cancellata: un nuovo grandi_carica la ricarica
    return _fase_semplice(g, a, indice, "fine", corpo)


def fase_atlas(quale):
    def f(g, a, st, piano, nome, indice):
        rec = {"fase": f"atlas_{quale}", "tipo": "atlas", "inizio": iso_us(),
               "file": file_fase(g.cartella, indice, f"atlas_{quale}")}
        if a.senza_atlas:
            rec["nota"] = "saltata (--senza-atlas)"
        else:
            rec["raccolta"] = raccolta_atlas(os.path.join(g.cartella, f"atlas_{quale}_{ora().strftime('%H%M%S')}"), a, g)
        rec["fine"] = iso_us()
        scrivi_json(os.path.join(g.cartella, rec["file"]), rec)
        return rec
    return f


def fase_lettura(f):
    def run(g, a, st, piano, nome, indice):
        tipo = "spiega" if f == "spiega" else "find"
        compiti = piano.get(f, [])
        if f.startswith("grandi_"):  # i clienti e gli _id dei documenti da 2 KB si conoscono solo dopo il caricamento
            p2, avv, _ = piano_giro16(st, a, nome, [f])
            compiti = p2.get(f, [])
            for x in avv:
                stampa("AVVISO:", x)
        return g.esegui_fase(indice, f, compiti, tipo=tipo)
    return run


FUNZIONI_FASE = {"atlas_inizio": fase_atlas("inizio"), "inizio": fase_inizio, "parallelo": fase_parallelo,
                 "carico_massimo": fase_carico_massimo, "cancellazione": fase_cancellazione,
                 "scritture": fase_scritture, "grandi_carica": fase_grandi_carica,
                 "grandi_lavaggio": fase_grandi_lavaggio, "freddo": fase_freddo, "fine": fase_fine,
                 "condizioni": fase_condizioni, "mentre_scrivi": fase_mentre_scrivi,
                 "atlas_fine": fase_atlas("fine")}
for _f in FASI_LETTURA + ("spiega",):
    FUNZIONI_FASE[_f] = fase_lettura(_f)


def raggiungibile(g, a):
    """Ping al cluster; se non risponde riprova ogni 30 s per a.attesa_rete_min minuti."""
    fine = time.time() + a.attesa_rete_min * 60
    while not STOP.is_set():
        try:
            g.mon.admin.command("ping")
            return True
        except PyMongoError as e:
            stampa(f"ATTENZIONE: il cluster non risponde ({pulito(e)}). Riprovo tra 30 s (fino a "
                   f"{a.attesa_rete_min:g} minuti): controlla VPN e rete.")
        if time.time() > fine:
            return False
        dorme(30)
    return False


def riassunto_di(rec):
    t = rec.get("tipo")
    if t == "parallelo":
        return riassumi_parallelo(rec)
    if t == "scritture":
        return riassumi_scritture(rec)
    if "misure" in rec and t in (None, "spiega"):
        return riassumi_fase(rec)
    return {"fase": rec.get("fase"), "inizio": rec.get("inizio"), "fine": rec.get("fine"),
            "riassunto": rec.get("riassunto"), "esito": rec.get("esito"), "accettata": rec.get("accettata")}


def stampa_qualsiasi_fase(rec):
    t = rec.get("tipo")
    if t == "parallelo":
        stampa_parallelo(rec)
    elif t == "scritture":
        stampa_scritture(rec)
    elif t in (None, "spiega") and "misure" in rec:
        rec["riassunto"] = riassumi_fase(rec)
        stampa_fase(rec)
    else:
        extra = ""
        if t == "grandi_carica" and rec.get("caricamento"):
            c = rec["caricamento"]
            extra = f": {c.get('byte_bson', 0) / GB:.3f} GiB in {c.get('secondi', 0) / 60:.1f} minuti, {c.get('mib_s')} MiB/s"
        elif t == "grandi_lavaggio":
            extra = f": {rec.get('esito')}"
        elif t == "freddo":
            extra = ": accettata" if rec.get("accettata") else f": rifiutata ({_get(rec, 'risposta_create', 'errore')})"
        elif t == "carico_massimo":
            extra = "; ".join(f" {x.get('parte')}: {x.get('operazioni_al_secondo')} op/s, CPU Atlas "
                              f"{_get(x, 'cpu_atlas', 'cpu_percento_di_un_core')}% di un core, limite: {x.get('limite')}"
                              for x in rec.get("parti", []))
        elif t == "cancellazione":
            r = rec.get("riassunto") or {}
            pw = r.get("per_w") or {}
            extra = (f": {r.get('documenti_cancellati')} documenti, deleteMany p50 {_r(r.get('ms') or {})} ms dal Mac "
                     f"(w=1 {_r((pw.get('w1') or {}).get('ms') or {})}, majority "
                     f"{_r((pw.get('majority') or {}).get('ms') or {})}), "
                     f"dataSize {r.get('dataSize_prima')} -> {r.get('dataSize_dopo')}")
        elif t == "condizioni":
            extra = "; ".join(f" {c}: {x.get('esito') or x.get('errore')} (nodo {x.get('nodo') or 'replica set'})"
                              for c, x in (rec.get("celle") or {}).items())
        elif t == "mentre_scrivi":
            extra = "; ".join(f" {c}: scritture {_get(x, 'scritture_durante_la_cella', 'lotti_al_secondo_ottenuti')} "
                              f"lotti/s, {_get(x, 'scritture_durante_la_cella', 'mb_al_secondo')} MB/s"
                              for b in rec.get("blocchi", []) for c, x in b.get("celle", {}).items())
        elif t == "atlas":
            r = rec.get("raccolta") or {}
            extra = f": {r.get('riuscite')} chiamate riuscite su {r.get('chiamate')}" if r.get("fatto") else \
                f": {rec.get('nota') or r.get('motivo')}"
        print(f"=== FASE {rec.get('fase')}: {rec.get('inizio')} - {rec.get('fine')} UTC{extra}")


def cmd_giro(a):
    """Il giro finale su M30, tutto in un comando. Si riprende con --da-fase FASE (nella stessa cartella, senza
    riusare i clienti gia' letti). Ogni fase salva i suoi file appena finisce; un errore in una fase non ferma le
    altre; Ctrl+C salva quello che c'e'."""
    _tb = traceback
    segnali_giro(a.cartella_dati)  # p1-1.6.1: anche la finestra del Terminale chiusa (SIGHUP) salva
    st = Stato(a.file)
    if "clienti" not in st.d:
        print(f"Nel file {a.file} non c'e' un caricamento: serve il file di stato di ieri (risultati_p1.json).")
        sys.exit(2)
    # p1-1.6.1 (A2 di A, E8 di B): i clienti segnati su file prima di ogni lettura fredda tornano negli 'usati' (dopo
    # un Mac spento di colpo il file di stato puo' non averli)
    ru = RegistroUsati(a.file)
    la, lg = ru.leggi()
    nuovi_a, nuovi_g = la - set(st.d.get("usati", [])), lg - set(st.d.get("usati_grandi", []))
    if nuovi_a or nuovi_g:
        st.d["usati"] = sorted(set(st.d.get("usati", [])) | la)
        st.d["usati_grandi"] = sorted(set(st.d.get("usati_grandi", [])) | lg)
        print(f"Dal registro {ru.path}: {len(nuovi_a)} clienti e {len(nuovi_g)} clienti da 2 KB gia' letti che il file "
              f"di stato non aveva (Mac spento di colpo?): segnati come usati, non si rileggono.")
    gf = st.d.get("giro_finale") or {}
    nome = a.giro or gf.get("nome") or "m30"
    if a.solo_fasi:
        fasi = [x.strip() for x in a.solo_fasi.split(",") if x.strip()]
    elif a.da_fase:
        if a.da_fase not in FASI_GIRO:
            print(f"Fase sconosciuta: {a.da_fase}. Fasi: {', '.join(FASI_GIRO)}")
            sys.exit(2)
        fasi = list(FASI_GIRO[FASI_GIRO.index(a.da_fase):])
    else:
        fasi = list(FASI_GIRO)
    senza = {x.strip() for x in (a.senza or "").split(",") if x.strip()}
    sconosciute = [f for f in fasi + sorted(senza) if f not in FASI_GIRO]
    if sconosciute:
        print(f"Fasi sconosciute: {', '.join(sconosciute)}. Fasi: {', '.join(FASI_GIRO)}")
        sys.exit(2)
    fasi = [f for f in fasi if f not in senza]
    piano, avvisi, conti = piano_giro16(st, a, nome, fasi)
    tot = stima_minuti(fasi)
    grandi_da_caricare = "grandi_carica" in fasi and not (st.d.get("grandi") or {}).get("finito")
    if grandi_da_caricare:  # caricamento e lavaggio dei 2 KB dentro questo giro (non fatti su M10)
        tot += MINUTI_GRANDI_SU_M30
    print(f"Giro finale '{nome}' (versione {VERSIONE}). Clienti attivi mai letti {conti['attivi_mai_letti']}, dormienti "
          f"mai letti {conti['dormienti_mai_letti']}" + (f", documenti da 2 KB mai letti {conti['grandi_mai_letti']}"
                                                        if "grandi_mai_letti" in conti else "") + ".")
    print(f"Il piano usa {conti.get('attivi_usati_dal_piano', 0)} clienti attivi mai letti su "
          f"{conti['attivi_mai_letti']} e {conti.get('dormienti_usati_dal_piano', 0)} dormienti su "
          f"{conti['dormienti_mai_letti']} (ogni cliente in una sola fase).")
    print("Ordine delle fasi e minuti STIMATI (pausa di 2 minuti compresa dove c'e'):")
    acc = 0.0
    for k, f in enumerate(fasi, 1):
        acc += MINUTI_STIMATI.get(f, 0)
        n = piano.get(f)
        quanti = quanti_nel_piano(n)
        extra = f" (+{MINUTI_FREDDO_ACCETTATA - MINUTI_STIMATI['freddo']} se Atlas accetta la collezione fredda)" \
            if f == "freddo" else ""
        if f == "grandi_carica" and grandi_da_caricare:
            extra = f" (+{MINUTI_GRANDI_SU_M30} con grandi_lavaggio: i 2 KB NON sono stati caricati su M10)"
        print(f"  {k:2d}. {f:16s} {MINUTI_STIMATI.get(f, 0):5.1f} min  (a {acc:5.1f})"
              + (f"  {quanti} ricerche/clienti" if quanti is not None else "") + extra)
    print(f"Totale STIMATO: circa {tot:.0f} minuti ({tot / 60:.1f} ore), piu' circa "
          f"{MINUTI_FREDDO_ACCETTATA - MINUTI_STIMATI['freddo']} se la collezione fredda e' accettata.")
    if conti.get("parallelo_livelli_gia_fatti"):
        print(f"Parallelo: livelli gia' finiti in un'esecuzione prima, non si rifanno: "
              f"{', '.join(conti['parallelo_livelli_gia_fatti'])}")
    if "grandi_carica" in fasi:  # gli _id dei documenti da 2 KB arrivano con il caricamento: avviso atteso
        avvisi = [x for x in avvisi if not x.startswith("grandi_Q2")]
    for x in avvisi:
        print("AVVISO:", x)
    # p1-1.6.1 (errore 2 di A, E7 di B): se i clienti mai letti non bastano il giro NON parte. Su una ripresa
    # (--da-fase, --solo-fasi) i dormienti che mancano sono normali (una fase interrotta ne ha gia' usati): solo avviso.
    ripresa = bool(a.da_fase or a.solo_fasi)
    bloccanti = [x for x in avvisi if not (ripresa and "dormienti" in x)]
    if bloccanti and not a.anche_se_pochi:
        print(f"\nFERMO: clienti mai letti insufficienti per: {'; '.join(bloccanti)}.\nIl giro NON parte (nessuna "
              f"connessione, niente letto). Scrivi al coordinatore queste righe. Per partire lo stesso (solo se il "
              f"coordinatore dice si'): aggiungi --anche-se-pochi.")
        sys.exit(2)
    if a.a_secco:
        for f in fasi:
            x = piano.get(f)
            if isinstance(x, dict):
                for k2, v in x.items():
                    if v and "compiti" in v[0]:  # parallelo: lotti
                        print(f"{f} {k2}: {len(v)} lotti, {sum(len(lo['compiti']) for lo in v)} clienti; primi lotti: "
                              + "; ".join(f"{lo['query']} x{len(lo['compiti'])}" for lo in v[:3]))
                    else:  # condizioni, mentre_scrivi: celle
                        print(f"{f} {k2}: {len(v)} clienti; primi 2: " + "; ".join(
                            f"{c.get('cliente')} {c['spec']['filtro']}" for c in v[:2]))
            elif x:
                print(f"{f}: {len(x)} ricerche; prime 2: " +
                      "; ".join(f"{c.get('cliente') or 'finestra'} [{c.get('gruppo')}] {c['spec']['filtro']}"
                                for c in x[:2]))
        print("A secco: nessuna connessione, niente scritto.")
        return
    copia = a.file + ".prima-del-giro-finale.json"
    if os.path.exists(a.file) and not os.path.exists(copia):
        shutil.copyfile(a.file, copia)
    cart = gf.get("cartella") if gf.get("cartella") and os.path.isdir(gf.get("cartella", "")) and not a.nuova_cartella \
        else None
    g = Giro(a, "giro_finale", nome, cartella=cart)
    os.makedirs(g.cartella, exist_ok=True)
    gf.update({"nome": nome, "cartella": g.cartella, "versione": VERSIONE})
    gf.setdefault("fasi", {})
    gf.setdefault("sessioni", []).append({"inizio": iso_us(), "fasi_chieste": fasi})
    st.d["giro_finale"] = gf
    st.salva()
    stampa(f"Dati grezzi in: {os.path.abspath(g.cartella)}" + (" (stessa cartella dell'esecuzione di prima)" if cart else ""))
    usati, usati_g = set(st.d.get("usati", [])), set(st.d.get("usati_grandi", []))
    lock = threading.Lock()

    def prenota(cs):
        """p1-1.6.1: prima della lettura fredda, i clienti si scrivono sul registro (flush e fsync) e negli usati."""
        voci = [("g" if c.get("gruppo") == "grandi" else "a", c["i"]) for c in cs
                if c.get("i") is not None and c.get("gruppo") not in ("fredda", "finestre", "gia_letti")]
        if not voci:
            return
        try:
            ru.scrivi(voci)
        except OSError as e:  # disco pieno o simile: si scrive e si va avanti (resta il salvataggio ogni 20)
            stampa(f"ATTENZIONE: registro dei clienti non scritto ({e})")
        with lock:
            for t, i in voci:
                (usati_g if t == "g" else usati).add(i)

    conta_segnati = [0]

    def salva_stato():
        with lock:
            st.d["usati"] = sorted(usati)
            st.d["usati_grandi"] = sorted(usati_g)
            st.salva()

    def segna(m):
        i = m.get("i")
        if i is None or m.get("gruppo") in ("fredda", "finestre", "gia_letti"):
            return
        with lock:
            (usati_g if m.get("gruppo") == "grandi" else usati).add(i)
            conta_segnati[0] += 1
            subito = conta_segnati[0] % 20 == 0
        if subito:  # anche durante la fase: se il Mac si spegne di colpo, i clienti letti restano segnati
            salva_stato()
    g.segna = segna
    g.prenota = prenota

    sessione = ora().strftime("%Y%m%dT%H%M%SZ")
    interrotta = None
    try:
        g.avvia_campioni()
        for f in fasi:
            if STOP.is_set():
                interrotta = interrotta or f
                break
            indice = FASI_GIRO.index(f) + 1
            if f not in ("atlas_inizio", "atlas_fine") and not raggiungibile(g, a):
                interrotta = f
                stampa(f"Il cluster non risponde da {a.attesa_rete_min:g} minuti: mi fermo. Quando la rete torna, "
                       f"riprendi con la riga 'giro --da-fase ...' scritta in fondo.")
                break
            senza_pausa = (f == "grandi_carica" and (st.d.get("grandi") or {}).get("finito")) or \
                (f == "freddo" and a.fredda_solo_carica)  # niente misure da separare sui grafici
            if f in PAUSA_PRIMA and a.pausa_s > 0 and not senza_pausa:
                g.fase = f"pausa prima di {f}"
                p0 = iso_us()
                stampa(f"Pausa di {a.pausa_s:g} s prima della fase {f} (i grafici di Atlas si separano per fase).")
                dorme(a.pausa_s)
                gf["fasi"].setdefault(f"pausa prima di {f}", []).append({"inizio": p0, "fine": iso_us()})
                if STOP.is_set():
                    interrotta = f
                    break
            g.fase = f
            esito = {"inizio": iso_us(), "sessione": sessione}
            stampa(f"===== {f} (fase {fasi.index(f) + 1} di {len(fasi)}; STIMATO {MINUTI_STIMATI.get(f, 0):g} minuti) =====")
            try:
                if os.environ.get("P1_PROVA_GUASTO") == f:  # solo per la prova locale: un errore inatteso in una fase
                    raise RuntimeError(f"guasto di prova nella fase {f}")
                r = FUNZIONI_FASE[f](g, a, st, piano, nome, indice) or {}
                esito["file"] = r.get("file")
                esito["stato"] = "interrotta" if STOP.is_set() else ("fermata" if r.get("fermata") else "fatta")
                if f == "freddo" and a.fredda_solo_carica and esito["stato"] == "fatta":
                    # p1-1.6.1: su M10 la fase fredda fa solo creazione e copia: non e' finita (letture su M30)
                    esito["stato"] = "solo_caricata"
            except Exception as e:
                esito.update({"stato": "errore", "errore": pulito(e), "traccia": scrub(_tb.format_exc())[-4000:]})
                stampa(f"ERRORE nella fase {f}: {pulito(e)}. Salvato; vado avanti con la fase successiva.")
                scrivi_json(os.path.join(g.cartella, f"errore_{indice:02d}_{f}_{ora().strftime('%H%M%S')}.json"), esito)
            finally:
                if g.profilo_acceso:
                    g.profiler_spento()
                esito["fine"] = iso_us()
                gf["fasi"].setdefault(f, []).append(esito)
                salva_stato()
                if esito.get("stato") == "interrotta":
                    interrotta = f
    finally:
        g.ferma_campioni()
        if g.profilo_acceso:
            g.profiler_spento()
        salva_stato()
        scrivi_json(os.path.join(g.cartella, f"sessione_{sessione}_serverStatus_ogni_30s.json"), g.campioni)
        scrivi_json(os.path.join(g.cartella, f"sessione_{sessione}_eventi_driver.json"),
                    [g.reg.esporta(), g.reg_c.esporta()])
        scrivi_json(os.path.join(g.cartella, "stato_delle_fasi.json"), gf)
        g.chiudi()
        ru.chiudi()
        # tutto.json.gz: i file delle fasi copiati uno dopo l'altro, senza rileggerli tutti in memoria (sul giro vero
        # possono essere centinaia di MB); i riassunti si calcolano una fase per volta
        import gzip
        file_fasi = [nf for nf in sorted(os.listdir(g.cartella)) if nf.startswith("fase_") and nf.endswith(".json")]
        with gzip.open(os.path.join(g.cartella, "tutto.json.gz"), "wt") as fz:
            fz.write('{"versione": "%s", "giro": %s, "scritto": "%s", "stato_delle_fasi": %s, "fasi": [\n' % (
                VERSIONE, json.dumps(nome), iso_us(), scrub(json_util.dumps(gf, json_options=JOPT))))
            for k, nf in enumerate(file_fasi):
                with open(os.path.join(g.cartella, nf)) as f1:
                    fz.write(("," if k else "") + f1.read() + "\n")
            fz.write("]}\n")
        an = _prova(lambda: analisi_giro(g.cartella))
        riass = {"versione": VERSIONE, "scritto": iso_us(), "analisi": an, "fasi": []}
        print()
        print(f"=== RIASSUNTO DEL GIRO FINALE '{nome}' (dai dati grezzi; si rifa' con: riassunto --cartella "
              f"{g.cartella}) ===")
        for nf in file_fasi:
            try:
                fr = leggi_json(os.path.join(g.cartella, nf))
                riass["fasi"].append(_prova(lambda fr=fr: riassunto_di(fr)))
                stampa_qualsiasi_fase(fr)
            except Exception as e:
                print(f"(riassunto di {nf} non riuscito: {pulito(e)})")
            fr = None
        scrivi_json(os.path.join(g.cartella, "RIASSUNTO.json"), riass)
        print()
        print("Stato delle fasi:")
        for f in FASI_GIRO:
            if f in gf["fasi"]:
                ult = gf["fasi"][f][-1]
                print(f"  {f:16s} {ult.get('stato', '?'):10s} {ult.get('file') or ''} {ult.get('errore') or ''}")
        mancano = [f for f in FASI_GIRO if f not in senza and not any(e.get("stato") == "fatta"
                                                                          for e in gf["fasi"].get(f, []))]

        def stato_qui(f):  # l'ultimo esito della fase in questa esecuzione, o None se non e' arrivata a farla
            es = [e for e in gf["fasi"].get(f, []) if e.get("sessione") == sessione]
            return es[-1].get("stato") if es else None
        con_errore = [f for f in fasi if stato_qui(f) == "errore"]
        if interrotta or STOP.is_set():
            # si riprende dalla prima fase non finita (interrotta, fermata per la rete o mai partita); le fasi con un
            # errore inatteso si rifanno a parte, una per una
            da = next((f for f in fasi if stato_qui(f) in (None, "interrotta", "fermata")), interrotta)
            print(f"\nGIRO INTERROTTO. Per riprendere (stessa cartella, nessun cliente riletto):\n"
                  f"  python3 prova_infinite.py giro --da-fase {da}")
            if con_errore:
                print(f"Poi, solo se il coordinatore dice si', le fasi con un errore:  python3 prova_infinite.py giro "
                      f"--solo-fasi {','.join(con_errore)}")
        elif mancano:
            print(f"\nFasi non riuscite: {', '.join(mancano)}. Si rifanno una per una con:  python3 prova_infinite.py giro "
                  f"--solo-fasi {','.join(mancano)}")
        else:
            print("\nGIRO FINITO: tutte le fasi fatte. Prossimo passo: consegna (vedi GIRO-FINALE.md).")
        if isinstance(an, dict) and "fasi" in an:
            print()
            stampa_analisi(an)
        n_file = scrivi_indice(g.cartella)
        print(f"Cartella: {os.path.abspath(g.cartella)} ({n_file} file, elencati con SHA-256 in INDICE.json)")


def cmd_consegna(a):
    """Impacchetta la cartella del giro (o --cartella) in pezzi da al massimo 45 MB, pronti per git o per la chat."""
    import gzip
    import tarfile
    st = Stato(a.file)
    cart = a.cartella or (st.d.get("giro_finale") or {}).get("cartella")
    if not cart or not os.path.isdir(cart):
        print("Non trovo la cartella del giro: indica --cartella.")
        sys.exit(2)
    nome = os.path.basename(os.path.normpath(cart))
    dest = os.path.join(a.cartella_dati, f"consegna_{nome}")
    os.makedirs(dest, exist_ok=True)
    arch = os.path.join(dest, f"{nome}.tar.gz")
    scrivi_indice(cart)
    altri = [x for x in sorted(os.listdir(a.cartella_dati)) if
             x.startswith(("composizione_", "atlas_", "latenza_", "scrivi_", "fredda_"))]
    with tarfile.open(arch, "w:gz") as t:
        t.add(cart, arcname=nome)
        for x in altri:
            t.add(os.path.join(a.cartella_dati, x), arcname=f"altri/{x}")
        for x in (a.file, a.file + ".prima-del-giro-finale.json", a.file + ".usati.log"):
            if os.path.exists(x):
                t.add(x, arcname=f"stato/{os.path.basename(x)}")
    pezzo, pezzi = 45 * MB, []
    with open(arch, "rb") as f:
        k = 0
        while True:
            b = f.read(pezzo)
            if not b:
                break
            nomep = f"{arch}.parte{k:02d}"
            with open(nomep, "wb") as g2:
                g2.write(b)
            pezzi.append({"file": os.path.basename(nomep), "byte": len(b), "sha256": hashlib.sha256(b).hexdigest()})
            k += 1
    os.remove(arch)
    with open(os.path.join(dest, "LEGGIMI.txt"), "w") as f:
        f.write(f"Giro finale P1 ({VERSIONE}), impacchettato {iso_us()}.\nPer ricomporre:  cat {nome}.tar.gz.parte* > "
                f"{nome}.tar.gz && tar xzf {nome}.tar.gz\nPezzi (sha256):\n" +
                "".join(f"  {p['file']}  {p['byte']} byte  {p['sha256']}\n" for p in pezzi))
    _ = gzip
    print(f"Pronto: {len(pezzi)} pezzi in {os.path.abspath(dest)} (ognuno al massimo 45 MB):")
    for p in pezzi:
        print(f"  {p['file']}  {p['byte'] / MB:.1f} MB")


# ----------------------------------------------------------------------------- comandi brevi

def cmd_riassunto(a):
    if a.cartella:
        t = os.path.join(a.cartella, "tutto.json")
        lat = os.path.join(a.cartella, "latenza.json")
        if os.path.exists(lat):
            rec = leggi_json(lat)
            rec["riassunto"] = riassumi_latenza(rec)
            stampa_latenza(rec)
            return
        fasi = []
        if os.path.exists(t):
            fasi = leggi_json(t).get("fasi", [])
        if not fasi:  # p1-1.6: una fase per volta (i file possono essere grandi)
            fasi = (leggi_json(os.path.join(a.cartella, nomef)) for nomef in sorted(os.listdir(a.cartella))
                    if nomef.startswith("fase_") and nomef.endswith(".json"))
        for fr in fasi:
            stampa_qualsiasi_fase(fr)  # ricalcolato dai dati grezzi (p1-1.6: anche parallelo e scritture)
        if os.path.exists(os.path.join(a.cartella, "stato_delle_fasi.json")):
            stampa_analisi(analisi_giro(a.cartella))
        return
    st = Stato(a.file)
    car = st.d.get("caricamento", {})
    if car:
        print(f"Caricamento: {car.get('byte_bson', 0) / GB:.2f} GB, {car.get('documenti', 0):,} documenti, "
              f"{(car.get('secondi') or 0) / 3600:.2f} ore, fine {car.get('fine')} ({car.get('motivo_fine')})")
    for g in st.d.get("giri", []):
        print(f"Giro vecchio '{g.get('nome')}' ({g.get('inizio')} - {g.get('fine')}): vedi RISULTATI-CORRETTI.md")
    for g in st.d.get("giri_p15", []):
        print(f"Giro p1-1.5 '{g['nome']}': riassunto con  riassunto --cartella {g['cartella']}")


def cmd_controlla(a):
    client = connetti(pool=1)
    info = client.admin.command("buildInfo")
    xs = [hello_ms(client) for _ in range(30)]
    xs = [hello_ms(client) for _ in range(100)]
    print(f"Connesso. Versione del server: {info.get('version')}. Versione dello script: {VERSIONE}.")
    print(f"Rete fino al cluster (hello): p50 {percentile(xs, 50):.1f} ms, p95 {percentile(xs, 95):.1f} ms, "
          f"p99 {percentile(xs, 99):.1f} ms.")
    try:
        s = client[a.db].command("dbStats")
        print(f"Database '{a.db}': {s.get('objects', 0):,} documenti, dati {s.get('dataSize', 0) / GB:.2f} GB.")
    except PyMongoError as e:
        print("dbStats:", pulito(e))
    print("Atlas Admin API: chiave " + ("presente nell'ambiente." if os.environ.get("ATLAS_PUBLIC_KEY") and
                                        os.environ.get("ATLAS_PRIVATE_KEY") else "ASSENTE (ATLAS_PUBLIC_KEY / ATLAS_PRIVATE_KEY)."))
    client.close()


def cmd_pulisci(a):
    client = connetti(pool=1)
    client.drop_database(a.db)
    print(f"Database '{a.db}' cancellato. Il cluster resta: cancellalo dalla console.")


def cmd_scrivi(a):
    """Scritture singole con documenti da circa 2 KB in una collezione a parte (B23): misura i byte inseriti al
    secondo; si legge nello stesso intervallo Storage Throughput > Bytes Written del primario.
    Con --per-cliente: collezione clustered con _id = {cliente, ora} (revisore D1: inserimenti nel mezzo
    dell'albero invece che in fondo). Alla fine cancella la collezione (salvo --tieni)."""
    signal.signal(signal.SIGINT, ferma)
    signal.signal(signal.SIGTERM, ferma)
    client = connetti(pool=a.thread_scrivi + 2)
    db = client[a.db]
    nome_c = "scratch_keyed" if a.per_cliente else "scritture_2kb"
    reg = {"versione": VERSIONE, "comando": "scrivi", "collezione": nome_c, "per_cliente": a.per_cliente,
           "clienti": a.clienti_scrivi if a.per_cliente else 5000, "thread": a.thread_scrivi}
    reg["serverStatus_prima"] = _prova(lambda: client.admin.command("serverStatus"))
    if a.per_cliente:
        try:
            db.create_collection(nome_c, clusteredIndex={"key": {"_id": 1}, "unique": True})
            reg["creata"] = "clustered su _id"
        except PyMongoError as e:
            reg["creata"] = {"errore": pulito(e)}
            stampa("La collezione clustered non si crea:", pulito(e), "- mi fermo (scritto nel file).")
            scrivi_json(os.path.join(a.cartella_dati, f"scrivi_{ora().strftime('%Y%m%dT%H%M%SZ')}.json"), reg)
            return
    col = db[nome_c]
    if not a.per_cliente:
        col.create_index([("cliente", 1), ("ts", -1)], name="cliente_ts")
    tetto = a.tetto_scrivi_gb * GB
    fine = time.time() + a.minuti * 60
    lock = threading.Lock()
    tot = {"doc": 0, "byte": 0, "errori": 0}

    def lavora(k):
        rng = random.Random(a.seme * 1000 + k)
        while not STOP.is_set() and time.time() < fine:
            with lock:
                if tot["byte"] >= tetto:
                    return
            if a.per_cliente:
                d = doc_2kb(rng, f"u{rng.randrange(a.clienti_scrivi):07d}")
                d["_id"] = {"c": d["cliente"], "t": d["ts"], "k": rng.getrandbits(48)}
            else:
                d = doc_2kb(rng, f"s2k-{rng.randrange(5000):05d}")
                d["_id"] = ObjectId()
            n = len(bson.encode(d))  # con l'_id (correzione S9 di A2)
            try:
                col.insert_one(d)
                with lock:
                    tot["doc"] += 1
                    tot["byte"] += n
            except PyMongoError as e:
                with lock:
                    tot["errori"] += 1
                if tot["errori"] <= 3:
                    stampa("errore di scrittura:", pulito(e))
                time.sleep(1)

    t0 = time.time()
    stampa(f"Scritture singole da circa 2 KB nella collezione {nome_c} per {a.minuti:g} minuti con "
           f"{a.thread_scrivi} thread (tetto {a.tetto_scrivi_gb:g} GB). INIZIO {dt.datetime.now().strftime('%H:%M:%S')}")
    th = [threading.Thread(target=lavora, args=(k,), daemon=True) for k in range(a.thread_scrivi)]
    for t in th:
        t.start()
    prima = (t0, 0)
    campioni = []
    while any(t.is_alive() for t in th):
        time.sleep(1)
        adesso = time.time()
        if adesso - prima[0] >= 30:
            with lock:
                b, nd = tot["byte"], tot["doc"]
            campioni.append({"t": iso_us(), "documenti": nd, "byte": b})
            stampa(f"inseriti {nd:,} documenti, {b / MB:.1f} MB, ultimi 30 s {(b - prima[1]) / MB / (adesso - prima[0]):.2f} MB/s")
            prima = (adesso, b)
    dur = time.time() - t0
    medio = tot["byte"] / max(1, tot["doc"])
    stampa(f"FINE: {tot['doc']:,} documenti, {tot['byte'] / MB:.1f} MB in {dur / 60:.1f} minuti, "
           f"media {tot['byte'] / MB / dur:.2f} MB/s, documento medio {medio:,.0f} byte, errori {tot['errori']}.")
    reg.update({"inizio": iso(dt.datetime.fromtimestamp(t0, dt.timezone.utc)), "fine": iso_us(),
                "minuti": round(dur / 60, 2), "documenti": tot["doc"], "byte": tot["byte"],
                "mb_s": round(tot["byte"] / MB / dur, 3), "documento_medio": round(medio), "errori": tot["errori"],
                "campioni_30s": campioni})
    reg["serverStatus_dopo"] = _prova(lambda: client.admin.command("serverStatus"))
    reg["collStats"] = _prova(lambda: db.command("collStats", nome_c))
    st = Stato(a.file)
    st.d.setdefault("scritture", []).append({k: v for k, v in reg.items() if not k.startswith(("serverStatus", "collStats"))})
    st.salva()
    if not a.tieni:
        col.drop()
        reg["cancellata"] = iso_us()
        print(f"Collezione {nome_c} cancellata (le collezioni del caricamento restano).")
    scrivi_json(os.path.join(a.cartella_dati, f"scrivi_{nome_c}_{ora().strftime('%Y%m%dT%H%M%SZ')}.json"), reg)
    client.close()


def cmd_composizione(a):
    client = connetti(pool=1)
    composizione(client, a)
    client.close()


def composizione(client, a, st=None):
    """Legge solo i numeri di controllo (dbStats, collStats, oplog): non legge i documenti dei clienti."""
    db = client[a.db]
    risultato = {"quando": iso(ora()), "versione": VERSIONE}

    def gb(x):
        return "non riportato" if x is None else f"{x / GB:.3f} GB"

    def num(x):
        return "non riportato" if x is None else f"{x:,}"

    try:
        s = db.command("dbStats")
        risultato["dbStats"] = {k: s.get(k) for k in ("objects", "dataSize", "storageSize", "indexSize", "totalSize",
                                                       "fsUsedSize", "fsTotalSize")}
        risultato["dbStats_completo"] = s
        print(f"Database '{a.db}': {num(s.get('objects'))} documenti; dati {gb(s.get('dataSize'))}, "
              f"spazio dati {gb(s.get('storageSize'))}, indici {gb(s.get('indexSize'))}, totale {gb(s.get('totalSize'))}")
    except PyMongoError as e:
        print("dbStats:", pulito(e))
    risultato["collezioni"] = {}
    for c in sorted(db.list_collection_names()):
        try:
            cs = db.command("collStats", c)
            risultato["collezioni"][c] = {k: cs.get(k) for k in ("count", "size", "storageSize", "totalIndexSize",
                                                                  "indexSizes", "avgObjSize")}
            print(f"  {c}: {num(cs.get('count'))} documenti, dati {gb(cs.get('size'))}, spazio {gb(cs.get('storageSize'))}, "
                  f"indici {gb(cs.get('totalIndexSize'))} {cs.get('indexSizes') or ''}")
        except PyMongoError as e:
            print(f"  {c}: collStats non disponibile:", pulito(e))
    loc = client["local"]
    try:
        cs = loc.command("collStats", "oplog.rs")
        risultato["oplog"] = {k: cs.get(k) for k in ("count", "size", "storageSize", "maxSize")}
        print(f"Oplog: {num(cs.get('count'))} voci, dati {gb(cs.get('size'))}, spazio {gb(cs.get('storageSize'))}, "
              f"tetto {gb(cs.get('maxSize'))}")
    except PyMongoError as e:
        print("Oplog (collStats): non disponibile:", pulito(e))
    try:
        primo = loc["oplog.rs"].find({}, {"ts": 1}).sort("$natural", 1).limit(1).next()["ts"]
        ultimo = loc["oplog.rs"].find({}, {"ts": 1}).sort("$natural", -1).limit(1).next()["ts"]
        ore = (ultimo.time - primo.time) / 3600
        risultato["oplog_finestra_ore"] = round(ore, 2)
        risultato["oplog_prima_voce"] = iso(dt.datetime.fromtimestamp(primo.time, dt.timezone.utc))
        print(f"Oplog: finestra di {ore:.1f} ore (dalla voce piu' vecchia alla piu' nuova, la piu' vecchia e' del "
              f"{risultato['oplog_prima_voce']})")
    except Exception as e:
        print("Oplog (finestra): non disponibile:", pulito(e))
    st = st or Stato(a.file)  # p1-1.6: dentro 'giro' si usa lo stesso stato del giro (niente scritture incrociate)
    st.d.setdefault("composizione", []).append({k: v for k, v in risultato.items() if k != "dbStats_completo"})
    st.salva()
    scrivi_json(os.path.join(a.cartella_dati, f"composizione_{ora().strftime('%Y%m%dT%H%M%SZ')}.json"), risultato)
    return risultato


def cmd_estese(a):
    print("Il comando 'estese' e' stato TOLTO nella versione p1-1.5: misurava letture 'fredde' che erano calde "
          "(revisori A2 e B). Non usarlo. Q4 e Q5 sono ora fasi del comando 'misura'.")
    sys.exit(2)


def cmd_tutto(a):
    signal.signal(signal.SIGINT, ferma)
    signal.signal(signal.SIGTERM, ferma)
    t_inizio = time.time()
    scad = t_inizio + a.max_ore * 3600
    st = cmd_carica(a)
    if not st.d.get("caricamento", {}).get("finito"):
        stampa("Caricamento non finito: mi fermo.")
        return
    fine = dt.datetime.strptime(st.d["caricamento"]["fine"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc)
    fatti = {g["nome"] for g in st.d.get("giri", [])} | {g["nome"] for g in st.d.get("giri_p15", [])}
    for h in [float(x) for x in a.giri.split(",")]:
        nome = f"{h:g}h"
        if nome in fatti:
            continue
        quando = fine + dt.timedelta(hours=h, minutes=a.attesa_iniziale_min if h == 0 else 0)
        attesa = (quando - ora()).total_seconds()
        if time.time() + max(0, attesa) + 1800 > scad:
            stampa(f"Il giro {nome} supererebbe il tetto di {a.max_ore:g} ore: mi fermo qui.")
            break
        if attesa > 0:
            stampa(f"Prossimo giro {nome} alle {quando.astimezone().strftime('%d/%m %H:%M')} "
                   f"(tra {attesa / 3600:.1f} ore). Puoi lasciare il computer acceso; non spegnerlo.")
            while not STOP.is_set() and (quando - ora()).total_seconds() > 0:
                dorme(min(1800, (quando - ora()).total_seconds()))
        if STOP.is_set():
            break
        st = cmd_misura(a, nome=nome, stato=Stato(a.file))
    stampa("Fine. Risultati in", os.path.abspath(a.file))


def main():
    p = argparse.ArgumentParser(description="FENVIX P1: letture fredde su Atlas Infinite (p1-1.5)")
    p.add_argument("comando", choices=["giro", "consegna", "controlla", "carica", "misura", "tutto", "riassunto",
                                       "pulisci", "scrivi", "composizione", "estese", "latenza", "scarica_atlas",
                                       "fredda"])
    p.add_argument("--file", default=FILE_DEFAULT, help="file JSON di stato (quello di ieri: clienti e usati)")
    p.add_argument("--db", default=DB_NOME_DEFAULT)
    p.add_argument("--cartella-dati", default=".", help="dove creare le cartelle dei dati grezzi")
    # carica (invariato)
    p.add_argument("--gb", type=float, default=30.0, help="GB di documenti da caricare (BSON)")
    p.add_argument("--mb-cliente", type=float, default=2.0, help="taglia media di un cliente in MB")
    p.add_argument("--quota-riservati", type=float, default=0.30, help="quota di clienti dormienti per le fredde")
    p.add_argument("--thread", type=int, default=4)
    p.add_argument("--lotto", type=int, default=1000, help="documenti per lotto")
    p.add_argument("--seme", type=int, default=42)
    p.add_argument("--tetto-logico-gb", type=float, default=50.0,
                   help="ferma il caricamento quando dati + indici superano questo valore")
    p.add_argument("--ogni-dbstats-s", type=int, default=120)
    p.add_argument("--max-ore-carico", type=float, default=8.0)
    p.add_argument("--forza", action="store_true", help="con 'carica': cancella e rifai il caricamento")
    # tutto
    p.add_argument("--max-ore", type=float, default=40.0, help="tetto totale di 'tutto', dalla partenza")
    p.add_argument("--giri", default="0,2,12,24", help="ore dalla fine del caricamento (con 'tutto')")
    p.add_argument("--attesa-iniziale-min", type=float, default=10.0, help="minuti prima del giro 0")
    # misura (p1-1.5)
    p.add_argument("--giro", default=None, help="nome del giro (con 'misura' e 'latenza')")
    p.add_argument("--fasi", default=FASI_DEFAULT, help="fasi del giro, in ordine")
    p.add_argument("--n-q2", type=int, default=300, help="clienti attivi mai letti per Q2")
    p.add_argument("--senza-q2-dormienti", dest="q2_dormienti", action="store_false",
                   help="non aggiungere a Q2 i dormienti rimasti mai letti (di serie si aggiungono, gruppo a parte)")
    p.add_argument("--n-q3", type=int, default=150)
    p.add_argument("--n-q1d", type=int, default=150)
    p.add_argument("--n-q4", type=int, default=60)
    p.add_argument("--n-q5", type=int, default=60, help="finestre da 7 giorni per Q5")
    p.add_argument("--q5-stato", default="nuovo", help="stato degli ordini per Q5 (M31 usa lo stato 0)")
    p.add_argument("--n-spiega", type=int, default=10, help="ricerche per tipo nella fase explain")
    p.add_argument("--rip-caldo", type=int, default=2, help="letture calde subito dopo ogni fredda")
    p.add_argument("--pausa-s", type=float, default=120.0, help="pausa prima di ogni fase")
    p.add_argument("--min-secondi-fase", type=float, default=150.0,
                   help="ogni fase dura almeno tanto (ricerche distribuite in modo uniforme)")
    p.add_argument("--ogni-s", type=float, default=30.0, help="serverStatus completo ogni N secondi")
    p.add_argument("--profilo-mb", type=float, default=64.0, help="taglia della collezione system.profile")
    p.add_argument("--senza-controllo", dest="controllo", action="store_false",
                   help="non controllare i documenti restituiti (di serie si controlla, dopo le misure)")
    p.add_argument("--senza-atlas", action="store_true", help="non chiamare l'Atlas Admin API")
    p.add_argument("--atlas-da", default=INIZIO_CARICO, help="inizio del periodo per metriche, log ed eventi")
    p.add_argument("--a-secco", action="store_true", help="con 'misura': mostra il piano senza collegarsi")
    p.add_argument("--n-latenza", type=int, default=300, help="scritture per tipo con 'latenza'")
    p.add_argument("--cartella", default=None, help="con 'riassunto': cartella di un giro p1-1.5")
    # scrivi
    p.add_argument("--minuti", type=float, default=8.0, help="con 'scrivi': durata")
    p.add_argument("--thread-scrivi", type=int, default=32, help="con 'scrivi': scritture in parallelo")
    p.add_argument("--tetto-scrivi-gb", type=float, default=2.0, help="con 'scrivi': tetto di byte inseriti")
    p.add_argument("--tieni", action="store_true", help="con 'scrivi': non cancellare la collezione alla fine")
    p.add_argument("--per-cliente", action="store_true", help="con 'scrivi': collezione clustered, _id per cliente")
    p.add_argument("--clienti-scrivi", type=int, default=1_000_000, help="con 'scrivi --per-cliente'")
    # giro (p1-1.6): il comando unico del giro finale
    p.add_argument("--da-fase", default=None, help="con 'giro': riprende da questa fase (stessa cartella)")
    p.add_argument("--solo-fasi", default=None, help="con 'giro': solo queste fasi, separate da virgole")
    p.add_argument("--senza", default=None, help="con 'giro': salta queste fasi, separate da virgole")
    p.add_argument("--nuova-cartella", action="store_true", help="con 'giro': non riusare la cartella del giro di prima")
    p.add_argument("--n-q1", type=int, default=150, help="clienti attivi per Q1 (ultimi 20)")
    p.add_argument("--dorm-q2", type=int, default=40, help="dormienti mai letti per Q2 (punto a 24 ore)")
    p.add_argument("--dorm-q3", type=int, default=24)
    p.add_argument("--dorm-q1", type=int, default=24)
    p.add_argument("--dorm-q1d", type=int, default=24)
    p.add_argument("--dorm-q4", type=int, default=24)
    p.add_argument("--livelli", default="1,10,50,100", help="letture fredde insieme nella fase parallelo")
    p.add_argument("--par-lotti", default="30,8,4,3", help="lotti per ricerca a ogni livello (clienti = lotti x livello)")
    p.add_argument("--par-query", default="Q2,Q3,Q1", help="ricerche della fase parallelo, a turno")
    p.add_argument("--par-secondi", type=float, default=90.0, help="durata minima di ogni livello")
    p.add_argument("--n-scritture", type=int, default=300, help="scritture singole e $set per combinazione")
    p.add_argument("--n-lotti-scritture", type=int, default=30,
                   help="insert a lotti per combinazione (matrice: 30 lotti da 100)")
    p.add_argument("--lotto-scritture", type=int, default=100, help="documenti per lotto")
    p.add_argument("--thread-volume", type=int, default=16)
    p.add_argument("--minuti-volume", type=float, default=3.0, help="durata di ogni passo di volume (0 = niente)")
    p.add_argument("--pausa-volume-s", type=float, default=60.0)
    p.add_argument("--grandi-gib", type=float, default=1.0, help="GiB di documenti da 2 KB in events_large")
    p.add_argument("--grandi-clienti", type=int, default=1080,
                   help="clienti dei documenti da 2 KB: con 1 GiB circa 512 documenti l'anno a cliente, cioe' circa 85 in "
                        "60 giorni, come i messaggi di M31 da 2 KB (Q4; errore 7 di A)")
    p.add_argument("--grandi-thread", type=int, default=8)
    p.add_argument("--grandi-lotto", type=int, default=250)
    p.add_argument("--grandi-n-q2", type=int, default=100)
    p.add_argument("--grandi-n-q1", type=int, default=100)
    p.add_argument("--grandi-n-q3", type=int, default=100)
    p.add_argument("--grandi-n-q4", type=int, default=60)
    p.add_argument("--ricarica-grandi", action="store_true", help="ricarica events_large anche se gia' finita")
    p.add_argument("--tieni-grandi", action="store_true", help="con 'giro': non cancellare events_large nella fase fine")
    p.add_argument("--lavaggio-collezioni", default="messaggi,eventi",
                   help="collezioni gia' misurate da leggere per intero lato server per svuotare il bancone")
    p.add_argument("--lavaggio-max-s", type=float, default=540.0,
                   help="tetto di una scansione del lavaggio (sotto i 600 s del socketTimeout del client)")
    p.add_argument("--lavaggio-soglia", type=float, default=0.05)
    p.add_argument("--thread-massimo", type=int, default=64, help="con 'giro': thread della fase carico_massimo")
    p.add_argument("--minuti-massimo", type=float, default=3.0, help="con 'giro': minuti per parte di carico_massimo")
    p.add_argument("--n-cancella", type=int, default=20, help="con 'giro': clienti interi da cancellare")
    p.add_argument("--attesa-rete-min", type=float, default=10.0, help="quanto aspettare se il cluster non risponde")
    # p1-1.6.1: celle nuove della matrice e sicurezza
    p.add_argument("--n-condizione", type=int, default=50,
                   help="clienti attivi mai letti per cella di condizioni (L49-L51) e mentre_scrivi (M1-M4)")
    p.add_argument("--min-secondi-condizione", type=float, default=100.0,
                   help="durata minima di ogni cella di condizioni e mentre_scrivi")
    p.add_argument("--sfondo-lotti-s", type=float, default=10.0,
                   help="mentre_scrivi: lotti al secondo delle scritture di fondo (velocita' costante)")
    p.add_argument("--sfondo-thread", type=int, default=4, help="mentre_scrivi: fili delle scritture di fondo")
    p.add_argument("--sfondo-riscaldamento-s", type=float, default=15.0,
                   help="mentre_scrivi: secondi di scritture prima delle letture")
    p.add_argument("--anche-se-pochi", action="store_true",
                   help="con 'giro': parte anche se i clienti mai letti non bastano (solo se il coordinatore dice si')")
    p.add_argument("--fredda-clienti", type=int, default=1080,
                   help="con --fredda-solo-carica: clienti dei documenti nuovi della collezione fredda")
    p.add_argument("--fredda-solo-carica", action="store_true",
                   help="con 'giro' (fase freddo): solo creazione e copia della collezione fredda, da fare su M10 prima "
                        "del cambio; il giro su M30 fa letture, modifica e cancellazione")
    p.add_argument("--tetto-errori-di-fila", type=int, default=5, help="misure fallite di fila prima di fermare una fase")
    # fredda (punto 7 del fondatore)
    p.add_argument("--fredda-gb", type=float, default=1.0, help="con 'fredda': GiB di eventi da copiare")
    p.add_argument("--fredda-lotto-mb", type=float, default=30.0, help="con 'fredda': MB per lotto di copia")
    p.add_argument("--fredda-max-min", type=float, default=15.0, help="con 'fredda': tempo massimo della copia")
    p.add_argument("--fredda-attesa-s", type=float, default=600.0, help="con 'fredda': attesa dopo la copia")
    p.add_argument("--n-fredda", type=int, default=100, help="con 'fredda': letture da 1 documento")
    p.add_argument("--n-fredda-100", type=int, default=20, help="con 'fredda': letture da 100 documenti di fila")
    a = p.parse_args()
    {"giro": cmd_giro, "consegna": cmd_consegna, "controlla": cmd_controlla, "carica": cmd_carica, "misura": cmd_misura, "tutto": cmd_tutto,
     "riassunto": cmd_riassunto, "pulisci": cmd_pulisci, "scrivi": cmd_scrivi, "latenza": cmd_latenza,
     "composizione": cmd_composizione, "estese": cmd_estese, "scarica_atlas": cmd_scarica_atlas,
     "fredda": cmd_fredda}[a.comando](a)


if __name__ == "__main__":
    main()
