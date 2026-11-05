"""P1 Irlanda v3 · controllo a secco di uno o piu' programmi (0 $, nessuna connessione), PRIMA di spendere.
Su una COPIA dei file di stato (cartella STATI con una sottocartella per database), passo per passo e nell'ordine:
  - carica: il file di stato che il caricamento scrivera' (stesse funzioni dello script, stesso seme: stessi clienti,
    stessi dormienti; gli _id per Q2 finti), cosi' i passi dopo si possono controllare prima del caricamento vero;
  - giro e dormienti: il piano a secco con lo SCRIPT VERO (--a-secco) su una copia preparata come fara' programma.py
    (giro di prima spostato; per dormienti i dormienti come "attivi"), poi il consumo simulato con le funzioni dello
    script (piano_giro16, stesso seme) e il file di stato aggiornato come lo lascera' il giro;
  - massimo: il piano a secco di una copia: deve usare 0 clienti mai letti.
Si ferma (FERMO) se: lo script dice FERMO o AVVISO, salta livelli del parallelo, un passo usa meno clienti di quelli
chiesti, un cliente verrebbe letto due volte, un passo dormienti tocca un cliente non dormiente, il programma ha errori
di forma (programma.py). Stampa per ogni passo i clienti usati e alla fine quanti restano.
Uso: simula_programma.py STATI PROGRAMMA [PROGRAMMA ...]   (STATI viene modificata: passare una copia)
Codice 0 e ultima riga "STATO: PRONTO" = si puo' partire."""
import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile

QUI = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(QUI, "prova_infinite.py")
MD5 = "8396ca455557baaa4e3a9b1f881451f9"
sys.path.insert(0, QUI)
import programma as P  # noqa: E402

spec = importlib.util.spec_from_file_location("pi", SCRIPT)
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)
from bson import ObjectId  # noqa: E402

FERMI = []


def fermo(msg):
    FERMI.append(msg)
    print("FERMO: " + msg)


def argomenti(comando, opz, file_stato):
    cattura = {}
    vecchi = (m.cmd_giro, m.cmd_carica)
    m.cmd_giro = lambda a: cattura.setdefault("a", a)
    m.cmd_carica = lambda a, stato=None: cattura.setdefault("a", a)
    argv = sys.argv
    sys.argv = ["prova_infinite.py", comando, "--file", file_stato, "--cartella-dati", "/tmp/nessuna"] + opz
    try:
        m.main()
    finally:
        sys.argv = argv
        m.cmd_giro, m.cmd_carica = vecchi
    return cattura["a"]


def a_secco(cartella, opz):
    """Lo script vero, --a-secco, su una copia temporanea della cartella (nessuna connessione)."""
    with tempfile.TemporaryDirectory() as t:
        for n in ("risultati_p1.json", "risultati_p1.json.usati.log"):
            if os.path.exists(os.path.join(cartella, n)):
                shutil.copy(os.path.join(cartella, n), t)
        shutil.copy(SCRIPT, t)
        env = {k: v for k, v in os.environ.items() if k not in ("FENVIX_P1_URI", "URI_GIRO")}
        r = subprocess.run([sys.executable, "prova_infinite.py", "giro", "--file", "risultati_p1.json",
                            "--cartella-dati", "dati"] + opz + ["--a-secco"], cwd=t, env=env, capture_output=True,
                           text=True, timeout=600)
        return r.returncode, r.stdout + r.stderr


def usati_di(cartella):
    d = json.load(open(os.path.join(cartella, "risultati_p1.json")))
    a, g = set(d.get("usati", [])), set(d.get("usati_grandi", []))
    lp = os.path.join(cartella, "risultati_p1.json.usati.log")
    if os.path.exists(lp):
        for r in open(lp):
            p = r.split()
            if len(p) >= 2 and p[1].lstrip("-").isdigit():
                (g if p[0] == "g" else a).add(int(p[1]))
    return a, g


def raccogli(x, att, gr):
    if isinstance(x, dict):
        if "compiti" in x:
            for c in x["compiti"]:
                raccogli(c, att, gr)
        elif "i" in x and "gruppo" in x:
            if x.get("i") is not None and x.get("gruppo") not in ("fredda", "finestre", "gia_letti"):
                (gr if x["gruppo"] == "grandi" else att).add(x["i"])
        else:
            for v in x.values():
                raccogli(v, att, gr)
    elif isinstance(x, list):
        for v in x:
            raccogli(v, att, gr)


def consuma(cartella, opz, nome_passo):
    """Consumo simulato del giro (come simula_giro.py di p1-giro-m10): ritorna (attivi, 2 KB) letti."""
    f = os.path.join(cartella, "risultati_p1.json")
    a = argomenti("giro", opz, f)
    st = m.Stato(f)
    la, lg = usati_di(cartella)
    st.d["usati"] = sorted(set(st.d.get("usati", [])) | la)
    st.d["usati_grandi"] = sorted(set(st.d.get("usati_grandi", [])) | lg)
    fasi = [x for x in a.solo_fasi.split(",") if x]
    if "grandi_carica" in fasi and not (st.d.get("grandi") or {}).get("finito"):
        st.d["grandi"] = {"finito": True, "clienti": a.grandi_clienti, "simulato": True,
                          "q2_id": [str(ObjectId()) for _ in range(a.grandi_clienti)]}
    nome = a.giro or "giro"
    piano, avvisi, conti = m.piano_giro16(st, a, nome, fasi)
    if "grandi_carica" in fasi:
        avvisi = [x for x in avvisi if not x.startswith("grandi_Q2")]
    for x in avvisi:
        fermo(f"{nome_passo}: {x}")
    att, gr = set(), set()
    raccogli(piano, att, gr)
    gia_a, gia_g = set(st.d["usati"]), set(st.d["usati_grandi"])
    if att & gia_a or gr & gia_g:
        fermo(f"{nome_passo}: {len(att & gia_a)} clienti e {len(gr & gia_g)} da 2 KB gia' letti nel piano")
    st.d["usati"] = sorted(gia_a | att)
    st.d["usati_grandi"] = sorted(gia_g | gr)
    gf = st.d.setdefault("giro_finale", {})
    gf.update({"nome": nome, "cartella": f"giro_finale_{nome}_SIMULATA"})
    gf.setdefault("fasi", {})
    for x in fasi:
        gf["fasi"].setdefault(x, []).append({"stato": "fatta", "sessione": f"simulata {nome}"})
    if "parallelo" in fasi:
        gf.setdefault("parallelo_livelli_fatti", [])
        gf["parallelo_livelli_fatti"] += [f"L{x}" for x in m._lista_int(a.livelli)]
    st.salva()
    return att, gr, conti


def controlla_uscita(nome_passo, rc, out, attesi_dorm=None):
    righe = [r for r in out.splitlines() if re.search(r"FERMO|AVVISO|livelli gia' finiti|Traceback|Error", r)]
    if rc != 0 or "A secco: nessuna connessione" not in out:
        fermo(f"{nome_passo}: il piano a secco dello script non e' riuscito (codice {rc}): {out.strip()[-400:]}")
    for r in righe:
        if "grandi_Q2" in r and "_id" in r:
            continue  # avviso atteso quando i 2 KB si caricano nello stesso giro
        fermo(f"{nome_passo}: lo script dice: {r.strip()}")
    for r in out.splitlines():
        if r.startswith(("Giro finale", "Il piano usa", "Totale STIMATO")) or re.match(r"parallelo L\d+:", r):
            print("      " + r.strip())


def passo_carica(cartella, opz, nome_passo):
    f = os.path.join(cartella, "risultati_p1.json")
    if os.path.exists(f) and (json.load(open(f)).get("caricamento") or {}).get("finito"):
        print(f"   {nome_passo}: gia' caricato")
        return
    os.makedirs(cartella, exist_ok=True)
    a = argomenti("carica", opz, f)
    import random
    rng = random.Random(a.seme)
    n = max(50, int(a.gb * 1024 / a.mb_cliente))
    taglie, ris = m.prepara_clienti(rng, n, a.mb_cliente, a.quota_riservati)
    d = {"versione_script": m.VERSIONE, "db": a.db, "simulato": True,
         "config": {"gb": a.gb, "mb_cliente": a.mb_cliente, "clienti": n, "riservati": len(ris), "seme": a.seme},
         "caricamento": {"finito": True, "motivo_fine": "obiettivo raggiunto (SIMULATO)"},
         "clienti": {"nomi_da": "cli-%06d", "n": n, "riservati": sorted(ris), "byte": [int(x * m.MB) for x in taglie],
                     "q2_id": [str(ObjectId()) for _ in range(n)], "fine_attivita": {"riservati": m.FINE_DORMIENTI,
                                                                                     "altri": 1.0}}}
    json.dump(d, open(f, "w"))
    open(f + ".usati.log", "a").close()
    print(f"   {nome_passo}: database {a.db}, {a.gb:g} GB, {n} clienti, {len(ris)} dormienti, {n - len(ris)} attivi "
          f"(file di stato SIMULATO: gli _id veri arrivano col caricamento)")


def stato_db(cartella):
    try:
        return json.load(open(os.path.join(cartella, "risultati_p1.json"))).get("db")
    except Exception:
        return None


def esegui_programma(stati, path, prog):
    passi = P.leggi_passi(path)
    for e in P.controlla_passi(passi):
        fermo(f"{prog}: {e}")
    tot = {}
    for k, p in enumerate(passi, 1):
        t, a = p["tipo"], p["arg"]
        if t == "aspetta":
            continue
        stato = a[0]
        c = os.path.join(stati, stato)
        resto = a[2:] if t == "massimo" else a[1:]
        nome_passo = f"{prog} passo {k} {t} {stato}"
        db = P.opzione(resto, "--db")
        if t != "carica" and stato_db(c) and db != stato_db(c):
            fermo(f"{nome_passo}: --db {db} ma il file di stato e' del database {stato_db(c)}")
        print(f" - {nome_passo}: {' '.join(resto)}")
        if t == "carica":
            passo_carica(c, resto, nome_passo)
            continue
        if not os.path.exists(os.path.join(c, "risultati_p1.json")):
            fermo(f"{nome_passo}: manca il file di stato {stato}")
            continue
        if t == "massimo":
            rc, out = a_secco(c, resto + ["--giro", "irlanda_massimo_01"])
            controlla_uscita(nome_passo, rc, out)
            if "Il piano usa 0 clienti attivi mai letti" not in out:
                fermo(f"{nome_passo}: il carico massimo userebbe clienti mai letti")
            continue
        # giro e dormienti: la copia preparata come fara' programma.py
        d = json.load(open(os.path.join(c, "risultati_p1.json")))
        if "giro_finale" in d:
            j = f"giro_finale_prima_passo_{k}"
            while j in d:
                j += "_"
            d[j] = d.pop("giro_finale")
            json.dump(d, open(os.path.join(c, "risultati_p1.json"), "w"))
        if t == "giro":
            rc, out = a_secco(c, resto)
            controlla_uscita(nome_passo, rc, out)
            att, gr, conti = consuma(c, resto, nome_passo)
            ris = set(d["clienti"]["riservati"])
            na, nd = len(att - ris), len(att & ris)
        else:  # dormienti
            ris = set(d["clienti"]["riservati"])
            prima_a, prima_g = usati_di(c)
            with tempfile.TemporaryDirectory() as t2:
                dc = json.loads(json.dumps(d))
                dc["clienti"]["riservati"] = []
                dc["usati"] = sorted(prima_a | {i for i in range(d["clienti"]["n"]) if i not in ris})
                dc.pop("giro_finale", None)
                json.dump(dc, open(os.path.join(t2, "risultati_p1.json"), "w"))
                open(os.path.join(t2, "risultati_p1.json.usati.log"), "w").close()
                rc, out = a_secco(t2, resto)
                controlla_uscita(nome_passo, rc, out)
                att, gr, conti = consuma(t2, resto, nome_passo)
            if att - ris:
                fermo(f"{nome_passo}: {len(att - ris)} clienti non dormienti nel passo dormienti")
            if att & prima_a:
                fermo(f"{nome_passo}: {len(att & prima_a)} dormienti gia' letti")
            dd = json.load(open(os.path.join(c, "risultati_p1.json")))
            dd["usati"] = sorted(set(dd.get("usati", [])) | att)
            json.dump(dd, open(os.path.join(c, "risultati_p1.json"), "w"))
            na, nd = 0, len(att)
        print(f"      -> letti: attivi {na}, dormienti {nd}, da 2 KB {len(gr)}")
        x = tot.setdefault(stato, [0, 0, 0])
        x[0] += na
        x[1] += nd
        x[2] += len(gr)
    return tot


def restano(stati):
    for s in sorted(os.listdir(stati)):
        c = os.path.join(stati, s)
        f = os.path.join(c, "risultati_p1.json")
        if not os.path.exists(f):
            continue
        st = m.Stato(f)
        la, lg = usati_di(c)
        us = set(st.d.get("usati", [])) | la | m.caldi_dei_giri_vecchi(st)
        cl = st.d["clienti"]
        ris = set(cl["riservati"])
        att = sum(1 for i in range(cl["n"]) if i not in ris and i not in us and cl["q2_id"][i])
        dor = sum(1 for i in ris if i not in us and cl["q2_id"][i])
        gq = (st.d.get("grandi") or {})
        gm = (gq.get("clienti") or 0) - len(set(st.d.get("usati_grandi", [])) | lg) if gq.get("finito") else None
        print(f"   {s:10s} db {st.d.get('db')}: restano mai letti attivi {att}, dormienti {dor}"
              + (f", da 2 KB {gm}" if gm is not None else ""))


def main():
    import hashlib
    if hashlib.md5(open(SCRIPT, "rb").read()).hexdigest() != MD5:
        print("FERMO: lo script non e' quello del fondatore (md5)")
        sys.exit(1)
    stati = sys.argv[1]
    for prog in sys.argv[2:]:
        path = prog if os.path.isfile(prog) else os.path.join(QUI, "programmi", prog, "passi.txt")
        nome = os.path.basename(os.path.dirname(path)) if path.endswith("passi.txt") else prog
        print(f"== programma {nome}")
        tot = esegui_programma(stati, path, nome)
        for s, (na, nd, ng) in sorted(tot.items()):
            print(f"   totale {s}: attivi {na}, dormienti {nd}, da 2 KB {ng}")
    print("== clienti mai letti che restano")
    restano(stati)
    if FERMI:
        print(f"STATO: FERMO ({len(FERMI)} problemi)")
        sys.exit(1)
    print("NESSUN CLIENTE LETTO DUE VOLTE, NESSUN FERMO, NESSUN LIVELLO SALTATO")
    print("STATO: PRONTO")


if __name__ == "__main__":
    main()
