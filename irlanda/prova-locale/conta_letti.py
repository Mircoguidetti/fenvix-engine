"""Prova locale v3 · nessun cliente letto due volte. Due controlli indipendenti dal file di stato:
1. il registro di ogni database (risultati_p1.json.usati.log, solo aggiunte, scritto PRIMA di ogni lettura fredda e
   portato da un giro all'altro dal workflow): nessuna riga ripetuta;
2. le misure salvate dallo script (tutti i fase_*.json di tutte le cartelle out-*): ogni (database, cliente) letto a
   freddo compare in un solo giro.
Uso: conta_letti.py CARTELLA_DELLA_PROVA CARTELLA_STATI"""
import collections
import glob
import json
import os
import sys

w, stati = sys.argv[1], sys.argv[2]
ok = True
for s in sorted(os.listdir(stati)):
    lp = os.path.join(stati, s, "risultati_p1.json.usati.log")
    if not os.path.exists(lp):
        continue
    righe = [r.strip() for r in open(lp) if r.strip()]
    c = collections.Counter(righe)
    doppie = {k: v for k, v in c.items() if v > 1}
    print(f"registro {s:8s}: {len(righe)} righe, {len(c)} clienti, ripetuti {len(doppie)}")
    ok &= not doppie
visti = collections.defaultdict(set)


def cerca(x, giro, db):
    if isinstance(x, dict):
        cli, gr = x.get("cliente"), x.get("gruppo")
        if isinstance(cli, str) and gr in ("attivi", "dormienti", "grandi") and x.get("tipo", "freddo") in ("freddo", None):
            visti[(db, cli)].add(giro)
        for v in x.values():
            cerca(v, giro, db)
    elif isinstance(x, list):
        for v in x:
            cerca(v, giro, db)


for f in glob.glob(os.path.join(w, "out-*", "dati", "*", "giro_finale_*", "fase_*.json")):
    db = f.split(os.sep)[-3]
    giro = f.split(os.sep)[-2].split("_2026")[0]
    if "carico_massimo" in f:
        continue
    try:
        cerca(json.load(open(f)), giro, db)
    except Exception as e:
        print("illeggibile:", f, e)
doppi = {k: v for k, v in visti.items() if len(v) > 1}
print(f"misure: {len(visti)} clienti letti in tutti i giri; in piu' di un giro: {len(doppi)}")
for k, v in list(doppi.items())[:10]:
    print("  ", k, sorted(v))
ok &= not doppi
print("NESSUN CLIENTE LETTO DUE VOLTE" if ok else "ERRORE: clienti letti due volte")
