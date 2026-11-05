"""P1 Irlanda · prima di pubblicare: nessun pezzo segreto della stringa di connessione (DEMO_DB_URI: stringa intera,
utente:password, password, anche decodificata; il solo nome utente no, puo' essere una parola comune come "admin") in nessun file della cartella, compresi i .gz. Codice 1 se ne trova uno (non si pubblica).
Uso: controlla_segreti.py CARTELLA   (la stringa arriva dall'ambiente, mai da argomenti)"""
import gzip
import os
import re
import sys
import urllib.parse

uri = os.environ.get("DEMO_DB_URI", "")
pezzi = set()
if len(uri) >= 12:
    pezzi.add(uri)
    m = re.match(r"mongodb(\+srv)?://([^@/]+)@", uri)
    if m:
        for p in [m.group(2)] + m.group(2).split(":")[1:]:
            for q in (p, urllib.parse.unquote(p)):
                if len(q) >= 4:
                    pezzi.add(q)
if not pezzi:
    print("controllo segreti: DEMO_DB_URI assente o corta: niente da cercare")
    sys.exit(0)
trovati = []
for radice, _, nomi in os.walk(sys.argv[1]):
    for n in nomi:
        p = os.path.join(radice, n)
        try:
            dati = gzip.open(p).read() if n.endswith(".gz") else open(p, "rb").read()
        except Exception:
            dati = open(p, "rb").read()
        if any(x.encode() in dati for x in pezzi):
            trovati.append(os.path.relpath(p, sys.argv[1]))
if trovati:
    print(f"SEGRETO TROVATO in {len(trovati)} file: {', '.join(trovati[:20])}. Non pubblico.")
    sys.exit(1)
print(f"controllo segreti: nessun pezzo della stringa di connessione nei file ({len(pezzi)} pezzi cercati)")
