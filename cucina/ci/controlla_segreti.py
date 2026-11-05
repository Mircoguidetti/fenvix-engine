"""Cucina minima (K2; da P1 Irlanda controlla_segreti.py) · prima di pubblicare: nessun segreto della giornata in nessun
file della cartella, compresi i .gz: la password del nostro server (CM_PW, anche come pezzo della stringa di
connessione) e nessuna chiave privata ("PRIVATE KEY": la chiave del certificato del server o della CA). Codice 1 se ne
trova uno (non si pubblica).
Uso: controlla_segreti.py CARTELLA   (la password arriva dall'ambiente, mai da argomenti)"""
import gzip
import os
import sys
import urllib.parse

pw = os.environ.get("CM_PW", "")
pezzi = set()
if len(pw) >= 8:
    pezzi |= {pw, urllib.parse.quote(pw, safe="")}
pezzi.add("PRIVATE KEY")
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
print(f"controllo segreti: nessun segreto nei file ({len(pezzi)} pezzi cercati, password {'compresa' if len(pw) >= 8 else 'ASSENTE'})")
