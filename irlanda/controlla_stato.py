"""P1 Irlanda · controllo sul runner, PRIMA di spendere (0 $, nessuna connessione):
  1. lo script e' quello del fondatore (md5);
  2. il file di stato e il registro arrivati dal Mac sono leggibili; quanti clienti risultano gia' letti;
  3. lo script stesso, con --a-secco e le opzioni di opzioni.txt, su una COPIA dei due file: piano, nessun FERMO;
  4. guardia contro un file di stato vecchio (di prima del giro M30): se i clienti attivi mai letti sono piu' di
     SOGLIA (di serie 1000; dopo il giro M30 completo restano 484, MISURATO a secco nel brain), si ferma: rileggerebbe
     clienti gia' letti su M30 e le letture "fredde" sarebbero calde. Si supera solo con FORZA=1 (comando.json "forza").
Uso: controlla_stato.py CARTELLA_CON_I_FILE   (stampa il riassunto; codice 0 = si puo' partire)"""
import hashlib
import os
import re
import shutil
import subprocess
import sys
import tempfile

QUI = os.path.dirname(os.path.abspath(__file__))
MD5 = "8396ca455557baaa4e3a9b1f881451f9"
SOGLIA = int(os.environ.get("SOGLIA_ATTIVI", "1000"))
FORZA = os.environ.get("FORZA") == "1"


def esci(msg):
    print("FERMO: " + msg)
    sys.exit(1)


def main():
    src = sys.argv[1]
    script = os.path.join(QUI, "prova_infinite.py")
    md5 = hashlib.md5(open(script, "rb").read()).hexdigest()
    if md5 != MD5:
        esci(f"md5 dello script {md5}, atteso {MD5}: lo script non si tocca")
    print(f"OK    script p1-1.6.2, md5 {md5}")
    f_stato = os.path.join(src, "risultati_p1.json")
    f_log = f_stato + ".usati.log"
    if not os.path.isfile(f_stato):
        esci(f"manca {f_stato}")
    if not os.path.isfile(f_log):
        esci(f"manca {f_log}: senza il registro il giro potrebbe rileggere clienti gia' letti")
    import json
    d = json.load(open(f_stato))
    if "clienti" not in d:
        esci("il file di stato non ha il caricamento ('clienti')")
    righe = sum(1 for _ in open(f_log))
    gf = d.get("giro_finale") or {}
    print(f"OK    stato: {len(d.get('usati', []))} clienti gia' letti nel file, {righe} righe nel registro, "
          f"ultimo giro '{gf.get('nome')}', fasi registrate {len(gf.get('fasi', {}))}")
    opz = open(os.path.join(QUI, "opzioni.txt")).read().split()
    with tempfile.TemporaryDirectory() as t:
        shutil.copy(f_stato, t)
        shutil.copy(f_log, t)
        shutil.copy(script, t)
        env = {k: v for k, v in os.environ.items() if k not in ("FENVIX_P1_URI", "ATLAS_PUBLIC_KEY", "ATLAS_PRIVATE_KEY")}
        r = subprocess.run([sys.executable, "prova_infinite.py"] + opz + ["--a-secco"], cwd=t, env=env,
                           capture_output=True, text=True, timeout=300)
        out = r.stdout + r.stderr
        print("----- piano a secco (lo script, nessuna connessione) -----")
        print(out.rstrip())
        print("-----")
        intatti = all(open(os.path.join(t, n), "rb").read() == open(os.path.join(src, n), "rb").read()
                      for n in ("risultati_p1.json", "risultati_p1.json.usati.log"))
    if r.returncode != 0 or "A secco: nessuna connessione" not in out:
        esci(f"il piano a secco non e' riuscito (codice {r.returncode})")
    if not intatti:
        esci("il piano a secco ha cambiato i file di stato (non dovrebbe)")
    m = re.search(r"Clienti attivi mai letti (\d+)", out)
    u = re.search(r"Il piano usa (\d+) clienti attivi mai letti", out)
    if not m or not u:
        esci("non trovo nel piano quanti clienti attivi mai letti ci sono")
    mai, usa = int(m.group(1)), int(u.group(1))
    if usa != 150:
        esci(f"il piano usa {usa} clienti attivi, attesi 150 (50 per fase)")
    if mai > SOGLIA and not FORZA:
        esci(f"{mai} clienti attivi mai letti: piu' di {SOGLIA}. Sembra il file di stato di PRIMA del giro M30 "
             f"(dopo il giro M30 completo ne restano circa 484). Carica i file aggiornati dopo il giro M30.")
    print(f"OK    {mai} clienti attivi mai letti; il giro ne usa {usa}; dopo ne restano {mai - usa} per il giro M10"
          + (" (FORZA=1: soglia ignorata)" if mai > SOGLIA else ""))
    print("STATO: PRONTO")


if __name__ == "__main__":
    main()
