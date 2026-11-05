"""P1 Irlanda · aspetta che il database risponda (l'IP della macchina deve essere nella lista di accesso di Atlas).
Uso: attendi_db.py MINUTI. Prova un ping ogni 30 secondi; esce 0 appena risponde, 1 dopo MINUTI minuti.
Non stampa mai la stringa di connessione (FENVIX_P1_URI): solo il tipo dell'errore."""
import datetime as dt
import os
import sys
import time

from pymongo import MongoClient

minuti = float(sys.argv[1])
uri = os.environ["FENVIX_P1_URI"]
fine = time.time() + minuti * 60
k = 0
while True:
    k += 1
    try:
        with MongoClient(uri, appname="app-demo", serverSelectionTimeoutMS=15000, connectTimeoutMS=10000) as c:
            c.admin.command("ping")
        print(f"{dt.datetime.now(dt.timezone.utc):%Y-%m-%dT%H:%M:%SZ} il database risponde (tentativo {k})", flush=True)
        sys.exit(0)
    except Exception as e:
        if type(e).__name__ in ("OperationFailure", "ConfigurationError", "InvalidURI"):
            # risponde ma rifiuta utente o password, o la stringa e' scritta male: aspettare non serve
            print(f"STRINGA DI CONNESSIONE RIFIUTATA ({type(e).__name__}, codice {getattr(e, 'code', None)}): "
                  f"controlla il segreto DEMO_DB_URI", flush=True)
            sys.exit(1)
        resto = (fine - time.time()) / 60
        print(f"{dt.datetime.now(dt.timezone.utc):%Y-%m-%dT%H:%M:%SZ} ATTESA LISTA DI ACCESSO: il database non risponde "
              f"({type(e).__name__}); riprovo tra 30 s, ancora {max(resto, 0):.0f} minuti", flush=True)
        if resto <= 0:
            print("ATLAS IRRAGGIUNGIBILE: l'IP non e' stato aggiunto in tempo, o la stringa di connessione e' sbagliata",
                  flush=True)
            sys.exit(1)
        time.sleep(30)
