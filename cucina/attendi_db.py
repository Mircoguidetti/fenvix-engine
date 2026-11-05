"""Cucina minima (K2; copia di P1 Irlanda attendi_db.py) · aspetta che il nostro server risponda (FENVIX_P1_URI).
Uso: attendi_db.py MINUTI. Prova un ping ogni 15 secondi; esce 0 appena risponde, 1 dopo MINUTI minuti o se il server
rifiuta utente e password. Non stampa mai la stringa di connessione: solo il tipo dell'errore."""
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
        with MongoClient(uri, appname="app-demo", serverSelectionTimeoutMS=10000, connectTimeoutMS=10000) as c:
            c.admin.command("ping")
        print(f"{dt.datetime.now(dt.timezone.utc):%Y-%m-%dT%H:%M:%SZ} il server risponde (tentativo {k})", flush=True)
        sys.exit(0)
    except Exception as e:
        if type(e).__name__ in ("OperationFailure", "ConfigurationError", "InvalidURI"):
            print(f"SERVER RIFIUTA ({type(e).__name__}, codice {getattr(e, 'code', None)}): utente, password o "
                  f"stringa di connessione", flush=True)
            sys.exit(1)
        resto = (fine - time.time()) / 60
        print(f"{dt.datetime.now(dt.timezone.utc):%Y-%m-%dT%H:%M:%SZ} il server non risponde ancora ({type(e).__name__}); "
              f"riprovo tra 15 s, ancora {max(resto, 0):.0f} minuti", flush=True)
        if resto <= 0:
            print("SERVER IRRAGGIUNGIBILE", flush=True)
            sys.exit(1)
        time.sleep(15)
