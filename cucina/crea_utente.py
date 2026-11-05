"""Cucina minima (K2) · sulla macchina server: crea l'utente root del nostro server, come aws-test3 (driver.py,
ensure_root): collegamento da 127.0.0.1 (l'eccezione localhost del server: finche' non c'e' nessun utente, il primo si
crea dalla macchina stessa), TLS con la CA della giornata. Se l'utente c'e' gia' (taglia dopo la prima: gli utenti
stanno nel bucket dei dati con il resto) va bene lo stesso. La password arriva dall'ambiente (CM_PW), mai da argomenti.
Uso: crea_utente.py PORTA FILE_CA   (codice 0 = utente pronto: login provato)"""
import os
import sys
import time

from pymongo import MongoClient
from pymongo.errors import OperationFailure

porta, ca = sys.argv[1], sys.argv[2]
pw = os.environ["CM_PW"]
base = f"127.0.0.1:{porta}/?directConnection=true&tls=true&tlsCAFile={ca}"
for k in range(60):
    try:
        with MongoClient(f"mongodb://{base}", serverSelectionTimeoutMS=5000) as c:
            try:
                c.admin.command("createUser", "root", pwd=pw, roles=[{"role": "root", "db": "admin"}])
                print("utente root creato")
            except OperationFailure as e:
                print(f"utente root non creato ({e.code}): c'e' gia' (taglia dopo la prima)")
        break
    except Exception as e:
        print(f"server non pronto ({type(e).__name__}), riprovo", flush=True)
        time.sleep(2)
with MongoClient(f"mongodb://root:{pw}@{base}&authSource=admin", serverSelectionTimeoutMS=10000) as c:
    c.admin.command("ping")
    n = len(c.admin.command("usersInfo")["users"])
print(f"login con root: OK ({n} utenti)")
