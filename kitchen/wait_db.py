"""Kitchen (K2, from P1 Ireland attendi_db.py) · wait until our server answers (FENVIX_P1_URI).
Usage: wait_db.py MINUTES. A ping every 15 s; exit 0 as soon as it answers, 1 after MINUTES or if the server refuses
user and password. Never prints the connection string: only the error type."""
import datetime as dt
import os
import sys
import time

from pymongo import MongoClient

minutes = float(sys.argv[1])
uri = os.environ["FENVIX_P1_URI"]
end = time.time() + minutes * 60
k = 0
while True:
    k += 1
    try:
        with MongoClient(uri, appname="app-demo", serverSelectionTimeoutMS=10000, connectTimeoutMS=10000) as c:
            c.admin.command("ping")
        print(f"{dt.datetime.now(dt.timezone.utc):%Y-%m-%dT%H:%M:%SZ} the server answers (attempt {k})", flush=True)
        sys.exit(0)
    except Exception as e:
        if type(e).__name__ in ("OperationFailure", "ConfigurationError", "InvalidURI"):
            print(f"SERVER REFUSES ({type(e).__name__}, code {getattr(e, 'code', None)}): user, password or connection "
                  f"string", flush=True)
            sys.exit(1)
        left = (end - time.time()) / 60
        print(f"{dt.datetime.now(dt.timezone.utc):%Y-%m-%dT%H:%M:%SZ} no answer yet ({type(e).__name__}); retry in 15 s, "
              f"{max(left, 0):.0f} minutes left", flush=True)
        if left <= 0:
            print("SERVER UNREACHABLE", flush=True)
            sys.exit(1)
        time.sleep(15)
