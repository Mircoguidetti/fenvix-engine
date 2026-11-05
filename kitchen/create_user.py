"""Kitchen (K2) · on the server machine: create the root user of our server, like aws-test3 (driver.py, ensure_root):
connection from 127.0.0.1 (the server's localhost exception: while there is no user, the first one is created from the
machine itself), TLS with the CA of the run. If the user is already there (the users live in the data bucket with the
rest: every cook after the loader finds it) that is fine. The password comes from the environment (CM_PW), never
from arguments.
Usage: create_user.py PORT CA_FILE   (exit 0 = user ready: login tried)"""
import os
import sys
import time

from pymongo import MongoClient
from pymongo.errors import OperationFailure

port, ca = sys.argv[1], sys.argv[2]
pw = os.environ["CM_PW"]
base = f"127.0.0.1:{port}/?directConnection=true&tls=true&tlsCAFile={ca}"
for k in range(60):
    try:
        with MongoClient(f"mongodb://{base}", serverSelectionTimeoutMS=5000) as c:
            try:
                c.admin.command("createUser", "root", pwd=pw, roles=[{"role": "root", "db": "admin"}])
                print("root user created")
            except OperationFailure as e:
                print(f"root user not created ({e.code}): already there")
        break
    except Exception as e:
        print(f"server not ready ({type(e).__name__}), retrying", flush=True)
        time.sleep(2)
with MongoClient(f"mongodb://root:{pw}@{base}&authSource=admin", serverSelectionTimeoutMS=10000) as c:
    c.admin.command("ping")
    n = len(c.admin.command("usersInfo")["users"])
print(f"login as root: OK ({n} users)")
