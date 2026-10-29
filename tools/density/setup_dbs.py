"""Create databases c{lo}..c{hi-1}: admin POST /v1/databases (as a control plane would), a user per database
(readWrite on its own database) and its data: 2 collections (`items` 100 documents, `events` 100 documents, about
1 KB each), 1 secondary index (items.k). Idempotent: an existing database is skipped.
usage: setup_dbs.py LO HI"""
import asyncio
import json
import os
import random
import sys
import time
import urllib.request
import urllib.error

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import *  # noqa
from pymongo import AsyncMongoClient, MongoClient
from pymongo.errors import OperationFailure


def admin(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(f"http://127.0.0.1:{ADMIN_PORT}{path}", data=data, method=method,
                                 headers={"Authorization": f"Bearer {ADMIN_TOKEN}", "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            return r.status, json.loads(r.read() or b"null")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def ensure_root():
    c = MongoClient("127.0.0.1", PORT, tls=True, tlsCAFile=CA, directConnection=True, serverSelectionTimeoutMS=20000)
    try:
        c.admin.command("createUser", ROOT_USER, pwd=ROOT_PW, roles=[{"role": "root", "db": "admin"}])
        print("root created through the localhost exception")
    except OperationFailure as e:
        if e.code not in (13, 51003, 11000):
            raise
    finally:
        c.close()


def doc(i, j):
    return {"_id": j, "k": j % 50, "name": f"customer-{i}-{j}", "n": 0, "pad": "x" * 900}


async def one(root, i, sem, stats):
    async with sem:
        db = db_name(i)
        st, body = await asyncio.to_thread(admin, "POST", "/v1/databases", {"name": db})
        if st == 409:
            stats["skipped"] += 1
            return
        if st != 201:
            raise RuntimeError(f"create {db}: {st} {body}")
        d = root[db]
        await d.command("createUser", "u", pwd=user_pw(i), roles=[{"role": "readWrite", "db": db}])
        await d.items.insert_many([doc(i, j) for j in range(100)])
        await d.events.insert_many([doc(i, j) for j in range(100)])
        await d.items.create_index("k")
        stats["created"] += 1


async def main(lo, hi):
    ensure_root()
    root = AsyncMongoClient("127.0.0.1", PORT, tls=True, tlsCAFile=CA, directConnection=True, username=ROOT_USER,
                            password=ROOT_PW, authSource="admin", maxPoolSize=32)
    sem = asyncio.Semaphore(16)
    stats = {"created": 0, "skipped": 0}
    t0 = time.time()
    await asyncio.gather(*(one(root, i, sem, stats) for i in range(lo, hi)))
    print(f"databases {lo}..{hi - 1}: {stats} in {time.time() - t0:.0f}s")
    await root.close()


if __name__ == "__main__":
    asyncio.run(main(int(sys.argv[1]), int(sys.argv[2])))
