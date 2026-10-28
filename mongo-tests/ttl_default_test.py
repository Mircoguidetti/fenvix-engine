#!/usr/bin/env python3
"""TTL with the default settings (results8): a sleeping tenant is not woken to expire documents, expired documents stay
hidden on read, and they are removed physically at the next natural writer session.
Own server on :27034 (default flags except --ttl-sweep-secs 1 for the writer-session sweeper). Output: out/ttl_default.json"""
import json, os, signal, subprocess, time, datetime

# The engine has no built-in S3 settings: MinIO of the tests, explicit (same values as tools/minio-env.sh). They replace any AWS_* already in the environment: a test must never talk to MinIO with real credentials, nor to real S3 with these.
for _k, _v in {"AWS_ACCESS_KEY_ID": "minioadmin", "AWS_SECRET_ACCESS_KEY": "minioadmin", "AWS_REGION": "us-east-1", "AWS_ENDPOINT": "http://127.0.0.1:9000", "AWS_ALLOW_HTTP": "true"}.items():
    os.environ[_k] = _v
for _k in ("AWS_SESSION_TOKEN", "AWS_PROFILE", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_CREDENTIALS_FULL_URI"):
    os.environ.pop(_k, None)
import pymongo

HERE = os.path.dirname(os.path.abspath(__file__))
BIN = os.path.join(HERE, "..", "target", "release", "mongo-server")
PORT = 27034
URI = f"mongodb://localhost:{PORT}/?directConnection=true"
log = open(os.path.join(HERE, "out", "ttl-default-server.log"), "ab")
p = subprocess.Popen([BIN, "--listen", f"127.0.0.1:{PORT}", "--idle-secs", "100000", "--holder", "ttld", "--ttl-sweep-secs", "1"], stdout=log, stderr=log)
try:
    for _ in range(100):
        try:
            pymongo.MongoClient(URI, serverSelectionTimeoutMS=300).admin.command("ping"); break
        except Exception:
            time.sleep(0.2)
    c = pymongo.MongoClient(URI)
    st = lambda: c.admin.command("tenantStats")
    out = {}
    names = [f"ttld{int(time.time()) % 100000}_{i}" for i in range(5)]
    soon = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) + datetime.timedelta(seconds=3)
    for n in names:
        c[n].c.create_index("t", expireAfterSeconds=0)
        c[n].c.insert_many([{"_id": i, "t": soon} for i in range(20)] + [{"_id": 99, "t": datetime.datetime(2100, 1, 1)}])
        c[n].command("tenantAdmin", close=True)
    s0 = st()
    time.sleep(12)  # every document is expired by now; the tenants are asleep
    s1 = st()
    r = lambda s: s["requests"]
    out["asleep_12s"] = {"requests_put": r(s1)["put"] - r(s0)["put"], "requests_get": r(s1)["get"] - r(s0)["get"], "requests_list": r(s1)["list"] - r(s0)["list"],
                         "ttl_wakes": s1["ttl"]["wakes"], "ttl_passes": s1["ttl"]["passes"], "open_writers": s1["openWriters"], "open_readers": s1["openReaders"],
                         "docs_expired_physically": s1["ttl"]["docsExpired"]}
    # a read wakes the tenant naturally: expired documents are hidden, and still not removed (a reader cannot write)
    n0 = names[0]
    seen = [d["_id"] for d in c[n0].c.find({})]
    s2 = st()
    out["after_read"] = {"visible_ids": seen, "docs_expired_physically": s2["ttl"]["docsExpired"], "mode": "reader" if s2["openReaders"] else "other"}
    # the next write (natural writer session) removes them
    c[n0].c.insert_one({"_id": 1000, "t": datetime.datetime(2100, 1, 1)})
    time.sleep(1.5)
    s3 = st()
    out["after_write"] = {"visible_ids": sorted(d["_id"] for d in c[n0].c.find({})), "docs_expired_physically": s3["ttl"]["docsExpired"]}
    # the other four tenants were never touched: still asleep, nothing removed
    out["untouched_tenants"] = {"count": len(names) - 1, "open_total": s3["openReaders"] + s3["openWriters"]}
    json.dump(out, open(os.path.join(HERE, "out", "ttl_default.json"), "w"), indent=1)
    print(json.dumps(out, indent=1))
finally:
    p.send_signal(signal.SIGTERM)
    try: p.wait(30)
    except Exception: p.kill()
