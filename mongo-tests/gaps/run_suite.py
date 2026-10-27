#!/usr/bin/env python3
"""Run the whole existing differential suite (core, extra, replset) with pymongo and then Node.js, in parallel shards.

Each shard has its own reference mongod (single-node replica set rs0, ttlMonitorSleepSecs=1, ports 28000+k), its own engine (28100+k) and
idle-close engine (28200+k); engines use a snapshot copy of the release binary and the S3 store at localhost:9000 (moto, or MinIO when
that is what listens there).  usage: run_suite.py [--shards 6] [--drivers py,node] [--only substr]
Output: mongo-tests/out/suite/{py,node}_<k>.log / .json and a summary on stdout."""
import argparse, json, os, shutil, subprocess, sys, time, glob

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
TESTS = os.path.join(ROOT, "mongo-tests")
TOOLS = os.path.join(ROOT, ".tools")
MONGOD = glob.glob(os.path.join(TOOLS, "mongodb-linux-*", "bin", "mongod"))[0]
OUT = os.path.join(TESTS, "out", "suite")


def sh(cmd, **kw):
    return subprocess.run(cmd, shell=True, check=True, **kw)


def start_real(k):
    port = 28000 + k
    d = os.path.join(TOOLS, "data", "suite", str(k))
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)
    sh(f"chown mongotest {d} {os.path.dirname(d)}")
    sh(f"su mongotest -c 'ulimit -n 20000; {MONGOD} --port {port} --dbpath {d} --bind_ip 127.0.0.1 --replSet rs0 --setParameter ttlMonitorSleepSecs=1 --fork --logpath {d}.log' > /dev/null")
    from pymongo import MongoClient
    c = MongoClient("localhost", port, directConnection=True, serverSelectionTimeoutMS=20000)
    c.admin.command("replSetInitiate", {"_id": "rs0", "members": [{"_id": 0, "host": f"localhost:{port}"}]})
    for _ in range(60):
        if c.admin.command("hello").get("isWritablePrimary"):
            return
        time.sleep(0.5)
    raise SystemExit("real mongod %d not primary" % port)


def stop_real(k):
    port = 28000 + k
    try:
        from pymongo import MongoClient
        MongoClient("localhost", port, directConnection=True, serverSelectionTimeoutMS=3000).admin.command("shutdown", force=True)
    except Exception:
        pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--shards", type=int, default=6)
    ap.add_argument("--drivers", default="py,node")
    ap.add_argument("--only", default="")
    ap.add_argument("--tests", default="")
    ap.add_argument("--bin", default=os.path.join(ROOT, "target", "release", "mongo-server"))
    a = ap.parse_args()
    os.makedirs(OUT, exist_ok=True)
    binp = os.path.join(OUT, "mongo-server")
    shutil.copy(a.bin, binp)
    env = dict(os.environ, MONGO_SERVER_BIN=binp, SERVER_EXTRA="--ttl-sweep-secs 1 --replica-set rs0 --advertise localhost:%PORT%")
    # S3 endpoint
    import socket
    s = socket.socket()
    if s.connect_ex(("127.0.0.1", 9000)) != 0:
        subprocess.Popen([os.path.join(TESTS, ".venv", "bin", "moto_server"), "-p", "9000"], stdout=open(os.path.join(OUT, "moto.log"), "w"), stderr=subprocess.STDOUT)
        time.sleep(3)
    import boto3
    boto3.client("s3", endpoint_url="http://localhost:9000", aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin", region_name="us-east-1").create_bucket(Bucket="bench")
    for k in range(a.shards):
        start_real(k)
    results = {}
    for drv in a.drivers.split(","):
        procs = []
        for k in range(a.shards):
            real = f"mongodb://localhost:{28000 + k}/?directConnection=true"
            ours = f"mongodb://localhost:{28100 + k}/?directConnection=true"
            idle = f"mongodb://localhost:{28200 + k}/?directConnection=true"
            sh(f"cd {TESTS} && ./server.sh {28100 + k} 60 > /dev/null && ./server.sh {28200 + k} 3 > /dev/null", env=env)
            out = os.path.join(OUT, f"{drv}_{k}")
            extra = (["--tests", a.tests] if (a.tests and drv == "py") else []) + (["--only", a.only] if a.only else [])
            if drv == "py":
                cmd = [os.path.join(TESTS, ".venv", "bin", "python"), "run_py.py", "--real", real, "--ours", ours, "--idle", idle, "--ours-port", str(28100 + k), "--idle-port", str(28200 + k), "--shard", f"{k}/{a.shards}", "--out", out + ".json"] + extra
                cwd = TESTS
            else:
                cmd = ["node", "run_node.js", f"--real={real}", f"--ours={ours}", f"--idle={idle}", f"--ours-port={28100 + k}", f"--idle-port={28200 + k}", f"--shard={k}/{a.shards}", f"--out={out}.json"] + ([f"--only={a.only}"] if a.only else [])
                cwd = os.path.join(TESTS, "node")
            procs.append(subprocess.Popen(cmd, cwd=cwd, env=env, stdout=open(out + ".log", "w"), stderr=subprocess.STDOUT))
        for p in procs:
            p.wait()
        allr = []
        for k in range(a.shards):
            try:
                allr += json.load(open(os.path.join(OUT, f"{drv}_{k}.json")))
            except Exception as e:  # noqa
                print(f"shard {k} {drv}: no result file ({e})")
        results[drv] = allr
        json.dump(allr, open(os.path.join(OUT, f"{drv}_all.json"), "w"), indent=1)
        print(f"== {drv}: {sum(r['status'] == 'pass' for r in allr)}/{len(allr)} passed")
        for cat in ("core", "extra", "replset"):
            sel = [r for r in allr if r["category"] == cat]
            if sel:
                print(f"   {cat}: {sum(r['status'] == 'pass' for r in sel)}/{len(sel)}")
        for r in allr:
            if r["status"] != "pass":
                print("   FAIL", r["name"], json.dumps(r["detail"])[:300])
    for k in range(a.shards):
        stop_real(k)
    for k in range(a.shards):
        for port in (28100 + k, 28200 + k):
            subprocess.run(f"pkill -f '[l]isten 127.0.0.1:{port} '", shell=True)


if __name__ == "__main__":
    main()
