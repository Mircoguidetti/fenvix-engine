"""Do SlateDB compactor loops outlive a database's close or drop? (integration-4, after the stray busy server.)
Starts our server on a fresh bucket of the judge's MinIO, creates N databases, then (a) lets them close by idleness,
(b) drops them; after each, measures object-store GETs per second with nothing open. Healthy: near 0.
Usage: leak_check.py BINARY OUTDIR  (env: the sealed-runs env of the run under test)"""
import json, os, subprocess, sys, time
import boto3, pymongo

BIN, OUT = sys.argv[1], sys.argv[2]
N, PORT, BUCKET = 20, 27031, f"leakcheck-{int(time.time())}"
s3 = boto3.client("s3", endpoint_url="http://127.0.0.1:9000", aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin", region_name="us-east-1")
s3.create_bucket(Bucket=BUCKET)
env = dict(os.environ, AWS_ACCESS_KEY_ID="minioadmin", AWS_SECRET_ACCESS_KEY="minioadmin")
log = open(os.path.join(OUT, "leak-server.log"), "w")
p = subprocess.Popen([BIN, "--store", "s3", "--bucket", BUCKET, "--region", "us-east-1", "--endpoint", "http://127.0.0.1:9000",
                      "--listen", f"127.0.0.1:{PORT}", "--idle-secs", "3", "--holder", "leakcheck"], stdout=log, stderr=log, env=env)
res = {"bucket": BUCKET, "n": N}
try:
    c = pymongo.MongoClient(f"mongodb://127.0.0.1:{PORT}/?directConnection=true", serverSelectionTimeoutMS=60000)
    c.admin.command("ping")

    def rate(secs=20):
        a = c.admin.command("serverStatus")["fenvix"]["s3"]["get"]
        time.sleep(secs)
        return (c.admin.command("serverStatus")["fenvix"]["s3"]["get"] - a) / secs

    res["gets_per_s_start"] = rate()
    for i in range(N):
        d = c[f"lk{i}"]
        for b in range(5):
            d.t.insert_many([{"_id": b * 2000 + j, "f": "x" * 100} for j in range(2000)])
    res["gets_per_s_open"] = rate(10)
    time.sleep(60)  # idle close (3 s) and any close-time work
    res["gets_per_s_after_idle_close"] = rate()
    for i in range(N):
        c[f"lk{i}"].t.find_one()
        c.drop_database(f"lk{i}")
    time.sleep(60)
    res["gets_per_s_after_drop"] = rate()
finally:
    p.terminate()
    p.wait(60)
print(json.dumps(res))
json.dump(res, open(os.path.join(OUT, "leak-check.json"), "w"), indent=1)
