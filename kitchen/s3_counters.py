"""K2b (point 4 of the reviewer) · on the server machine, every N seconds: the S3 requests our server has made since it
started, from serverStatus.fenvix.s3 (K1, branch server-per-script: every request the counting object store forwards,
the compactor's included), and what they cost at the DECLARED S3 Standard prices (NUMBERS.md: PUT, COPY, POST and LIST
$0.005 per 1,000; GET $0.0004 per 1,000; HEAD is billed like GET; DELETE is free). The runner adds these to the SPESA
line every minute. If the server has no such counters (main), the file says available=false and the runner keeps its
fixed S3 allowance.
Usage: s3_counters.py PORT CA_FILE OUT_JSON SECONDS   (password from CM_PW)"""
import datetime as dt
import json
import os
import sys
import time

from pymongo import MongoClient

PUT_PER_1000 = 0.005
GET_PER_1000 = 0.0004


def snapshot(c):
    s = c.admin.command("serverStatus")
    f = (s.get("fenvix") or {}).get("s3")
    rec = {"t": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "available": f is not None}
    if f is not None:
        puts = sum(int(f.get(k, 0)) for k in ("put", "multipartPart", "copy", "list"))
        gets = sum(int(f.get(k, 0)) for k in ("get", "getRange", "head"))
        rec.update({"s3": {k: int(v) for k, v in f.items()}, "billed_like_put": puts, "billed_like_get": gets,
                    "dollars": round(puts * PUT_PER_1000 / 1000 + gets * GET_PER_1000 / 1000, 6)})
    return rec


def main():
    port, ca, out, every = sys.argv[1], sys.argv[2], sys.argv[3], float(sys.argv[4])
    uri = f"mongodb://root:{os.environ['CM_PW']}@127.0.0.1:{port}/?directConnection=true&tls=true&tlsCAFile={ca}&authSource=admin"
    with MongoClient(uri, serverSelectionTimeoutMS=10000) as c:
        while True:
            try:
                rec = snapshot(c)
                with open(out + ".tmp", "w") as f:
                    json.dump(rec, f)
                os.replace(out + ".tmp", out)
            except Exception as e:  # the server may be stopping: keep the last good file
                print(f"s3_counters: {type(e).__name__}", file=sys.stderr, flush=True)
            time.sleep(every)


if __name__ == "__main__":
    main()
