#!/usr/bin/env python3
"""Experiment (not part of the suite): what the engine does when the metadata service is unavailable while it renews.
Fake IMDS (ctl-tests/fake_aws.py) with credentials valid 320 s, so object_store keeps one for 20 s and then asks again (it renews
when less than 300 s are left). Timeline: 0-12 s IMDS up; 12-32 s IMDS answers 503 (outage, across the renewal at ~20 s, while
the old key still has ~300 s of validity); 32-50 s IMDS up again. One write + read every 0.25 s; per 2-second bucket: writes
acknowledged / failed, and S3 requests per key. usage: imds_outage.py <engine binary> > out.txt"""
import os, sys, threading, time
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "ctl-tests"))
os.environ["MONGO_SERVER_BIN"] = sys.argv[1]
import lib  # noqa: E402  (sets BIN from MONGO_SERVER_BIN)
from fake_aws import FakeImds, FakeS3, Registry  # noqa: E402
import test_19_imds_credential_refresh as t19  # noqa: E402

t19.BIN = sys.argv[1]
reg = Registry(); s3 = FakeS3(reg); imds = FakeImds(reg, ttl=320)
eng = t19.Engine(s3, {"AWS_METADATA_ENDPOINT": imds.url, "AWS_ALLOW_HTTP": "true"}, "imds-outage")
t0 = time.time()
def switch():
    time.sleep(12); imds.down = True
    time.sleep(20); imds.down = False
threading.Thread(target=switch, daemon=True).start()
c = eng.client(); events = []; n = 0
while time.time() - t0 < 50:
    a = time.time()
    try:
        c.outage.t.insert_one({"_id": n}); ok = c.outage.t.find_one({"_id": n}) is not None
    except Exception as e:
        ok = False
    events.append((a - t0, time.time() - a, ok)); n += 1
    time.sleep(0.25)
alive = eng.proc.poll() is None
eng.stop(); imds.close(); s3.close()
print(f"engine alive at the end: {alive}; credentials issued: {[(round(t - t0, 2), k) for t, k, _ in imds.issued]}")
print(f"IMDS requests answered 503 during the outage: {sum(1 for x in imds.log if x[3] is None)}")
print("bucket(s) | writes ok | writes failed | max write latency s | S3 requests by key (refused)")
for b in range(0, 50, 2):
    ev = [e for e in events if b <= e[0] < b + 2]
    rq = {}
    for r in s3.log:
        if b <= r["t"] - t0 < b + 2:
            k = rq.setdefault(r["key_id"], [0, 0]); k[0] += 1; k[1] += r["status"] == 403
    print(f"{b:2d}-{b+2:2d} | {sum(e[2] for e in ev)} | {sum(not e[2] for e in ev)} | {max([e[1] for e in ev] or [0]):.2f} | {rq}")
